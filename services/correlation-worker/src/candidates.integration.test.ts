// Integration tests for H3 proximity candidate lookup
// Run against a REAL Redis (docker-compose), not a mock: the guarantee under
// test is that gridDisk(cell, k) plus ZRANGEBYSCORE against the exact
// geo-cell:{cell_id} sorted sets the Position Consumer writes in production
// actually returns (or correctly excludes) real H3 cell IDs and real Redis
// scores -- a fake gridDisk/sorted-set would only prove the fake behaves as
// coded, not that a real cell boundary or a real stale score is handled.
//
// Requires: `make up` (locally) or the CI Redis service container.
//
// All cell IDs and coordinates below come from a direct h3-js exploration
// (gridDisk/gridRing/cellToBoundary) against h3-js 4.5.0 at resolution 7,
// not from a formula assuming H3's average edge length is a worst-case bound.
// See docs/implementation/phase-05-correlation-worker/concepts/h3-candidate-lookup/
// for how these specific cells and coordinates were derived and verified.
import { randomUUID } from 'node:crypto';
import { Redis } from 'ioredis';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';
import { config } from './config.js';
import { findProximityCandidates } from './candidates.js';

// Origin cell at resolution 7 for base point (37.0, -121.0), inside the
// ingestion poller's California bbox.
const ORIGIN_CELL = '8729a9749ffffff';

// Immediate neighbor of ORIGIN_CELL (gridDisk(ORIGIN_CELL, 1)) -- the two
// cells share a boundary edge. Verified boundary-crossing points ~20m apart
// straddling that edge: (36.989033, -120.988104) in ORIGIN_CELL vs
// (36.988954, -120.987902) in NEIGHBOR_CELL.
const NEIGHBOR_CELL = '8729a9748ffffff';

// Present in gridDisk(ORIGIN_CELL, 2) but absent from gridDisk(ORIGIN_CELL, 1)
// -- an outer-ring-only cell, center ~5021m from ORIGIN_CELL's center.
const OUTER_RING_CELL = '8729a976bffffff';

const NOW_MS = 1_700_000_000_000;
const FRESH_MS = NOW_MS;
const STALE_MS = NOW_MS - 10_000;
const MIN_LAST_SEEN_MS = NOW_MS - 5_000; // freshness lower bound used by every test below

const redis = new Redis(config.REDIS_URL);

// Prefixed so cleanup can never touch real dev/live data, and so a stray
// failed run's leftovers are trivially identifiable and safe to wipe.
const testId = () => `test-candidates-${randomUUID()}`;

async function seedMember(cell: string, entityId: string, scoreMs: number): Promise<void> {
	await redis.zadd(`geo-cell:${cell}`, scoreMs, entityId);
}

const seededKeys = new Set<string>();
function trackCell(cell: string): void {
	seededKeys.add(`geo-cell:${cell}`);
}

describe('findProximityCandidates', () => {
	beforeAll(() => {
		trackCell(ORIGIN_CELL);
		trackCell(NEIGHBOR_CELL);
		trackCell(OUTER_RING_CELL);
	});

	afterEach(async () => {
		if (seededKeys.size > 0) await redis.del(...seededKeys);
	});

	afterAll(async () => {
		await redis.quit();
	});

	it('finds a candidate in the same cell at k=0', async () => {
		const a = testId();
		const b = testId();
		await seedMember(ORIGIN_CELL, a, FRESH_MS);
		await seedMember(ORIGIN_CELL, b, FRESH_MS);

		const result = await findProximityCandidates(redis, a, ORIGIN_CELL, 0, MIN_LAST_SEEN_MS);

		expect(result).toEqual([b]);
	});

	it('excludes the querying entity from its own cell', async () => {
		const a = testId();
		await seedMember(ORIGIN_CELL, a, FRESH_MS);

		const result = await findProximityCandidates(redis, a, ORIGIN_CELL, 0, MIN_LAST_SEEN_MS);

		expect(result).toEqual([]);
	});

	it('misses a boundary-crossing neighbor at k=0 but finds it at k=1', async () => {
		// a and b are ~20m apart physically (verified via h3-js greatCircleDistance
		// during exploration) but sit in different H3 cells across a shared edge.
		const a = testId();
		const b = testId();
		await seedMember(ORIGIN_CELL, a, FRESH_MS);
		await seedMember(NEIGHBOR_CELL, b, FRESH_MS);

		const atZero = await findProximityCandidates(redis, a, ORIGIN_CELL, 0, MIN_LAST_SEEN_MS);
		expect(atZero).toEqual([]);

		const atOne = await findProximityCandidates(redis, a, ORIGIN_CELL, 1, MIN_LAST_SEEN_MS);
		expect(atOne).toEqual([b]);
	});

	it('misses an outer-ring candidate at k=1 but finds it at k=2', async () => {
		const a = testId();
		const c = testId();
		await seedMember(ORIGIN_CELL, a, FRESH_MS);
		await seedMember(OUTER_RING_CELL, c, FRESH_MS);

		const atOne = await findProximityCandidates(redis, a, ORIGIN_CELL, 1, MIN_LAST_SEEN_MS);
		expect(atOne).toEqual([]);

		const atTwo = await findProximityCandidates(redis, a, ORIGIN_CELL, 2, MIN_LAST_SEEN_MS);
		expect(atTwo).toEqual([c]);
	});

	it('excludes a same-cell member whose last_seen_ms is older than the freshness bound', async () => {
		const a = testId();
		const stale = testId();
		await seedMember(ORIGIN_CELL, a, FRESH_MS);
		await seedMember(ORIGIN_CELL, stale, STALE_MS);

		const result = await findProximityCandidates(redis, a, ORIGIN_CELL, 0, MIN_LAST_SEEN_MS);

		expect(result).toEqual([]);
	});

	it('dedupes a candidate reachable through more than one ring cell', async () => {
		// gridDisk(ORIGIN_CELL, 1) includes NEIGHBOR_CELL; querying at k=1 must
		// not return the same entity twice even though multiple cells are scanned.
		const a = testId();
		const b = testId();
		await seedMember(ORIGIN_CELL, a, FRESH_MS);
		await seedMember(NEIGHBOR_CELL, b, FRESH_MS);

		const result = await findProximityCandidates(redis, a, ORIGIN_CELL, 1, MIN_LAST_SEEN_MS);

		expect(result).toEqual([b]);
		expect(result.length).toBe(new Set(result).size);
	});
});
