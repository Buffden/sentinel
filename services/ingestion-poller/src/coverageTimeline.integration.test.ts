// Integration tests for the coverage timeline against a REAL Redis. The
// guarantees live in the Lua scripts (lease check, bootstrap predicate, one
// revision per mutation, idempotent close, pruning), so a mocked client would
// only prove the mock.
//
// Every test uses its own random hash-tagged keys, never the real
// {live-provider} keys. Requires Redis at REDIS_URL (default
// redis://localhost:6379): `make up` or the CI service container.
import { randomUUID } from 'node:crypto';
import { Redis } from 'ioredis';
import { afterAll, afterEach, beforeEach, describe, expect, it } from 'vitest';
import type { SplitResult } from './adsbfiPoller.js';
import { Coordinator } from './coordinator.js';
import { CoordinatorLease } from './coordinatorLease.js';
import { CoverageTimeline } from './coverageTimeline.js';
import { ProviderHealthStore } from './providerHealthStore.js';

const REDIS_URL = process.env['REDIS_URL'] ?? 'redis://localhost:6379';
const redis = new Redis(REDIS_URL);

const TOKEN = 'token-a';
const RETENTION_MS = 100_000;

let leaseKey: string;
let authorityKey: string;
let coverageKey: string;
let timeline: CoverageTimeline;

async function authority(): Promise<Record<string, string>> {
	return redis.hgetall(authorityKey);
}

async function coverage(): Promise<string[]> {
	return redis.zrange(coverageKey, '0', '-1', 'WITHSCORES');
}

async function snapshot(): Promise<string> {
	return JSON.stringify([await authority(), await coverage(), await redis.get(leaseKey)]);
}

beforeEach(async () => {
	const tag = `{test-coverage-${randomUUID()}}`;
	leaseKey = `${tag}:lease`;
	authorityKey = `${tag}:authority`;
	coverageKey = `${tag}:coverage`;
	timeline = new CoverageTimeline(redis, RETENTION_MS, leaseKey, authorityKey, coverageKey);
	await redis.set(leaseKey, TOKEN, 'PX', 60_000);
});

afterEach(async () => {
	await redis.del(
		leaseKey,
		authorityKey,
		coverageKey,
		`${leaseKey}:health:adsbfi`,
		`${leaseKey}:health:opensky`,
	);
});

afterAll(async () => {
	await redis.quit();
});

describe('CoverageTimeline against real Redis', () => {
	it('never bootstraps from CREDIT: a record with no authority stays untouched', async () => {
		await redis.hset(authorityKey, 'heartbeat_ms', '1790283444182');
		const before = await snapshot();
		expect(await timeline.credit(TOKEN, 'adsbfi', 1_000)).toEqual({
			status: 'authority_changed',
			authority: null,
		});
		expect(await snapshot()).toBe(before);
		// Provider without epoch is still not an initialized record.
		await redis.hset(authorityKey, 'provider', 'adsbfi');
		expect((await timeline.credit(TOKEN, 'adsbfi', 1_000)).status).toBe('authority_changed');
	});

	it('COMMIT on a record that never had an authority creates epoch 1, one revision', async () => {
		await redis.hset(authorityKey, 'heartbeat_ms', '1790283444182');
		expect(await timeline.commit(TOKEN, 'opensky', 1_000)).toEqual({
			status: 'committed',
			epoch: 1,
			timelineVersion: 1,
		});
		expect(await authority()).toEqual({
			heartbeat_ms: '1790283444182',
			provider: 'opensky',
			epoch: '1',
			authority_since_ms: '1000',
			coverage_open_since_ms: '',
			timeline_version: '1',
		});
	});

	it('COMMIT stores 13-digit authority times exactly without fabricating coverage', async () => {
		const t = 1790283486599;
		expect((await timeline.commit(TOKEN, 'adsbfi', t)).status).toBe('committed');
		const record = await authority();
		expect(record['authority_since_ms']).toBe(String(t));
		expect(record['coverage_open_since_ms']).toBe('');
		expect(record['last_active_success_ms']).toBeUndefined();
	});

	it('CREDIT opens coverage after COMMIT, then extends without a new revision or moving backwards', async () => {
		await timeline.commit(TOKEN, 'adsbfi', 1_000);
		expect(await timeline.credit(TOKEN, 'adsbfi', 1_000)).toEqual({
			status: 'opened',
			timelineVersion: 2,
		});
		expect(await timeline.credit(TOKEN, 'adsbfi', 3_000)).toEqual({
			status: 'extended',
			timelineVersion: 2,
		});
		expect(await timeline.credit(TOKEN, 'adsbfi', 2_000)).toEqual({
			status: 'stale_clock',
			timelineVersion: 2,
		});
		expect(await timeline.credit(TOKEN, 'adsbfi', 3_000)).toEqual({
			status: 'stale_clock',
			timelineVersion: 2,
		});

		const a = await authority();
		expect(a['coverage_open_since_ms']).toBe('1000');
		expect(a['last_active_success_ms']).toBe('3000');
		expect(a['timeline_version']).toBe('2');
	});

	it('closes as failure at last_active_success_ms, then later closes are no-ops', async () => {
		await timeline.commit(TOKEN, 'adsbfi', 1_000);
		await timeline.credit(TOKEN, 'adsbfi', 1_000);
		await timeline.credit(TOKEN, 'adsbfi', 3_000);

		expect(await timeline.close(TOKEN, 'failure', 4_000)).toEqual({
			status: 'closed',
			member: 'adsbfi|1000|3000|failure',
			timelineVersion: 3,
			pruned: 0,
		});
		expect(await coverage()).toEqual(['adsbfi|1000|3000|failure', '3000']);
		expect((await authority())['coverage_open_since_ms']).toBe('');

		const before = await snapshot();
		expect(await timeline.close(TOKEN, 'failure', 5_000)).toEqual({ status: 'already_closed' });
		expect(await timeline.close(TOKEN, 'coordinator_down', 6_000)).toEqual({
			status: 'already_closed',
		});
		expect(await snapshot()).toBe(before);
	});

	// Three shapes of close, decided by last_active_success_ms against
	// coverage_open_since_ms.
	it('a positive open segment closes exactly as before: member, version, prune', async () => {
		await timeline.commit(TOKEN, 'adsbfi', 1_000);
		await timeline.credit(TOKEN, 'adsbfi', 1_000);
		await timeline.credit(TOKEN, 'adsbfi', 1_001);
		expect(await timeline.close(TOKEN, 'coordinator_shutdown', 2_000)).toEqual({
			status: 'closed',
			member: 'adsbfi|1000|1001|coordinator_shutdown',
			timelineVersion: 3,
			pruned: 0,
		});
		expect(await coverage()).toEqual(['adsbfi|1000|1001|coordinator_shutdown', '1001']);
	});

	it('a zero-length open segment closes without writing a member, still one revision', async () => {
		// Opened by one credited cycle and closed before another: start == end.
		await timeline.commit(TOKEN, 'adsbfi', 1_000);
		await timeline.credit(TOKEN, 'adsbfi', 1_000);
		expect(await timeline.close(TOKEN, 'coordinator_shutdown', 2_000)).toEqual({
			status: 'closed_empty',
			timelineVersion: 3,
		});
		expect(await coverage()).toEqual([]);
		const record = await authority();
		expect(record['coverage_open_since_ms']).toBe('');
		expect(record['last_active_success_ms']).toBe('1000');
		expect(record['timeline_version']).toBe('3');
		// Closed now: a second close is a true no-op.
		const before = await snapshot();
		expect(await timeline.close(TOKEN, 'failure', 3_000)).toEqual({ status: 'already_closed' });
		expect(await snapshot()).toBe(before);
	});

	it('a backwards open segment is an invariant error: it writes nothing and throws', async () => {
		// The credit script cannot produce this; only a corrupted hash can.
		await redis.hset(
			authorityKey,
			'provider',
			'adsbfi',
			'epoch',
			'1',
			'coverage_open_since_ms',
			'5000',
			'last_active_success_ms',
			'4000',
			'timeline_version',
			'3',
		);
		const before = await snapshot();
		await expect(timeline.close(TOKEN, 'failure', 6_000)).rejects.toThrow(
			/before coverage_open_since_ms/,
		);
		expect(await snapshot()).toBe(before);
	});

	it('opens a new segment on recovery, keeping epoch and authority_since_ms', async () => {
		await timeline.commit(TOKEN, 'adsbfi', 1_000);
		await timeline.credit(TOKEN, 'adsbfi', 1_000);
		await timeline.close(TOKEN, 'failure', 1_500);

		expect(await timeline.credit(TOKEN, 'adsbfi', 5_000)).toEqual({
			status: 'opened',
			timelineVersion: 4,
		});
		const a = await authority();
		expect(a['coverage_open_since_ms']).toBe('5000');
		expect(a['last_active_success_ms']).toBe('5000');
		expect(a['epoch']).toBe('1');
		expect(a['authority_since_ms']).toBe('1000');
	});

	it('refuses to open a segment that would start before the last success', async () => {
		await timeline.commit(TOKEN, 'adsbfi', 5_000);
		await timeline.credit(TOKEN, 'adsbfi', 5_000);
		await timeline.close(TOKEN, 'failure', 6_000);
		expect(await timeline.credit(TOKEN, 'adsbfi', 4_000)).toEqual({
			status: 'stale_clock',
			timelineVersion: 3,
		});
		expect((await authority())['coverage_open_since_ms']).toBe('');
	});

	it('records coordinator_shutdown and coordinator_down as the close reason', async () => {
		await timeline.commit(TOKEN, 'adsbfi', 1_000);
		await timeline.credit(TOKEN, 'adsbfi', 1_000);
		await timeline.credit(TOKEN, 'adsbfi', 1_500);
		await timeline.close(TOKEN, 'coordinator_shutdown', 2_000);
		await timeline.credit(TOKEN, 'adsbfi', 3_000);
		await timeline.credit(TOKEN, 'adsbfi', 4_000);
		await timeline.close(TOKEN, 'coordinator_down', 5_000);

		expect(await coverage()).toEqual([
			'adsbfi|1000|1500|coordinator_shutdown',
			'1500',
			'adsbfi|3000|4000|coordinator_down',
			'4000',
		]);
		expect((await authority())['timeline_version']).toBe('5');
	});

	it('writes nothing when closing a pre-authority hash', async () => {
		await redis.hset(authorityKey, 'heartbeat_ms', '5');
		const before = await snapshot();
		expect(await timeline.close(TOKEN, 'coordinator_down', 1_000)).toEqual({
			status: 'already_closed',
		});
		expect(await snapshot()).toBe(before);
	});

	it('refuses every write with a wrong token and changes nothing', async () => {
		await timeline.commit(TOKEN, 'adsbfi', 1_000);
		const before = await snapshot();

		expect(await timeline.credit('token-b', 'adsbfi', 2_000)).toEqual({ status: 'lease_mismatch' });
		expect(await timeline.close('token-b', 'failure', 2_000)).toEqual({ status: 'lease_mismatch' });
		await redis.del(leaseKey); // no lease at all
		expect(await timeline.credit(TOKEN, 'adsbfi', 3_000)).toEqual({ status: 'lease_mismatch' });
		await redis.set(leaseKey, TOKEN, 'PX', 60_000);

		expect(await snapshot()).toBe(before);
	});

	it('CREDIT for a provider without authority is benign and writes nothing', async () => {
		await redis.hset(authorityKey, 'provider', 'opensky', 'epoch', '2', 'timeline_version', '7');
		const before = await snapshot();
		expect(await timeline.credit(TOKEN, 'adsbfi', 1_000)).toEqual({
			status: 'authority_changed',
			authority: 'opensky',
		});
		await redis.hset(authorityKey, 'provider', 'none');
		expect(await timeline.credit(TOKEN, 'opensky', 1_000)).toEqual({
			status: 'authority_changed',
			authority: 'none',
		});
		await redis.hset(authorityKey, 'provider', 'opensky');
		expect(await snapshot()).toBe(before);
	});

	// ---- Authority: RELINQUISH and COMMIT (ADR-022 section 4, CP3e) ----

	it('RELINQUISH with coverage already closed: none, epoch kept, one revision', async () => {
		await timeline.commit(TOKEN, 'adsbfi', 1_000);
		await timeline.credit(TOKEN, 'adsbfi', 1_000);
		await timeline.credit(TOKEN, 'adsbfi', 2_000);
		await timeline.close(TOKEN, 'failure', 2_500); // the first failed cycle
		expect(await timeline.relinquish(TOKEN, 'adsbfi', 3_000)).toEqual({
			status: 'relinquished',
			timelineVersion: 4,
			member: null,
		});
		expect(await authority()).toMatchObject({
			provider: 'none',
			epoch: '1',
			authority_since_ms: '3000',
			coverage_open_since_ms: '',
			last_active_success_ms: '2000',
			timeline_version: '4',
		});
		expect(await coverage()).toEqual(['adsbfi|1000|2000|failure', '2000']);
	});

	it('RELINQUISH closes an open segment as failure in the same revision', async () => {
		await timeline.commit(TOKEN, 'opensky', 4_000);
		await timeline.credit(TOKEN, 'opensky', 4_000);
		await timeline.credit(TOKEN, 'opensky', 5_000);
		expect(await timeline.relinquish(TOKEN, 'opensky', 6_000)).toEqual({
			status: 'relinquished',
			timelineVersion: 3,
			member: 'opensky|4000|5000|failure',
		});
		expect(await coverage()).toEqual(['opensky|4000|5000|failure', '5000']);
		expect((await authority())['provider']).toBe('none');
	});

	it('RELINQUISH of a zero-length open segment writes no member', async () => {
		await timeline.commit(TOKEN, 'adsbfi', 1_000);
		await timeline.credit(TOKEN, 'adsbfi', 1_000);
		expect(await timeline.relinquish(TOKEN, 'adsbfi', 2_000)).toEqual({
			status: 'relinquished',
			timelineVersion: 3,
			member: null,
		});
		expect(await coverage()).toEqual([]);
	});

	it('RELINQUISH refuses a backwards open segment, a wrong token, a wrong provider and a record with no authority', async () => {
		await redis.hset(
			authorityKey,
			'provider',
			'adsbfi',
			'epoch',
			'1',
			'coverage_open_since_ms',
			'5000',
			'last_active_success_ms',
			'4000',
			'timeline_version',
			'3',
		);
		let before = await snapshot();
		await expect(timeline.relinquish(TOKEN, 'adsbfi', 6_000)).rejects.toThrow(
			/before coverage_open_since_ms/,
		);
		expect(await timeline.relinquish('token-b', 'adsbfi', 6_000)).toEqual({
			status: 'lease_mismatch',
		});
		expect(await timeline.relinquish(TOKEN, 'opensky', 6_000)).toEqual({
			status: 'unexpected_provider',
			authority: 'adsbfi',
		});
		expect(await snapshot()).toBe(before);

		await redis.del(authorityKey);
		await redis.hset(authorityKey, 'heartbeat_ms', '5');
		before = await snapshot();
		expect(await timeline.relinquish(TOKEN, 'adsbfi', 6_000)).toEqual({
			status: 'not_initialized',
		});
		expect(await snapshot()).toBe(before);
	});

	it('COMMIT from none increments epoch once and leaves coverage for CREDIT', async () => {
		await timeline.commit(TOKEN, 'adsbfi', 1_000);
		await timeline.credit(TOKEN, 'adsbfi', 1_000);
		await timeline.credit(TOKEN, 'adsbfi', 2_000);
		await timeline.relinquish(TOKEN, 'adsbfi', 3_000);
		expect(await timeline.commit(TOKEN, 'opensky', 4_000)).toEqual({
			status: 'committed',
			epoch: 2,
			timelineVersion: 4,
		});
		expect(await authority()).toMatchObject({
			provider: 'opensky',
			epoch: '2',
			authority_since_ms: '4000',
			coverage_open_since_ms: '',
			last_active_success_ms: '2000',
		});
		expect(await timeline.credit(TOKEN, 'opensky', 4_000)).toEqual({
			status: 'opened',
			timelineVersion: 5,
		});
		expect(await timeline.credit(TOKEN, 'opensky', 5_000)).toEqual({
			status: 'extended',
			timelineVersion: 5,
		});
	});

	it('COMMIT never takes over a provider that holds authority', async () => {
		await timeline.commit(TOKEN, 'opensky', 1_000);
		const before = await snapshot();
		expect(await timeline.commit(TOKEN, 'adsbfi', 2_000)).toEqual({
			status: 'not_none',
			authority: 'opensky',
		});
		expect(await timeline.commit('token-b', 'adsbfi', 2_000)).toEqual({ status: 'lease_mismatch' });
		expect(await snapshot()).toBe(before);
	});

	it('HANDOVER changes exactly the expected live authority after coverage is closed', async () => {
		await timeline.commit(TOKEN, 'opensky', 1_000);
		await timeline.credit(TOKEN, 'opensky', 1_000);
		await timeline.credit(TOKEN, 'opensky', 2_000);
		expect((await timeline.close(TOKEN, 'handover_attempt', 2_500)).status).toBe('closed');

		expect(await timeline.handover(TOKEN, 'opensky', 'adsbfi', 3_000)).toEqual({
			status: 'handed_over',
			epoch: 2,
			timelineVersion: 4,
		});
		expect(await authority()).toMatchObject({
			provider: 'adsbfi',
			epoch: '2',
			authority_since_ms: '3000',
			coverage_open_since_ms: '',
			last_active_success_ms: '2000',
			timeline_version: '4',
		});
		expect(await coverage()).toEqual(['opensky|1000|2000|handover_attempt', '2000']);
	});

	it('HANDOVER refuses an open segment, a wrong authority, a stale time and a wrong lease', async () => {
		await timeline.commit(TOKEN, 'opensky', 1_000);
		await timeline.credit(TOKEN, 'opensky', 1_000);
		expect(await timeline.handover(TOKEN, 'opensky', 'adsbfi', 2_000)).toEqual({
			status: 'coverage_open',
		});

		await timeline.close(TOKEN, 'handover_attempt', 2_500);
		const before = await snapshot();
		expect(await timeline.handover(TOKEN, 'adsbfi', 'opensky', 2_000)).toEqual({
			status: 'unexpected_provider',
			authority: 'opensky',
		});
		expect(await timeline.handover(TOKEN, 'opensky', 'adsbfi', 1_000)).toEqual({
			status: 'stale_clock',
		});
		expect(await timeline.handover('token-b', 'opensky', 'adsbfi', 2_000)).toEqual({
			status: 'lease_mismatch',
		});
		expect(await snapshot()).toBe(before);
	});

	it('COMMIT refuses a time that is not after the last success: equal or backwards', async () => {
		await timeline.commit(TOKEN, 'adsbfi', 1_000);
		await timeline.credit(TOKEN, 'adsbfi', 1_000);
		await timeline.credit(TOKEN, 'adsbfi', 2_000);
		await timeline.relinquish(TOKEN, 'adsbfi', 3_000);
		const before = await snapshot();
		expect(await timeline.commit(TOKEN, 'opensky', 2_000)).toEqual({ status: 'stale_clock' });
		expect(await timeline.commit(TOKEN, 'opensky', 1_500)).toEqual({ status: 'stale_clock' });
		expect(await snapshot()).toBe(before);
		// Once the time is newer, the same commit succeeds.
		expect((await timeline.commit(TOKEN, 'opensky', 2_001)).status).toBe('committed');
	});

	it('prunes segments whose end is older than the retention, only when it closes one', async () => {
		await redis.zadd(coverageKey, 100, 'adsbfi|50|100|failure', 200, 'adsbfi|150|200|failure');
		await timeline.commit(TOKEN, 'adsbfi', 1_000);
		await timeline.credit(TOKEN, 'adsbfi', 1_000);
		await timeline.credit(TOKEN, 'adsbfi', 2_000);

		// Cutoff = 100_200 - 100_000 = 200: an end strictly before 200 is pruned,
		// an end exactly at the cutoff is kept.
		const result = await timeline.close(TOKEN, 'failure', 100_200);
		expect(result).toMatchObject({ status: 'closed', pruned: 1 });
		expect(await coverage()).toEqual([
			'adsbfi|150|200|failure',
			'200',
			'adsbfi|1000|2000|failure',
			'2000',
		]);
	});
});

describe('Coordinator with the real lease and timeline', () => {
	async function waitFor(condition: () => Promise<boolean>, timeoutMs: number): Promise<void> {
		const deadline = Date.now() + timeoutMs;
		while (!(await condition())) {
			if (Date.now() > deadline) throw new Error(`condition not met within ${timeoutMs} ms`);
			await new Promise((resolve) => setTimeout(resolve, 20));
		}
	}

	it('bootstraps, extends, closes on a failed publish and on a frozen feed, recovers, and closes on shutdown', async () => {
		await redis.del(leaseKey); // the coordinator acquires its own lease
		// The timeout is far above the CLIENT PAUSE used by the fail-closed tests
		// in coordinatorLease.integration.test.ts, which vitest may run in
		// parallel with this file: a pause there must delay this test, not fail it.
		const client = new Redis(REDIS_URL, { commandTimeout: 5_000 });
		let now = 1790283486000;
		let advance = true;
		let publishFails = false;
		let published = 0;

		const coordinator = new Coordinator({
			lease: new CoordinatorLease(client, 5_000, leaseKey, authorityKey),
			// Provider health is not under test here: a store that always
			// writes and has nothing stored, without touching Redis.
			// Real health on this test's keys, so the acquisition snapshot sees
			// the same authority record as the timeline.
			health: new ProviderHealthStore(client, leaseKey, authorityKey, {
				adsbfi: `${leaseKey}:health:adsbfi`,
				opensky: `${leaseKey}:health:opensky`,
			}),
			// OpenSky is down, so adsb.fi makes the first commit.
			fetchOpensky: async () => ({ kind: 'failed' as const, error: 'http_503' }),
			openskyAuthenticated: true,
			healthTiming: { degradedTimeoutMs: 60_000, recoveryWindowMs: 120_000 },
			openskyCadence: {
				healthyMs: 900_000,
				degradedMs: 30_000,
				recoveringMs: 25_000,
				backoffBaseMs: 60_000,
				backoffMaxMs: 900_000,
			},
			timeline: new CoverageTimeline(client, RETENTION_MS, leaseKey, authorityKey, coverageKey),
			fetchCycle: async (): Promise<SplitResult> => {
				if (advance) now += 1_000;
				return {
					messages: [{ key: 'abc123', value: '{}' }],
					responseNowMs: now,
					total: 1,
					skippedNonIcao: 0,
					skippedNoPosition: 0,
					skippedOutsideBox: 0,
				};
			},
			publish: async () => {
				if (publishFails) throw new Error('broker unavailable');
				published++;
				return '0';
			},
			log: () => {},
			renewalIntervalMs: 500,
			followerRetryMs: 500,
			pollIntervalMs: 30,
			backoffBaseMs: 30,
			backoffMaxMs: 30,
			frozenFeedMs: 300,
			adsbfiStandbyIntervalMs: 30,
			openskyActiveIntervalMs: 25_000,
			selectionRetryBaseMs: 60_000,
			selectionRetryMaxMs: 900_000,
			failbackMinOpenskyAuthorityMs: 300_000,
		});

		const version = async () => Number((await authority())['timeline_version'] ?? 0);
		const field = async (name: string) => (await authority())[name];
		try {
			coordinator.start();
			// No authority was ever committed: selection. The first valid
			// adsb.fi response may commit epoch 1 even though it only seeds
			// freshness; coverage remains closed until a later fresh cycle.
			await waitFor(async () => (await version()) === 1, 5_000);
			expect(await field('provider')).toBe('adsbfi');
			expect(await field('epoch')).toBe('1');
			expect(await field('coverage_open_since_ms')).toBe('');

			// The next fresh active cycle CREDITs and opens coverage as a
			// separate revision. Later successful cycles only extend it.
			await waitFor(async () => (await version()) === 2, 5_000);
			const firstSuccess = Number(await field('last_active_success_ms'));
			await waitFor(
				async () => Number(await field('last_active_success_ms')) > firstSuccess,
				5_000,
			);
			expect(await version()).toBe(2);

			// A failed publish closes at the previous last success.
			publishFails = true;
			await waitFor(async () => (await version()) === 3, 5_000);
			const lastBeforeFailure = await field('last_active_success_ms');
			const [failureMember] = await redis.zrange(coverageKey, '0', '0');
			expect(failureMember).toMatch(new RegExp(`^adsbfi\\|\\d+\\|${lastBeforeFailure}\\|failure$`));

			// Recovery opens a new segment.
			publishFails = false;
			await waitFor(async () => (await version()) === 4, 5_000);
			expect(await field('coverage_open_since_ms')).not.toBe('');
			// Let it extend at least once, so the frozen close below has length.
			await waitFor(
				async () =>
					Number(await field('last_active_success_ms')) >
					Number(await field('coverage_open_since_ms')),
				5_000,
			);

			// A frozen feed never extends the segment, then closes it at the
			// last cycle whose now advanced.
			advance = false;
			await waitFor(async () => (await version()) === 5, 5_000);
			const frozenEnd = await field('last_active_success_ms');
			const members = await redis.zrange(coverageKey, '0', '-1');
			expect(members.at(-1)).toMatch(new RegExp(`\\|${frozenEnd}\\|failure$`));
			const publishedWhileFrozen = published;
			await new Promise((resolve) => setTimeout(resolve, 200));
			expect(await field('last_active_success_ms')).toBe(frozenEnd);
			expect(await version()).toBe(5); // repeated frozen failures are no-ops
			expect(published).toBe(publishedWhileFrozen); // frozen cycles are not published

			// Advancing again reopens, and clean shutdown closes it.
			advance = true;
			await waitFor(async () => (await version()) === 6, 5_000);
			// One extension, so the shutdown close has length and writes a member.
			await waitFor(
				async () =>
					Number(await field('last_active_success_ms')) >
					Number(await field('coverage_open_since_ms')),
				5_000,
			);
		} finally {
			await coordinator.shutdown();
			await client.quit();
		}
		expect(await version()).toBe(7);
		const members = await redis.zrange(coverageKey, '0', '-1');
		expect(members.at(-1)).toMatch(/\|coordinator_shutdown$/);
		expect(await field('coverage_open_since_ms')).toBe('');
		expect(await redis.exists(leaseKey)).toBe(0);
	}, 40_000);
});
