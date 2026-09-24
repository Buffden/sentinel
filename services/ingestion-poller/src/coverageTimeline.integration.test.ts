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
	await redis.del(leaseKey, authorityKey, coverageKey);
});

afterAll(async () => {
	await redis.quit();
});

describe('CoverageTimeline against real Redis', () => {
	it('bootstraps a heartbeat-only hash: commits adsbfi and opens coverage as one revision', async () => {
		await redis.hset(authorityKey, 'heartbeat_ms', '1790283444182');

		expect(await timeline.credit(TOKEN, 1_000)).toEqual({
			status: 'bootstrapped',
			timelineVersion: 1,
		});
		expect(await authority()).toEqual({
			heartbeat_ms: '1790283444182',
			provider: 'adsbfi',
			epoch: '1',
			authority_since_ms: '1000',
			coverage_open_since_ms: '1000',
			last_active_success_ms: '1000',
			timeline_version: '1',
		});
	});

	it('bootstraps with no hash at all, and stores 13-digit times exactly', async () => {
		const t = 1790283486599;
		expect(await timeline.credit(TOKEN, t)).toEqual({ status: 'bootstrapped', timelineVersion: 1 });
		expect((await authority())['last_active_success_ms']).toBe(String(t));
	});

	it('treats a hash with provider but no epoch as not initialized', async () => {
		await redis.hset(authorityKey, 'provider', 'adsbfi', 'heartbeat_ms', '5');
		expect((await timeline.credit(TOKEN, 1_000)).status).toBe('bootstrapped');
		expect((await authority())['epoch']).toBe('1');
	});

	it('extends the open segment without a new revision, and never moves backwards', async () => {
		await timeline.credit(TOKEN, 1_000);
		expect(await timeline.credit(TOKEN, 3_000)).toEqual({ status: 'extended', timelineVersion: 1 });
		expect(await timeline.credit(TOKEN, 2_000)).toEqual({
			status: 'stale_clock',
			timelineVersion: 1,
		});
		expect(await timeline.credit(TOKEN, 3_000)).toEqual({
			status: 'stale_clock',
			timelineVersion: 1,
		});

		const a = await authority();
		expect(a['coverage_open_since_ms']).toBe('1000');
		expect(a['last_active_success_ms']).toBe('3000');
		expect(a['timeline_version']).toBe('1');
	});

	it('closes as failure at last_active_success_ms, then later closes are no-ops', async () => {
		await timeline.credit(TOKEN, 1_000);
		await timeline.credit(TOKEN, 3_000);

		expect(await timeline.close(TOKEN, 'failure', 4_000)).toEqual({
			status: 'closed',
			member: 'adsbfi|1000|3000|failure',
			timelineVersion: 2,
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

	it('opens a new segment on recovery, keeping epoch and authority_since_ms', async () => {
		await timeline.credit(TOKEN, 1_000);
		await timeline.close(TOKEN, 'failure', 1_500);

		expect(await timeline.credit(TOKEN, 5_000)).toEqual({ status: 'opened', timelineVersion: 3 });
		const a = await authority();
		expect(a['coverage_open_since_ms']).toBe('5000');
		expect(a['last_active_success_ms']).toBe('5000');
		expect(a['epoch']).toBe('1');
		expect(a['authority_since_ms']).toBe('1000');
	});

	it('refuses to open a segment that would start before the last success', async () => {
		await timeline.credit(TOKEN, 5_000);
		await timeline.close(TOKEN, 'failure', 6_000);
		expect(await timeline.credit(TOKEN, 4_000)).toEqual({
			status: 'stale_clock',
			timelineVersion: 2,
		});
		expect((await authority())['coverage_open_since_ms']).toBe('');
	});

	it('records coordinator_shutdown and coordinator_down as the close reason', async () => {
		await timeline.credit(TOKEN, 1_000);
		await timeline.close(TOKEN, 'coordinator_shutdown', 2_000);
		await timeline.credit(TOKEN, 3_000);
		await timeline.credit(TOKEN, 4_000);
		await timeline.close(TOKEN, 'coordinator_down', 5_000);

		expect(await coverage()).toEqual([
			'adsbfi|1000|1000|coordinator_shutdown',
			'1000',
			'adsbfi|3000|4000|coordinator_down',
			'4000',
		]);
		expect((await authority())['timeline_version']).toBe('4');
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
		await timeline.credit(TOKEN, 1_000);
		const before = await snapshot();

		expect(await timeline.credit('token-b', 2_000)).toEqual({ status: 'lease_mismatch' });
		expect(await timeline.close('token-b', 'failure', 2_000)).toEqual({ status: 'lease_mismatch' });
		await redis.del(leaseKey); // no lease at all
		expect(await timeline.credit(TOKEN, 3_000)).toEqual({ status: 'lease_mismatch' });
		await redis.set(leaseKey, TOKEN, 'PX', 60_000);

		expect(await snapshot()).toBe(before);
	});

	it('refuses to credit adsb.fi while another provider holds authority', async () => {
		await redis.hset(authorityKey, 'provider', 'opensky', 'epoch', '2', 'timeline_version', '7');
		const before = await snapshot();
		await expect(timeline.credit(TOKEN, 1_000)).rejects.toThrow(/refusing to credit adsbfi/);
		expect(await snapshot()).toBe(before);
	});

	it('prunes segments whose end is older than the retention, only when it closes one', async () => {
		await redis.zadd(coverageKey, 100, 'adsbfi|50|100|failure', 200, 'adsbfi|150|200|failure');
		await timeline.credit(TOKEN, 1_000);
		await timeline.credit(TOKEN, 2_000);

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
		});

		const version = async () => Number((await authority())['timeline_version'] ?? 0);
		const field = async (name: string) => (await authority())[name];
		try {
			coordinator.start();
			// The first cycle only seeds freshness; the second bootstraps.
			await waitFor(async () => (await version()) === 1, 5_000);
			expect(await field('provider')).toBe('adsbfi');
			expect(await field('epoch')).toBe('1');

			// Normal cycles extend without a new revision.
			const firstSuccess = Number(await field('last_active_success_ms'));
			await waitFor(
				async () => Number(await field('last_active_success_ms')) > firstSuccess,
				5_000,
			);
			expect(await version()).toBe(1);

			// A failed publish closes at the previous last success.
			publishFails = true;
			await waitFor(async () => (await version()) === 2, 5_000);
			const lastBeforeFailure = await field('last_active_success_ms');
			const [failureMember] = await redis.zrange(coverageKey, '0', '0');
			expect(failureMember).toMatch(new RegExp(`^adsbfi\\|\\d+\\|${lastBeforeFailure}\\|failure$`));

			// Recovery opens a new segment.
			publishFails = false;
			await waitFor(async () => (await version()) === 3, 5_000);
			expect(await field('coverage_open_since_ms')).not.toBe('');

			// A frozen feed never extends the segment, then closes it at the
			// last cycle whose now advanced.
			advance = false;
			await waitFor(async () => (await version()) === 4, 5_000);
			const frozenEnd = await field('last_active_success_ms');
			const members = await redis.zrange(coverageKey, '0', '-1');
			expect(members.at(-1)).toMatch(new RegExp(`\\|${frozenEnd}\\|failure$`));
			const publishedWhileFrozen = published;
			await new Promise((resolve) => setTimeout(resolve, 200));
			expect(await field('last_active_success_ms')).toBe(frozenEnd);
			expect(await version()).toBe(4); // repeated frozen failures are no-ops
			expect(published).toBe(publishedWhileFrozen); // frozen cycles are not published

			// Advancing again reopens, and clean shutdown closes it.
			advance = true;
			await waitFor(async () => (await version()) === 5, 5_000);
		} finally {
			await coordinator.shutdown();
			await client.quit();
		}
		expect(await version()).toBe(6);
		const members = await redis.zrange(coverageKey, '0', '-1');
		expect(members.at(-1)).toMatch(/\|coordinator_shutdown$/);
		expect(await field('coverage_open_since_ms')).toBe('');
		expect(await redis.exists(leaseKey)).toBe(0);
	}, 40_000);
});
