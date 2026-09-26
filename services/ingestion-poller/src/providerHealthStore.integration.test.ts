// Integration tests for provider health persistence against a REAL Redis.
// The guarantees (lease check, whole-record replacement, one-instant
// acquisition read) live in Redis, so a mocked client would only prove the
// mock. The end-to-end test drives a real Coordinator, lease, coverage
// timeline and health store together to show that health never moves
// authority.
//
// Every test uses its own random hash-tagged keys, never the real
// {live-provider} keys. Requires Redis at REDIS_URL (default
// redis://localhost:6379): `make up` or the CI service container.
import { randomUUID } from 'node:crypto';
import { Redis } from 'ioredis';
import { afterAll, afterEach, beforeEach, describe, expect, it } from 'vitest';
import type { AdsbfiFetchFailure, SplitResult } from './adsbfiPoller.js';
import { Coordinator } from './coordinator.js';
import { CoordinatorLease } from './coordinatorLease.js';
import { CoverageTimeline } from './coverageTimeline.js';
import type { Provider, ProviderHealth } from './providerHealth.js';
import { ProviderHealthStore } from './providerHealthStore.js';

const REDIS_URL = process.env['REDIS_URL'] ?? 'redis://localhost:6379';
const redis = new Redis(REDIS_URL);

const TOKEN = 'token-a';

let tag: string;
let leaseKey: string;
let authorityKey: string;
let coverageKey: string;
let healthKeys: Record<Provider, string>;
let store: ProviderHealthStore;

const health = (overrides: Partial<ProviderHealth>): ProviderHealth => ({
	state: 'HEALTHY',
	stateSinceMs: 1790400000000,
	lastSuccessMs: 1790400000000,
	lastFailureMs: null,
	consecutiveFailures: 0,
	lastError: null,
	successStreakSinceMs: null,
	pausedUntilMs: null,
	creditsRemaining: null,
	lastProbeMs: null,
	...overrides,
});

async function waitFor(check: () => Promise<boolean>, timeoutMs = 5_000): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	while (Date.now() < deadline) {
		if (await check()) return;
		await new Promise((r) => setTimeout(r, 20));
	}
	throw new Error(`condition not met within ${timeoutMs} ms`);
}

beforeEach(async () => {
	tag = `{test-health-${randomUUID()}}`;
	leaseKey = `${tag}:lease`;
	authorityKey = `${tag}:authority`;
	coverageKey = `${tag}:coverage`;
	healthKeys = { adsbfi: `${tag}:health:adsbfi`, opensky: `${tag}:health:opensky` };
	store = new ProviderHealthStore(redis, leaseKey, authorityKey, healthKeys);
	await redis.set(leaseKey, TOKEN, 'PX', 60_000);
});

afterEach(async () => {
	await redis.del(leaseKey, authorityKey, coverageKey, healthKeys.adsbfi, healthKeys.opensky);
});

afterAll(async () => {
	await redis.quit();
});

describe('ProviderHealthStore against real Redis', () => {
	it('writes exactly the ADR-022 fields, OpenSky with its three extra', async () => {
		expect(await store.write(TOKEN, 'adsbfi', health({ lastError: 'http_400' }))).toBe('written');
		expect(await redis.hgetall(healthKeys.adsbfi)).toEqual({
			state: 'HEALTHY',
			state_since_ms: '1790400000000',
			last_success_ms: '1790400000000',
			last_failure_ms: '',
			consecutive_failures: '0',
			last_error: 'http_400',
			success_streak_since_ms: '',
		});
		await store.write(
			TOKEN,
			'opensky',
			health({ creditsRemaining: 399, lastProbeMs: 1790400000001 }),
		);
		expect(Object.keys(await redis.hgetall(healthKeys.opensky)).sort()).toEqual(
			[
				'consecutive_failures',
				'credits_remaining',
				'last_error',
				'last_failure_ms',
				'last_probe_ms',
				'last_success_ms',
				'paused_until_ms',
				'state',
				'state_since_ms',
				'success_streak_since_ms',
			].sort(),
		);
	});

	it('replaces the whole record, so no field of an earlier state survives', async () => {
		await redis.hset(healthKeys.adsbfi, 'leftover', 'x');
		await store.write(TOKEN, 'adsbfi', health({ state: 'RECOVERING', successStreakSinceMs: 5 }));
		await store.write(TOKEN, 'adsbfi', health({ state: 'HEALTHY' }));
		const record = await redis.hgetall(healthKeys.adsbfi);
		expect(record['leftover']).toBeUndefined();
		expect(record['success_streak_since_ms']).toBe('');
	});

	it('writes nothing under a wrong or empty token', async () => {
		await store.write(TOKEN, 'adsbfi', health({ state: 'DEGRADED' }));
		const before = JSON.stringify([
			await redis.hgetall(healthKeys.adsbfi),
			await redis.get(leaseKey),
		]);
		expect(await store.write('token-b', 'adsbfi', health({ state: 'HEALTHY' }))).toBe(
			'lease_mismatch',
		);
		expect(await store.write('', 'adsbfi', health({ state: 'HEALTHY' }))).toBe('lease_mismatch');
		const after = JSON.stringify([
			await redis.hgetall(healthKeys.adsbfi),
			await redis.get(leaseKey),
		]);
		expect(after).toBe(before);
	});

	it('reads authority initialization and both health records from one instant', async () => {
		// Heartbeat-only authority and no health: a true first deployment.
		await redis.hset(authorityKey, 'heartbeat_ms', '1790400000000');
		let snapshot = await store.readForAcquisition();
		expect(snapshot.authorityInitialized).toBe(false);
		expect(snapshot.stored.adsbfi).toEqual({ health: null, problem: null, present: false });

		// Initialized authority with an adsb.fi record and an unreadable OpenSky one.
		await redis.hset(
			authorityKey,
			'provider',
			'adsbfi',
			'epoch',
			'1',
			'authority_since_ms',
			'1790400000123',
		);
		await store.write(TOKEN, 'adsbfi', health({ state: 'RECOVERING', successStreakSinceMs: 7 }));
		await redis.hset(healthKeys.opensky, 'state', 'SIDEWAYS');
		snapshot = await store.readForAcquisition();
		expect(snapshot).toMatchObject({
			authorityInitialized: true,
			authorityProvider: 'adsbfi',
			authoritySinceMs: 1790400000123,
		});
		expect(snapshot.stored.adsbfi.health).toMatchObject({
			state: 'RECOVERING',
			successStreakSinceMs: 7,
		});
		expect(snapshot.stored.opensky).toMatchObject({ health: null, present: true });
		expect(snapshot.stored.opensky.problem).toContain('SIDEWAYS');
	});
});

describe('Coordinator health end to end against real Redis', () => {
	it('adsb.fi commits, fails to UNAVAILABLE, relinquishes to none, then recovers and commits again', async () => {
		await redis.del(leaseKey); // the coordinator acquires it itself
		const client = new Redis(REDIS_URL, { commandTimeout: 5_000 });
		let now = 1_790_283_486_000;
		let failing = false;
		const coordinator = new Coordinator({
			lease: new CoordinatorLease(client, 5_000, leaseKey, authorityKey),
			timeline: new CoverageTimeline(client, 100_000, leaseKey, authorityKey, coverageKey),
			health: new ProviderHealthStore(client, leaseKey, authorityKey, healthKeys),
			fetchCycle: async (): Promise<SplitResult | AdsbfiFetchFailure> => {
				if (failing) return { error: 'http_503' };
				now += 1_000;
				return {
					messages: [{ key: 'abc123', value: '{}' }],
					responseNowMs: now,
					total: 1,
					skippedNonIcao: 0,
					skippedNoPosition: 0,
					skippedOutsideBox: 0,
				};
			},
			fetchOpensky: async () => ({ kind: 'failed' as const, error: 'http_503' }),
			openskyAuthenticated: true,
			// Scaled: 300 ms to UNAVAILABLE, 300 ms to recover.
			healthTiming: { degradedTimeoutMs: 300, recoveryWindowMs: 300 },
			openskyCadence: {
				healthyMs: 900_000,
				degradedMs: 30_000,
				recoveringMs: 25_000,
				backoffBaseMs: 60_000,
				backoffMaxMs: 900_000,
			},
			publish: async () => '0',
			log: () => {},
			renewalIntervalMs: 500,
			followerRetryMs: 500,
			pollIntervalMs: 30,
			backoffBaseMs: 30,
			backoffMaxMs: 60,
			frozenFeedMs: 10_000,
			adsbfiStandbyIntervalMs: 30,
			openskyActiveIntervalMs: 25_000,
			selectionRetryBaseMs: 60_000,
			selectionRetryMaxMs: 900_000,
			failbackMinOpenskyAuthorityMs: 300_000,
		});
		const state = async () => (await redis.hget(healthKeys.adsbfi, 'state')) ?? 'unknown';
		const authorityCore = async () =>
			redis.hmget(authorityKey, 'provider', 'epoch', 'authority_since_ms');

		try {
			coordinator.start();
			// No authority ever committed and no health: a true first deployment.
			// The first valid adsb.fi response seeds freshness, publishes, and
			// commits epoch 1. Coverage stays closed until the next fresh cycle.
			await waitFor(async () => (await redis.hget(authorityKey, 'provider')) === 'adsbfi');
			expect(await state()).toBe('HEALTHY');
			expect(await authorityCore()).toEqual(['adsbfi', '1', expect.any(String)]);
			expect(await redis.hget(authorityKey, 'timeline_version')).toBe('1');
			await waitFor(async () => (await redis.hget(authorityKey, 'coverage_open_since_ms')) !== '');
			expect(await redis.hget(authorityKey, 'timeline_version')).toBe('2');
			// One extension, so the failure close below has length.
			await waitFor(async () => {
				const [open, last] = await redis.hmget(
					authorityKey,
					'coverage_open_since_ms',
					'last_active_success_ms',
				);
				return Number(last) > Number(open);
			});

			failing = true;
			await waitFor(async () => (await state()) === 'DEGRADED');
			const degradedSince = await redis.hget(healthKeys.adsbfi, 'state_since_ms');
			await waitFor(async () => (await redis.hget(authorityKey, 'provider')) === 'none');
			expect(await state()).toBe('UNAVAILABLE');
			expect(Number(await redis.hget(healthKeys.adsbfi, 'state_since_ms'))).toBe(
				Number(degradedSince) + 300,
			);
			// Relinquished: literal none, epoch kept. Revisions are commit 1,
			// coverage open 2, failure close 3, relinquish 4.
			const none = await redis.hgetall(authorityKey);
			expect(none).toMatchObject({ provider: 'none', epoch: '1', coverage_open_since_ms: '' });
			expect(none['timeline_version']).toBe('4');
			const closed = await redis.zrange(coverageKey, '0', '-1');
			expect(closed).toHaveLength(1);
			expect(closed[0]).toMatch(/^adsbfi\|\d+\|\d+\|failure$/);

			// adsb.fi answers again: RECOVERING is eligible, so its next fresh
			// candidate commits epoch 2 and separately opens coverage.
			failing = false;
			await waitFor(async () => (await redis.hget(authorityKey, 'provider')) === 'adsbfi');
			expect(await redis.hget(authorityKey, 'epoch')).toBe('2');
			await waitFor(async () => (await redis.hget(authorityKey, 'coverage_open_since_ms')) !== '');
			expect(await redis.hget(authorityKey, 'timeline_version')).toBe('6');
			await waitFor(async () => (await state()) === 'HEALTHY');
			expect(await redis.hget(healthKeys.adsbfi, 'last_error')).toBe('http_503');
		} finally {
			await coordinator.shutdown();
			await client.quit();
		}
	});
});
