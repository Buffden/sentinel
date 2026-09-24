// Integration tests for the coordinator lease against a REAL Redis. The
// guarantees under test live in the Lua scripts (compare-and-renew,
// compare-and-delete, Redis TIME heartbeat), so a mocked client would only
// prove the mock.
//
// Every test uses its own random hash-tagged keys, never the real
// {live-provider} keys, so a coordinator running locally is not disturbed.
//
// Requires Redis at REDIS_URL (default redis://localhost:6379): `make up` or
// the CI service container.
//
// The fail-closed tests below use CLIENT PAUSE, which freezes writes and key
// expiry for the whole Redis instance. Every CLIENT PAUSE test lives in this
// file, because vitest runs files in parallel but the tests in one file in
// order: the pause cannot disturb the expiry timing of the lease tests, and
// two pausing tests can never unpause each other.
import { randomUUID } from 'node:crypto';
import { Redis } from 'ioredis';
import { afterAll, afterEach, beforeEach, describe, expect, it } from 'vitest';
import type { SplitResult } from './adsbfiPoller.js';
import { Coordinator } from './coordinator.js';
import { CoordinatorLease } from './coordinatorLease.js';
import { CoverageTimeline } from './coverageTimeline.js';

const REDIS_URL = process.env['REDIS_URL'] ?? 'redis://localhost:6379';
const redis = new Redis(REDIS_URL);

const TTL_MS = 15_000;
// Short TTL for the expiry test only.
const SHORT_TTL_MS = 300;

let leaseKey: string;
let authorityKey: string;

function leaseFor(ttlMs = TTL_MS): CoordinatorLease {
	return new CoordinatorLease(redis, ttlMs, leaseKey, authorityKey);
}

async function heartbeat(): Promise<string | null> {
	return redis.hget(authorityKey, 'heartbeat_ms');
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

beforeEach(() => {
	const tag = `{test-live-provider-${randomUUID()}}`;
	leaseKey = `${tag}:lease`;
	authorityKey = `${tag}:authority`;
});

afterEach(async () => {
	await redis.del(leaseKey, authorityKey);
});

afterAll(async () => {
	await redis.quit();
});

describe('CoordinatorLease against real Redis', () => {
	it('lets exactly one coordinator acquire, with a TTL, under a fresh token', async () => {
		const a = leaseFor();
		const b = leaseFor();

		expect(await a.tryAcquire()).toBe(true);
		expect(await b.tryAcquire()).toBe(false);

		expect(await redis.get(leaseKey)).toBe(a.token);
		expect(b.token).toBeNull();
		const pttl = await redis.pttl(leaseKey);
		expect(pttl).toBeGreaterThan(TTL_MS - 1_000);
		expect(pttl).toBeLessThanOrEqual(TTL_MS);
	});

	it('uses a different token on every acquisition', async () => {
		const a = leaseFor();
		expect(await a.tryAcquire()).toBe(true);
		const first = a.token;
		expect(await a.release()).toBe(true);
		expect(await a.tryAcquire()).toBe(true);
		expect(a.token).not.toBe(first);
	});

	it('advances heartbeat_ms from Redis TIME on each renewal, as an integer', async () => {
		const a = leaseFor();
		await a.tryAcquire();

		expect(await a.renew()).toBe(true);
		const first = await heartbeat();
		await sleep(20);
		expect(await a.renew()).toBe(true);
		const second = await heartbeat();

		expect(first).toMatch(/^\d{13}$/);
		expect(second).toMatch(/^\d{13}$/);
		expect(Number(second)).toBeGreaterThan(Number(first));
		const [seconds] = await redis.time();
		expect(Math.abs(Number(second) - Number(seconds) * 1000)).toBeLessThan(2_000);
	});

	it('writes only heartbeat_ms to the authority hash (pre-authority bootstrap state)', async () => {
		const a = leaseFor();
		await a.tryAcquire();
		await a.renew();
		expect(Object.keys(await redis.hgetall(authorityKey))).toEqual(['heartbeat_ms']);
	});

	it('extends the TTL on renewal', async () => {
		const a = leaseFor();
		await a.tryAcquire();
		await redis.pexpire(leaseKey, 1_000);
		expect(await a.renew()).toBe(true);
		expect(await redis.pttl(leaseKey)).toBeGreaterThan(TTL_MS - 1_000);
	});

	it('rejects a renewal with the wrong token without extending the lease or the heartbeat', async () => {
		// b's token comes from a real acquisition that expired, so it is a
		// genuinely wrong token by the time a holds the lease.
		const b = leaseFor(SHORT_TTL_MS);
		await b.tryAcquire();
		await sleep(SHORT_TTL_MS + 100);
		const a = leaseFor();
		await a.tryAcquire();
		await a.renew();
		const heartbeatBefore = await heartbeat();
		await redis.pexpire(leaseKey, 5_000);

		expect(await b.renew()).toBe(false);
		expect(await leaseFor().renew()).toBe(false); // no token at all
		expect(await heartbeat()).toBe(heartbeatBefore);
		expect(await redis.pttl(leaseKey)).toBeLessThanOrEqual(5_000);
		expect(await redis.get(leaseKey)).toBe(a.token);
	});

	it('lets a follower acquire after the leader stops renewing, and the old token cannot resume', async () => {
		const a = leaseFor(SHORT_TTL_MS);
		const b = leaseFor(SHORT_TTL_MS);
		await a.tryAcquire();
		expect(await b.tryAcquire()).toBe(false);

		await sleep(SHORT_TTL_MS + 100); // a "crashed": no renewal, the key expires
		expect(await b.tryAcquire()).toBe(true);

		const bPttl = await redis.pttl(leaseKey);
		expect(await a.renew()).toBe(false);
		expect(await redis.get(leaseKey)).toBe(b.token);
		expect(await redis.pttl(leaseKey)).toBeLessThanOrEqual(bPttl);
	});

	it('releases only its own lease (compare-and-delete)', async () => {
		const a = leaseFor(SHORT_TTL_MS);
		const b = leaseFor();
		await a.tryAcquire();
		await sleep(SHORT_TTL_MS + 100);
		await b.tryAcquire();

		// a still believes it holds the lease, but the key is b's.
		expect(await a.release()).toBe(false);
		expect(await redis.get(leaseKey)).toBe(b.token);

		expect(await b.release()).toBe(true);
		expect(await redis.exists(leaseKey)).toBe(0);
		expect(b.token).toBeNull();
	});
});

describe('Coordinator against real Redis', () => {
	// Scaled-down ADR-022 timings that still satisfy the config invariant:
	// 200 ms + 2 x 250 ms < 2000 ms.
	const TTL = 2_000;
	const RENEWAL = 200;
	const COMMAND_TIMEOUT = 250;
	const PAUSE = 1_000;

	async function waitFor(condition: () => boolean, timeoutMs: number): Promise<void> {
		const deadline = Date.now() + timeoutMs;
		while (!condition()) {
			if (Date.now() > deadline) throw new Error(`condition not met within ${timeoutMs} ms`);
			await sleep(20);
		}
	}

	it('fails closed when a renewal times out, without deleting the key, then leads again under a new token', async () => {
		// The coordinator gets its own client with a command timeout, as in
		// production. The shared `redis` client has none, so it can still read
		// while writes are paused.
		const leaseClient = new Redis(REDIS_URL, { commandTimeout: COMMAND_TIMEOUT });
		const lease = new CoordinatorLease(leaseClient, TTL, leaseKey, authorityKey);
		const messages: string[] = [];
		let published = 0;
		let responseNowMs = 1790283486000;
		const coordinator = new Coordinator({
			lease,
			// A timeline that always succeeds without touching Redis, so the only
			// command the pause can time out is the lease renewal under test.
			// The timeline's own fail-closed paths are covered in
			// coverageTimeline.integration.test.ts and coordinator.test.ts.
			timeline: {
				credit: async () => ({ status: 'extended', timelineVersion: 1 }),
				close: async () => ({ status: 'already_closed' }),
			},
			fetchCycle: async (): Promise<SplitResult> => ({
				messages: [{ key: 'abc123', value: '{}' }],
				responseNowMs: (responseNowMs += 1_000),
				total: 1,
				skippedNonIcao: 0,
				skippedNoPosition: 0,
				skippedOutsideBox: 0,
			}),
			publish: async () => {
				published++;
				return '0';
			},
			log: (_level, message, extra) => messages.push(`${message} ${JSON.stringify(extra ?? {})}`),
			renewalIntervalMs: RENEWAL,
			followerRetryMs: RENEWAL,
			pollIntervalMs: 50,
			backoffBaseMs: 50,
			backoffMaxMs: 50,
			frozenFeedMs: 10_000,
		});

		try {
			coordinator.start();
			await waitFor(() => coordinator.isLeader && published > 0, 2_000);
			const firstToken = lease.token;

			// Hold every write, including the renewal script, inside Redis. The
			// renewal is sent but gets no reply, so ioredis times it out: a real
			// command timeout, not a mocked rejection.
			await redis.call('CLIENT', 'PAUSE', String(PAUSE), 'WRITE');
			await waitFor(() => !coordinator.isLeader, PAUSE - 100);

			expect(
				messages.some((m) => m.startsWith('lease lost') && m.includes('Command timed out')),
			).toBe(true);
			// Fail closed without touching Redis: the key still holds the old
			// token (reads are not paused) and nothing more is published.
			expect(await redis.get(leaseKey)).toBe(firstToken);
			const publishedAtLoss = published;
			await sleep(300);
			expect(published).toBe(publishedAtLoss);

			// A timed-out renewal is not a cancelled one. Once writes resume,
			// Redis still runs it and restores the full TTL under the old token.
			// That only delays takeover by up to one TTL; the old holder has
			// already stopped.
			await redis.call('CLIENT', 'UNPAUSE');
			await sleep(50);
			expect(await redis.get(leaseKey)).toBe(firstToken);
			expect(await redis.pttl(leaseKey)).toBeGreaterThan(TTL - 500);
			expect(published).toBe(publishedAtLoss);

			// After that key expires, the coordinator acquires again under a
			// fresh token and resumes publishing.
			await waitFor(() => coordinator.isLeader && lease.token !== firstToken, TTL + 2_000);
			await waitFor(() => published > publishedAtLoss, 1_000);
			expect(await redis.get(leaseKey)).toBe(lease.token);
		} finally {
			await redis.call('CLIENT', 'UNPAUSE');
			await coordinator.shutdown();
			await leaseClient.quit();
		}
	}, 15_000);

	it('fails closed when a coverage timeline write times out, before any renewal can', async () => {
		// Renewal every 10 s under a 30 s TTL: after the acquisition's own
		// renewal, none can fall inside the pause, so the only write that can
		// time out is the timeline's. The CP3a test above covers renewal.
		const LONG_TTL = 30_000;
		const LONG_RENEWAL = 10_000;
		const coverageKey = leaseKey.replace(/:lease$/, ':coverage');
		const leaseClient = new Redis(REDIS_URL, { commandTimeout: COMMAND_TIMEOUT });
		const lease = new CoordinatorLease(leaseClient, LONG_TTL, leaseKey, authorityKey);
		const renewals: number[] = [];
		const renew = lease.renew.bind(lease);
		lease.renew = async () => {
			renewals.push(Date.now());
			return renew();
		};
		const timeline = new CoverageTimeline(leaseClient, 60_000, leaseKey, authorityKey, coverageKey);
		const credits: number[] = [];
		const messages: string[] = [];
		let published = 0;
		let responseNowMs = 1790283486000;
		const coordinator = new Coordinator({
			lease,
			timeline: {
				credit: (token, activeSuccessMs) => {
					credits.push(activeSuccessMs);
					return timeline.credit(token, activeSuccessMs);
				},
				close: (token, reason, nowMs) => timeline.close(token, reason, nowMs),
			},
			fetchCycle: async (): Promise<SplitResult> => ({
				messages: [{ key: 'abc123', value: '{}' }],
				responseNowMs: (responseNowMs += 1_000),
				total: 1,
				skippedNonIcao: 0,
				skippedNoPosition: 0,
				skippedOutsideBox: 0,
			}),
			publish: async () => {
				published++;
				return '0';
			},
			log: (_level, message, extra) => messages.push(`${message} ${JSON.stringify(extra ?? {})}`),
			renewalIntervalMs: LONG_RENEWAL,
			followerRetryMs: LONG_RENEWAL,
			pollIntervalMs: 50,
			backoffBaseMs: 50,
			backoffMaxMs: 50,
			frozenFeedMs: 10_000,
		});

		try {
			coordinator.start();
			// The first credit bootstraps authority, later ones extend.
			await waitFor(() => credits.length >= 3, 3_000);
			const firstToken = lease.token;
			const renewalsBeforePause = renewals.length;

			await redis.call('CLIENT', 'PAUSE', String(PAUSE), 'WRITE');
			await waitFor(() => !coordinator.isLeader, PAUSE - 100);
			const timedOutCreditMs = credits.at(-1);

			// It was the timeline write that timed out, not a renewal.
			expect(
				messages.some(
					(m) =>
						m.startsWith('lease lost') &&
						m.includes('timeline write error') &&
						m.includes('Command timed out'),
				),
			).toBe(true);
			expect(messages.some((m) => m.includes('renewal error'))).toBe(false);
			expect(renewals).toHaveLength(renewalsBeforePause);

			// Fail closed: the key is untouched and nothing more is published.
			expect(await redis.get(leaseKey)).toBe(firstToken);
			const publishedAtLoss = published;
			await sleep(300);
			expect(published).toBe(publishedAtLoss);

			// Like a timed-out renewal, the timed-out credit still runs once
			// writes resume. That is truthful: it credits a cycle that really
			// was published, under the token that still holds the key.
			await redis.call('CLIENT', 'UNPAUSE');
			await sleep(50);
			expect(await redis.hget(authorityKey, 'last_active_success_ms')).toBe(
				String(timedOutCreditMs),
			);
			expect(published).toBe(publishedAtLoss);
			expect(coordinator.isLeader).toBe(false);
		} finally {
			await redis.call('CLIENT', 'UNPAUSE');
			await coordinator.shutdown();
			await leaseClient.quit();
			await redis.del(coverageKey);
		}
	}, 15_000);
});
