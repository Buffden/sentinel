import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { Redis } from 'ioredis';
import { LeaderElection } from './leader.js';

// Minimal in-memory stand-in for the two ioredis calls LeaderElection makes.
// `set` implements only the PX/NX combination the class actually sends.
// `eval` mirrors the GET-then-conditional semantics of RENEW_SCRIPT and
// RELEASE_SCRIPT (distinguished by their distinctive Lua command) against the
// same in-memory store, so the ownership race conditions those scripts exist
// to prevent are exercised for real, not asserted against a trivial mock.
class FakeRedis {
	private store = new Map<string, string>();

	setOwner(key: string, owner: string): void {
		this.store.set(key, owner);
	}

	getOwner(key: string): string | undefined {
		return this.store.get(key);
	}

	async set(
		key: string,
		value: string,
		_mode: 'PX',
		_ttlMs: number,
		flag: 'NX',
	): Promise<'OK' | null> {
		if (flag === 'NX' && this.store.has(key)) return null;
		this.store.set(key, value);
		return 'OK';
	}

	async eval(script: string, _numKeys: number, key: string, instanceId: string): Promise<number> {
		if (this.store.get(key) !== instanceId) return 0;
		if (script.includes('PEXPIRE')) return 1; // renew: ownership confirmed, TTL extended
		if (script.includes('DEL')) {
			this.store.delete(key);
			return 1; // release: ownership confirmed, key removed
		}
		throw new Error('unrecognized script in FakeRedis.eval');
	}
}

const LEADER_KEY = 'alert-evaluator:leader';
const LEASE_TTL_MS = 5_000;
const RENEWAL_INTERVAL_MS = 2_000;

describe('LeaderElection', () => {
	let redis: FakeRedis;

	beforeEach(() => {
		redis = new FakeRedis();
	});

	describe('tryAcquire', () => {
		it('acquires the lease when no instance currently holds it', async () => {
			const election = new LeaderElection(
				redis as unknown as Redis,
				'instance-a',
				LEADER_KEY,
				LEASE_TTL_MS,
				RENEWAL_INTERVAL_MS,
			);
			await expect(election.tryAcquire()).resolves.toBe(true);
			expect(redis.getOwner(LEADER_KEY)).toBe('instance-a');
		});

		it('does not acquire when another instance already holds the lease', async () => {
			redis.setOwner(LEADER_KEY, 'instance-a');
			const election = new LeaderElection(
				redis as unknown as Redis,
				'instance-b',
				LEADER_KEY,
				LEASE_TTL_MS,
				RENEWAL_INTERVAL_MS,
			);
			await expect(election.tryAcquire()).resolves.toBe(false);
			// The existing owner must be untouched by the failed acquire attempt.
			expect(redis.getOwner(LEADER_KEY)).toBe('instance-a');
		});
	});

	describe('renew', () => {
		it('succeeds while this instance still owns the key', async () => {
			redis.setOwner(LEADER_KEY, 'instance-a');
			const election = new LeaderElection(
				redis as unknown as Redis,
				'instance-a',
				LEADER_KEY,
				LEASE_TTL_MS,
				RENEWAL_INTERVAL_MS,
			);
			await expect(election.renew()).resolves.toBe(true);
		});

		it('fails, and must not evict the new owner, once another instance holds the key', async () => {
			// Simulates: instance-a's lease expired, instance-b acquired it, then
			// instance-a's stale renewal timer fires.
			redis.setOwner(LEADER_KEY, 'instance-b');
			const staleElection = new LeaderElection(
				redis as unknown as Redis,
				'instance-a',
				LEADER_KEY,
				LEASE_TTL_MS,
				RENEWAL_INTERVAL_MS,
			);
			await expect(staleElection.renew()).resolves.toBe(false);
			expect(redis.getOwner(LEADER_KEY)).toBe('instance-b');
		});
	});

	describe('release', () => {
		it('removes the key when this instance owns it', async () => {
			redis.setOwner(LEADER_KEY, 'instance-a');
			const election = new LeaderElection(
				redis as unknown as Redis,
				'instance-a',
				LEADER_KEY,
				LEASE_TTL_MS,
				RENEWAL_INTERVAL_MS,
			);
			await election.release();
			expect(redis.getOwner(LEADER_KEY)).toBeUndefined();
		});

		it('does not remove the key when another instance now owns it', async () => {
			// Simulates releasing after a network partition where a new owner
			// already took over the lease.
			redis.setOwner(LEADER_KEY, 'instance-b');
			const staleElection = new LeaderElection(
				redis as unknown as Redis,
				'instance-a',
				LEADER_KEY,
				LEASE_TTL_MS,
				RENEWAL_INTERVAL_MS,
			);
			await staleElection.release();
			expect(redis.getOwner(LEADER_KEY)).toBe('instance-b');
		});
	});

	describe('startRenewal', () => {
		beforeEach(() => {
			vi.useFakeTimers();
		});

		afterEach(() => {
			vi.useRealTimers();
		});

		it('keeps renewing on the interval while ownership holds', async () => {
			redis.setOwner(LEADER_KEY, 'instance-a');
			const election = new LeaderElection(
				redis as unknown as Redis,
				'instance-a',
				LEADER_KEY,
				LEASE_TTL_MS,
				RENEWAL_INTERVAL_MS,
			);
			const onLeaseLost = vi.fn();
			election.startRenewal(onLeaseLost);

			await vi.advanceTimersByTimeAsync(RENEWAL_INTERVAL_MS * 3);

			expect(onLeaseLost).not.toHaveBeenCalled();
			expect(redis.getOwner(LEADER_KEY)).toBe('instance-a');
			election.stopRenewal();
		});

		it('calls onLeaseLost and stops renewing once ownership is lost', async () => {
			redis.setOwner(LEADER_KEY, 'instance-a');
			const election = new LeaderElection(
				redis as unknown as Redis,
				'instance-a',
				LEADER_KEY,
				LEASE_TTL_MS,
				RENEWAL_INTERVAL_MS,
			);
			const onLeaseLost = vi.fn();
			election.startRenewal(onLeaseLost);

			// Simulate another instance taking over the key between renewals.
			redis.setOwner(LEADER_KEY, 'instance-b');
			await vi.advanceTimersByTimeAsync(RENEWAL_INTERVAL_MS);

			expect(onLeaseLost).toHaveBeenCalledTimes(1);

			// A further interval tick must not call onLeaseLost again — the
			// renewal loop must have actually stopped, not just skipped once.
			await vi.advanceTimersByTimeAsync(RENEWAL_INTERVAL_MS * 2);
			expect(onLeaseLost).toHaveBeenCalledTimes(1);
		});

		it('fails closed and treats a Redis error as lease loss', async () => {
			redis.setOwner(LEADER_KEY, 'instance-a');
			const election = new LeaderElection(
				redis as unknown as Redis,
				'instance-a',
				LEADER_KEY,
				LEASE_TTL_MS,
				RENEWAL_INTERVAL_MS,
			);
			const onLeaseLost = vi.fn();
			election.startRenewal(onLeaseLost);

			vi.spyOn(redis, 'eval').mockRejectedValueOnce(new Error('connection reset'));
			await vi.advanceTimersByTimeAsync(RENEWAL_INTERVAL_MS);

			expect(onLeaseLost).toHaveBeenCalledTimes(1);
		});
	});
});
