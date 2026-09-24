// Redis lease for the ingestion coordinator (ADR-022 section 7).
//
// The lease keeps two coordinators from both working at once. It is a
// duplicate-instance guard, not fencing: Kafka never checks the token, so a
// coordinator paused past its TTL can still complete a send it had already
// started (ADR-022 section 8).
//
// Every acquisition uses a fresh random token. A stable instance name would
// let a restarted process "renew" the lease of its previous life and skip the
// expiry that is supposed to protect a successor.

import { randomUUID } from 'node:crypto';
import type { Redis } from 'ioredis';

export const LEASE_KEY = '{live-provider}:lease';
export const AUTHORITY_KEY = '{live-provider}:authority';

// Compare-and-renew. Only the current holder may extend the TTL, and the
// heartbeat is written in the same atomic step, so heartbeat_ms advancing
// always means "the holder of this exact token renewed". It is stamped with
// Redis TIME so every writer and reader shares one clock.
//
// It writes only heartbeat_ms to the authority hash. Until the coverage
// timeline exists, a hash without provider and epoch is pre-authority
// bootstrap state, not a restored authority record.
export const RENEW_SCRIPT = `
	if redis.call('GET', KEYS[1]) ~= ARGV[1] then
		return 0
	end
	redis.call('PEXPIRE', KEYS[1], ARGV[2])
	local t = redis.call('TIME')
	redis.call('HSET', KEYS[2], 'heartbeat_ms', t[1] * 1000 + math.floor(t[2] / 1000))
	return 1
`;

// Compare-and-delete. A coordinator must never delete a lease that has
// already passed to a successor.
export const RELEASE_SCRIPT = `
	if redis.call('GET', KEYS[1]) == ARGV[1] then
		return redis.call('DEL', KEYS[1])
	end
	return 0
`;

export class CoordinatorLease {
	// The token of the lease this process believes it holds, or null.
	private heldToken: string | null = null;

	constructor(
		private readonly redis: Redis,
		private readonly ttlMs: number,
		private readonly leaseKey: string = LEASE_KEY,
		private readonly authorityKey: string = AUTHORITY_KEY,
	) {}

	get token(): string | null {
		return this.heldToken;
	}

	// SET NX PX: claim and expiry in one atomic command.
	async tryAcquire(): Promise<boolean> {
		const token = randomUUID();
		const result = await this.redis.set(this.leaseKey, token, 'PX', this.ttlMs, 'NX');
		if (result !== 'OK') return false;
		this.heldToken = token;
		return true;
	}

	// True only when Redis confirmed this token still holds the lease. A 0
	// result is the only reliable signal that ownership has been lost.
	async renew(): Promise<boolean> {
		if (this.heldToken === null) return false;
		const result = await this.redis.eval(
			RENEW_SCRIPT,
			2,
			this.leaseKey,
			this.authorityKey,
			this.heldToken,
			String(this.ttlMs),
		);
		return result === 1;
	}

	// Drop local ownership without touching Redis. Used after a lost lease,
	// where the key may already belong to a successor.
	forget(): void {
		this.heldToken = null;
	}

	// Returns true when this token's lease was deleted.
	async release(): Promise<boolean> {
		const token = this.heldToken;
		this.heldToken = null;
		if (token === null) return false;
		const result = await this.redis.eval(RELEASE_SCRIPT, 1, this.leaseKey, token);
		return result === 1;
	}

	// Current holder, for follower logs only. Never used to decide ownership.
	async currentHolder(): Promise<string | null> {
		return this.redis.get(this.leaseKey);
	}
}
