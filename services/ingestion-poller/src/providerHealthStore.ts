// Provider health in Redis (ADR-022 section 7).
//
// One hash per provider, {live-provider}:health:<provider>, with exactly the
// ADR's fields (providerHealth.ts toHashFields). The coordinator is the only
// writer, and every write is one script that checks the lease token first
// and writes nothing on a mismatch, the same rule as the coverage timeline.
//
// Health is persisted so a restart can restore it and the coordinator can
// make authority decisions. The Alert Evaluator never uses it for signal loss.

import type { Redis } from 'ioredis';
import { AUTHORITY_KEY, LEASE_KEY } from './coordinatorLease.js';
import {
	parseHealthHash,
	toHashFields,
	type Provider,
	type ProviderHealth,
} from './providerHealth.js';

export const PROVIDERS: readonly Provider[] = ['adsbfi', 'opensky'];

export const HEALTH_KEYS: Readonly<Record<Provider, string>> = {
	adsbfi: '{live-provider}:health:adsbfi',
	opensky: '{live-provider}:health:opensky',
};

// KEYS: lease, health hash. ARGV: token, then field/value pairs.
// DEL then HSET in one script replaces the whole record, so no field from an
// earlier state survives and no reader sees half a transition.
export const HEALTH_WRITE_SCRIPT = `
	if redis.call('GET', KEYS[1]) ~= ARGV[1] then
		return 'lease_mismatch'
	end
	redis.call('DEL', KEYS[2])
	redis.call('HSET', KEYS[2], unpack(ARGV, 2))
	return 'written'
`;

export interface StoredHealth {
	health: ProviderHealth | null;
	// Set when a record exists but cannot be read; it is then treated as unknown.
	problem: string | null;
	// Whether the hash held any state at all, readable or not.
	present: boolean;
}

export interface AcquisitionSnapshot {
	// Initialized means both provider and epoch exist (the CP3b rule).
	authorityInitialized: boolean;
	authorityProvider: string | null;
	// Null when the timeline has never had authority, or when an older/malformed
	// record lacks a usable start time. Null is conservative for failback.
	authoritySinceMs: number | null;
	stored: Record<Provider, StoredHealth>;
}

export type HealthWriteResult = 'written' | 'lease_mismatch';

export class ProviderHealthStore {
	constructor(
		private readonly redis: Redis,
		private readonly leaseKey: string = LEASE_KEY,
		private readonly authorityKey: string = AUTHORITY_KEY,
		private readonly healthKeys: Readonly<Record<Provider, string>> = HEALTH_KEYS,
	) {}

	// Everything lease acquisition needs, from one instant. Throws when any
	// part cannot be read: the coordinator then fails closed.
	async readForAcquisition(): Promise<AcquisitionSnapshot> {
		const results = await this.redis
			.multi()
			.hmget(this.authorityKey, 'provider', 'epoch', 'authority_since_ms')
			.hgetall(this.healthKeys.adsbfi)
			.hgetall(this.healthKeys.opensky)
			.exec();
		if (!results || results.length !== 3) throw new Error('unexpected MULTI/EXEC reply');
		for (const [err] of results) if (err) throw err;

		const [provider, epoch, authoritySince] = results[0]![1] as (string | null)[];
		const stored = (index: number): StoredHealth => {
			const record = results[index]![1] as Record<string, string>;
			return { ...parseHealthHash(record), present: Boolean(record['state']) };
		};
		return {
			authorityInitialized: Boolean(provider) && Boolean(epoch),
			authorityProvider: provider || null,
			authoritySinceMs:
				authoritySince !== null && /^\d+$/.test(authoritySince) ? Number(authoritySince) : null,
			stored: { adsbfi: stored(1), opensky: stored(2) },
		};
	}

	async write(
		token: string,
		provider: Provider,
		health: ProviderHealth,
	): Promise<HealthWriteResult> {
		const reply = await this.redis.eval(
			HEALTH_WRITE_SCRIPT,
			2,
			this.leaseKey,
			this.healthKeys[provider],
			token,
			...toHashFields(provider, health),
		);
		if (reply === 'written' || reply === 'lease_mismatch') return reply;
		throw new Error(`unexpected health write reply: ${JSON.stringify(reply)}`);
	}
}
