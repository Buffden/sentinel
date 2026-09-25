// adsb.fi authority and coverage timeline in Redis (ADR-022 sections 5 to 7).
//
// Two atomic scripts own every timeline write. Each checks the lease token
// first and writes nothing on a mismatch, so a coordinator that has lost its
// lease can never extend, reopen or close coverage its successor now owns.
// That keeps the Redis timeline consistent. It does not fence Kafka: a
// coordinator paused past its lease can still finish a send (ADR-022 section 8).
//
// Authority hash fields: provider, epoch, authority_since_ms,
// coverage_open_since_ms ("" when closed), last_active_success_ms,
// heartbeat_ms (written only by the lease renewal script) and
// timeline_version. A hash is an initialized authority record only when both
// provider and epoch exist. One holding only heartbeat_ms is pre-authority
// bootstrap state left by a coordinator that never credited a cycle.
//
// timeline_version is a revision: each script run that opens or closes a
// segment or commits authority increments it once. Extending the open segment
// does not.
//
// Coverage sorted set: one member per closed segment,
// `<provider>|<start_ms>|<end_ms>|<reason>`, scored by end_ms. The member is
// deterministic, so writing the same segment twice stores it once.
//
// Every value is passed to and stored from Lua as a string. Lua 5.1 numbers
// are doubles, and converting one back to a string can change its format, so
// arithmetic stays in TypeScript and Lua only compares with tonumber.

import type { Redis } from 'ioredis';
import { AUTHORITY_KEY, LEASE_KEY } from './coordinatorLease.js';

export const COVERAGE_KEY = '{live-provider}:coverage';

// CP3b has one provider. OpenSky authority arrives with failover.
export const ADSBFI = 'adsbfi';

export type CoverageCloseReason = 'failure' | 'coordinator_shutdown' | 'coordinator_down';

export type CreditResult =
	| { status: 'lease_mismatch' }
	// Pre-authority hash: authority committed and coverage opened, one revision.
	| { status: 'bootstrapped'; timelineVersion: number }
	// Coverage was closed: a new segment opened.
	| { status: 'opened'; timelineVersion: number }
	// Coverage was open: last_active_success_ms advanced, no new revision.
	| { status: 'extended'; timelineVersion: number }
	// The time is not after last_active_success_ms (the clock stepped back):
	// nothing written, so the timeline never moves backwards.
	| { status: 'stale_clock'; timelineVersion: number };

export type CloseResult =
	| { status: 'lease_mismatch' }
	| { status: 'closed'; member: string; timelineVersion: number; pruned: number }
	// The open segment had no length (opened by one credited cycle, closed
	// before another): coverage closed and a revision taken, but no member,
	// since a zero-length segment holds no coverage.
	| { status: 'closed_empty'; timelineVersion: number }
	// Nothing was open: nothing written, no new revision.
	| { status: 'already_closed' };

// KEYS: lease, authority. ARGV: token, success time (ms), provider.
export const CREDIT_SCRIPT = `
	if redis.call('GET', KEYS[1]) ~= ARGV[1] then
		return {'lease_mismatch'}
	end
	local t = ARGV[2]
	local provider = redis.call('HGET', KEYS[2], 'provider')
	local epoch = redis.call('HGET', KEYS[2], 'epoch')
	if not provider or not epoch then
		redis.call('HSET', KEYS[2],
			'provider', ARGV[3],
			'epoch', '1',
			'authority_since_ms', t,
			'coverage_open_since_ms', t,
			'last_active_success_ms', t)
		local v = redis.call('HINCRBY', KEYS[2], 'timeline_version', 1)
		return {'bootstrapped', v}
	end
	if provider ~= ARGV[3] then
		return redis.error_reply('authority is ' .. provider .. ', refusing to credit ' .. ARGV[3])
	end
	local last = redis.call('HGET', KEYS[2], 'last_active_success_ms')
	local v = redis.call('HGET', KEYS[2], 'timeline_version') or '0'
	if last and tonumber(t) <= tonumber(last) then
		return {'stale_clock', v}
	end
	local open = redis.call('HGET', KEYS[2], 'coverage_open_since_ms')
	if not open or open == '' then
		redis.call('HSET', KEYS[2], 'coverage_open_since_ms', t, 'last_active_success_ms', t)
		v = redis.call('HINCRBY', KEYS[2], 'timeline_version', 1)
		return {'opened', v}
	end
	redis.call('HSET', KEYS[2], 'last_active_success_ms', t)
	return {'extended', v}
`;

// KEYS: lease, authority, coverage. ARGV: token, reason, prune cutoff (ms).
// Closes at last_active_success_ms, never at the failure or crash time, so no
// known failure is ever counted as coverage. Prunes only when it writes a
// member, so a close with nothing open writes nothing at all.
//
// A segment is written only when it has length. When the last success is
// the open time itself, coverage still closes (one revision) but no member is
// written. A last success before the open time cannot come from the credit
// script, so it is refused as an invariant error rather than written
// backwards, and the caller fails closed.
export const CLOSE_SCRIPT = `
	if redis.call('GET', KEYS[1]) ~= ARGV[1] then
		return {'lease_mismatch'}
	end
	local open = redis.call('HGET', KEYS[2], 'coverage_open_since_ms')
	local provider = redis.call('HGET', KEYS[2], 'provider')
	if not open or open == '' or not provider then
		return {'already_closed'}
	end
	local last = redis.call('HGET', KEYS[2], 'last_active_success_ms')
	if not last or tonumber(last) < tonumber(open) then
		return redis.error_reply('invariant: last_active_success_ms ' .. tostring(last) ..
			' is before coverage_open_since_ms ' .. open)
	end
	if tonumber(last) == tonumber(open) then
		redis.call('HSET', KEYS[2], 'coverage_open_since_ms', '')
		local v = redis.call('HINCRBY', KEYS[2], 'timeline_version', 1)
		return {'closed_empty', v}
	end
	local member = provider .. '|' .. open .. '|' .. last .. '|' .. ARGV[2]
	redis.call('ZADD', KEYS[3], last, member)
	redis.call('HSET', KEYS[2], 'coverage_open_since_ms', '')
	local v = redis.call('HINCRBY', KEYS[2], 'timeline_version', 1)
	local pruned = redis.call('ZREMRANGEBYSCORE', KEYS[3], '-inf', '(' .. ARGV[3])
	return {'closed', member, v, pruned}
`;

export class CoverageTimeline {
	constructor(
		private readonly redis: Redis,
		private readonly retentionMs: number,
		private readonly leaseKey: string = LEASE_KEY,
		private readonly authorityKey: string = AUTHORITY_KEY,
		private readonly coverageKey: string = COVERAGE_KEY,
	) {}

	// Credit one successful, fresh active cycle whose publish finished at
	// activeSuccessMs (coordinator processing time).
	async credit(token: string, activeSuccessMs: number): Promise<CreditResult> {
		const reply = (await this.redis.eval(
			CREDIT_SCRIPT,
			2,
			this.leaseKey,
			this.authorityKey,
			token,
			String(activeSuccessMs),
			ADSBFI,
		)) as [string, (string | number)?];
		const [status, version] = reply;
		if (status === 'lease_mismatch') return { status };
		if (
			status === 'bootstrapped' ||
			status === 'opened' ||
			status === 'extended' ||
			status === 'stale_clock'
		) {
			return { status, timelineVersion: Number(version) };
		}
		throw new Error(`unexpected credit script reply: ${JSON.stringify(reply)}`);
	}

	// Close the open segment, if any, at last_active_success_ms. Idempotent:
	// with nothing open it writes nothing.
	async close(token: string, reason: CoverageCloseReason, nowMs: number): Promise<CloseResult> {
		const reply = (await this.redis.eval(
			CLOSE_SCRIPT,
			3,
			this.leaseKey,
			this.authorityKey,
			this.coverageKey,
			token,
			reason,
			String(nowMs - this.retentionMs),
		)) as [string, ...(string | number)[]];
		const [status, member, version, pruned] = reply;
		if (status === 'lease_mismatch' || status === 'already_closed') return { status };
		if (status === 'closed_empty') return { status, timelineVersion: Number(member) };
		if (status === 'closed') {
			return {
				status,
				member: String(member),
				timelineVersion: Number(version),
				pruned: Number(pruned),
			};
		}
		throw new Error(`unexpected close script reply: ${JSON.stringify(reply)}`);
	}
}
