// Provider authority and coverage timeline in Redis (ADR-022 sections 4 to 7).
//
// Four atomic scripts own every timeline write: COMMIT and RELINQUISH change
// authority, CREDIT and CLOSE change coverage. Each checks the lease token
// first and writes nothing on a mismatch, so a coordinator that has lost its
// lease can never extend, reopen or close coverage its successor now owns.
// That keeps the Redis timeline consistent. It does not fence Kafka: a
// coordinator paused past its lease can still finish a send (ADR-022 section 8).
//
// Authority hash fields: provider, epoch, authority_since_ms,
// coverage_open_since_ms ("" when closed), last_active_success_ms,
// heartbeat_ms (written only by the lease renewal script) and
// timeline_version. A hash is an initialized authority record only when both
// provider and epoch exist. One holding only heartbeat_ms has never had an
// authority: the coordinator treats it as `none` in memory, and the first
// COMMIT, by either provider, creates epoch 1. Once initialized, having no
// authority is stored literally as provider=none, never as an empty or
// missing field, which readers treat as an uninitialized timeline.
//
// timeline_version is a revision: each script run that opens or closes a
// segment, commits authority or relinquishes it increments it once. Extending
// the open segment does not. epoch counts commits only.
//
// Coverage sorted set: one member per closed segment,
// `<provider>|<start_ms>|<end_ms>|<reason>`, scored by end_ms. The member is
// deterministic, so writing the same segment twice stores it once.
//
// Every value is passed to and stored from Lua as a string. Lua 5.1 numbers
// are doubles, and converting one back to a string can change its format, so
// Lua only compares with tonumber. Counters use HINCRBY, which is Redis's own
// integer arithmetic.

import type { Redis } from 'ioredis';
import { AUTHORITY_KEY, LEASE_KEY } from './coordinatorLease.js';

export const COVERAGE_KEY = '{live-provider}:coverage';

export type AuthorityProvider = 'adsbfi' | 'opensky';

export type CoverageCloseReason = 'failure' | 'coordinator_shutdown' | 'coordinator_down';

export type CreditResult =
	| { status: 'lease_mismatch' }
	// The supplied provider does not hold authority (it is none, another
	// provider, or no authority was ever committed): nothing written. Benign,
	// for example a cycle that finished after its provider was relinquished.
	| { status: 'authority_changed'; authority: string | null }
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

export type CommitResult =
	| { status: 'lease_mismatch' }
	| { status: 'committed'; epoch: number; timelineVersion: number }
	// Another provider holds authority: never taken over by a commit.
	| { status: 'not_none'; authority: string }
	// The commit time is not after last_active_success_ms: nothing written.
	| { status: 'stale_clock' };

export type RelinquishResult =
	| { status: 'lease_mismatch' }
	// member is the segment closed on the way, or null if none was written.
	| { status: 'relinquished'; timelineVersion: number; member: string | null }
	| { status: 'unexpected_provider'; authority: string }
	// No authority was ever committed: nothing to relinquish, nothing written.
	| { status: 'not_initialized' };

// KEYS: lease, authority. ARGV: token, success time (ms), provider.
// Extends or reopens coverage for the provider that already holds authority.
// It never commits authority: that is COMMIT's job alone.
export const CREDIT_SCRIPT = `
	if redis.call('GET', KEYS[1]) ~= ARGV[1] then
		return {'lease_mismatch'}
	end
	local t = ARGV[2]
	local provider = redis.call('HGET', KEYS[2], 'provider')
	local epoch = redis.call('HGET', KEYS[2], 'epoch')
	if not provider or not epoch or provider ~= ARGV[3] then
		return {'authority_changed', provider or ''}
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

// KEYS: lease, authority. ARGV: token, provider, commit time (ms).
// After a candidate's successful publish: authority and the epoch increment,
// as one revision. Only from none, or from a record that never had an
// authority (then epoch becomes 1). Never takes over a provider that holds
// authority. It never opens coverage: authority says who may publish, not
// that anything was observed. The caller credits the same cycle separately
// if it qualifies as coverage.
export const COMMIT_SCRIPT = `
	if redis.call('GET', KEYS[1]) ~= ARGV[1] then
		return {'lease_mismatch'}
	end
	local provider = redis.call('HGET', KEYS[2], 'provider')
	local epoch = redis.call('HGET', KEYS[2], 'epoch')
	if provider and epoch and provider ~= 'none' then
		return {'not_none', provider}
	end
	local open = redis.call('HGET', KEYS[2], 'coverage_open_since_ms')
	if open and open ~= '' then
		return redis.error_reply('invariant: coverage is open while there is no authority')
	end
	local last = redis.call('HGET', KEYS[2], 'last_active_success_ms')
	if last and last ~= '' and tonumber(ARGV[3]) <= tonumber(last) then
		return {'stale_clock'}
	end
	local e = redis.call('HINCRBY', KEYS[2], 'epoch', 1)
	redis.call('HSET', KEYS[2],
		'provider', ARGV[2],
		'authority_since_ms', ARGV[3],
		'coverage_open_since_ms', '')
	local v = redis.call('HINCRBY', KEYS[2], 'timeline_version', 1)
	return {'committed', e, v}
`;

// KEYS: lease, authority, coverage. ARGV: token, expected provider, time (ms),
// prune cutoff (ms). The authoritative provider has become UNAVAILABLE:
// authority becomes none, as one revision. Coverage has normally closed at
// the first failed cycle already; anything still open closes here as
// `failure`, with CLOSE's rules (a member only with length, backwards refused).
// epoch and last_active_success_ms are kept; authority_since_ms records when
// none began.
export const RELINQUISH_SCRIPT = `
	if redis.call('GET', KEYS[1]) ~= ARGV[1] then
		return {'lease_mismatch'}
	end
	local provider = redis.call('HGET', KEYS[2], 'provider')
	local epoch = redis.call('HGET', KEYS[2], 'epoch')
	if not provider or not epoch then
		return {'not_initialized'}
	end
	if provider ~= ARGV[2] then
		return {'unexpected_provider', provider}
	end
	local member = ''
	local open = redis.call('HGET', KEYS[2], 'coverage_open_since_ms')
	if open and open ~= '' then
		local last = redis.call('HGET', KEYS[2], 'last_active_success_ms')
		if not last or tonumber(last) < tonumber(open) then
			return redis.error_reply('invariant: last_active_success_ms ' .. tostring(last) ..
				' is before coverage_open_since_ms ' .. open)
		end
		if tonumber(last) > tonumber(open) then
			member = provider .. '|' .. open .. '|' .. last .. '|failure'
			redis.call('ZADD', KEYS[3], last, member)
			redis.call('ZREMRANGEBYSCORE', KEYS[3], '-inf', '(' .. ARGV[4])
		end
	end
	redis.call('HSET', KEYS[2],
		'provider', 'none',
		'authority_since_ms', ARGV[3],
		'coverage_open_since_ms', '')
	local v = redis.call('HINCRBY', KEYS[2], 'timeline_version', 1)
	return {'relinquished', v, member}
`;

export class CoverageTimeline {
	constructor(
		private readonly redis: Redis,
		private readonly retentionMs: number,
		private readonly leaseKey: string = LEASE_KEY,
		private readonly authorityKey: string = AUTHORITY_KEY,
		private readonly coverageKey: string = COVERAGE_KEY,
	) {}

	// Credit one successful, fresh active cycle of the authoritative provider,
	// whose publish finished at activeSuccessMs (coordinator processing time).
	async credit(
		token: string,
		provider: AuthorityProvider,
		activeSuccessMs: number,
	): Promise<CreditResult> {
		const reply = (await this.redis.eval(
			CREDIT_SCRIPT,
			2,
			this.leaseKey,
			this.authorityKey,
			token,
			String(activeSuccessMs),
			provider,
		)) as [string, (string | number)?];
		const [status, value] = reply;
		if (status === 'lease_mismatch') return { status };
		if (status === 'authority_changed') return { status, authority: value ? String(value) : null };
		if (status === 'opened' || status === 'extended' || status === 'stale_clock') {
			return { status, timelineVersion: Number(value) };
		}
		throw new Error(`unexpected credit script reply: ${JSON.stringify(reply)}`);
	}

	// Commit provider as the authority after its candidate cycle published.
	// Coverage stays closed until a CREDIT for a cycle that qualifies.
	async commit(
		token: string,
		provider: AuthorityProvider,
		commitMs: number,
	): Promise<CommitResult> {
		const reply = (await this.redis.eval(
			COMMIT_SCRIPT,
			2,
			this.leaseKey,
			this.authorityKey,
			token,
			provider,
			String(commitMs),
		)) as [string, ...(string | number)[]];
		const [status, a, b] = reply;
		if (status === 'lease_mismatch' || status === 'stale_clock') return { status };
		if (status === 'not_none') return { status, authority: String(a) };
		if (status === 'committed') {
			return { status, epoch: Number(a), timelineVersion: Number(b) };
		}
		throw new Error(`unexpected commit script reply: ${JSON.stringify(reply)}`);
	}

	// Relinquish expected's authority to none at nowMs.
	async relinquish(token: string, expected: string, nowMs: number): Promise<RelinquishResult> {
		const reply = (await this.redis.eval(
			RELINQUISH_SCRIPT,
			3,
			this.leaseKey,
			this.authorityKey,
			this.coverageKey,
			token,
			expected,
			String(nowMs),
			String(nowMs - this.retentionMs),
		)) as [string, ...(string | number)[]];
		const [status, a, b] = reply;
		if (status === 'lease_mismatch' || status === 'not_initialized') return { status };
		if (status === 'unexpected_provider') return { status, authority: String(a) };
		if (status === 'relinquished') {
			return { status, timelineVersion: Number(a), member: b ? String(b) : null };
		}
		throw new Error(`unexpected relinquish script reply: ${JSON.stringify(reply)}`);
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
