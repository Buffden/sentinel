// CP2: read-only eligibility resolution for composite correlation.
//
// No Redis writes, no composite_issued mutation, no recent-loss deletion,
// no Kafka emission. See DATA_MODEL.md's "Composite eligibility rule" for
// the accepted formula and tie-break this implements. Wiring this into
// handleProximityCandidate and actually claiming/emitting a COMPOSITE are
// later checkpoints.
import type { Redis } from 'ioredis';

export type LossEpisodeSource = 'ACTIVE' | 'RECENT';

export interface QualifyingLossEpisode {
	entity_id: string;
	source: LossEpisodeSource;
	dark_since_ms: number;
	signal_loss_alert_id: string;
	resumed_at_ms: number | null;
	gap_ms: number;
}

// dark_since_ms is the sole eligibility anchor for both alert-state (active)
// and recent-loss (resumed) -- DATA_MODEL.md. A negative gap (the candidate
// predates the loss) or a gap beyond windowMs rejects; windowMs itself is
// an inclusive boundary.
function qualifyingGap(
	candidateEpisodeStartMs: number,
	darkSinceMs: number,
	windowMs: number,
): number | null {
	const gapMs = candidateEpisodeStartMs - darkSinceMs;
	if (gapMs < 0 || gapMs > windowMs) return null;
	return gapMs;
}

// Reads at most one of alert-state/recent-loss for entityId -- never both.
// This relies on an invariant established by clearSignalLossEpisode
// (position-consumer/src/consumer.ts): the two keys are mutually exclusive
// by construction, replaced atomically in one MULTI on resume. alert-state
// is checked first because its presence means the entity is still dark,
// which recent-loss existing at the same time would contradict.
export async function resolveEntityLossEpisode(
	redis: Redis,
	entityId: string,
	candidateEpisodeStartMs: number,
	windowMs: number,
): Promise<QualifyingLossEpisode | null> {
	const activeState = await redis.hgetall(`alert-state:${entityId}`);
	if (activeState && activeState['dark_since_ms']) {
		// An episode already upgraded to a composite must not be reused.
		if (activeState['composite_issued'] !== '0') return null;

		const darkSinceMs = Number(activeState['dark_since_ms']);
		if (!Number.isFinite(darkSinceMs)) return null;

		// signal_loss_alert_id is required episode data, not merely carried
		// through when present: CP3 needs it for supersedes_alert_ids, so a
		// half-corrupt hash must not qualify here only to fail downstream.
		const signalLossAlertId = activeState['signal_loss_alert_id'];
		if (!signalLossAlertId) return null;

		const gapMs = qualifyingGap(candidateEpisodeStartMs, darkSinceMs, windowMs);
		if (gapMs === null) return null;

		return {
			entity_id: entityId,
			source: 'ACTIVE',
			dark_since_ms: darkSinceMs,
			signal_loss_alert_id: signalLossAlertId,
			resumed_at_ms: null,
			gap_ms: gapMs,
		};
	}

	// Key existence alone is never sufficient here -- the same explicit
	// dark_since_ms gap check applies. recent-loss's Redis TTL is retention
	// only; it can outlive the true eligibility deadline after a long dark
	// interval, so a live key can still fail this check.
	const recentLoss = await redis.hgetall(`recent-loss:${entityId}`);
	if (recentLoss && recentLoss['dark_since_ms']) {
		const darkSinceMs = Number(recentLoss['dark_since_ms']);
		if (!Number.isFinite(darkSinceMs)) return null;

		// DATA_MODEL.md defines recent-loss as three fields together
		// (dark_since_ms, resumed_at_ms, signal_loss_alert_id); a hash missing
		// either of the other two is equally malformed and must not qualify.
		const signalLossAlertId = recentLoss['signal_loss_alert_id'];
		if (!signalLossAlertId) return null;

		const resumedAtMs = Number(recentLoss['resumed_at_ms']);
		if (!Number.isFinite(resumedAtMs)) return null;

		const gapMs = qualifyingGap(candidateEpisodeStartMs, darkSinceMs, windowMs);
		if (gapMs === null) return null;

		return {
			entity_id: entityId,
			source: 'RECENT',
			dark_since_ms: darkSinceMs,
			signal_loss_alert_id: signalLossAlertId,
			resumed_at_ms: resumedAtMs,
			gap_ms: gapMs,
		};
	}

	return null;
}

// Pure: DATA_MODEL.md's deterministic tie-break -- smaller gap_ms wins; an
// exact tie goes to the lexicographically smaller entity_id (matching
// pair_key's own min:max convention). The losing episode is untouched here
// -- claiming/consuming it is a later checkpoint's concern.
export function selectWinningEpisode(
	a: QualifyingLossEpisode | null,
	b: QualifyingLossEpisode | null,
): QualifyingLossEpisode | null {
	if (!a) return b;
	if (!b) return a;
	if (a.gap_ms !== b.gap_ms) return a.gap_ms < b.gap_ms ? a : b;
	return a.entity_id <= b.entity_id ? a : b;
}

// Read-only: no Redis writes, no Kafka. Resolves both pair members
// independently, then applies the deterministic tie-break if both qualify.
export async function resolveCompositeEligibility(
	redis: Redis,
	entityAId: string,
	entityBId: string,
	candidateEpisodeStartMs: number,
	windowMs: number,
): Promise<QualifyingLossEpisode | null> {
	const [a, b] = await Promise.all([
		resolveEntityLossEpisode(redis, entityAId, candidateEpisodeStartMs, windowMs),
		resolveEntityLossEpisode(redis, entityBId, candidateEpisodeStartMs, windowMs),
	]);
	return selectWinningEpisode(a, b);
}

// Composite episode claim / finalize
//
// Coordination primitives only: no Kafka work, no alert-decision records,
// not wired into handleProximityCandidate. See DATA_MODEL.md's "Composite
// claim and decision protocol" for the accepted design this implements.
//
// Both operations locate the logical episode by entityId + a source-time
// identity (expectedDarkSinceMs), never by which Redis key currently holds
// it. CP3A's atomic handoff can move an episode from alert-state to
// recent-loss at any time -- including between CP2's read-only snapshot and
// a CLAIM/FINALIZE call -- so both scripts search both representations
// atomically inside one script rather than assuming one.
//
// candidateId is the caller's proximity-episode identity,
// {pair_key}:{episode_start_ms} -- never a bare pair_key, since the same
// pair can produce multiple distinct proximity episodes over time (see
// composite-claim-protocol's "canonical claim identity" note).

// KEYS[1] = alert-state:{entity_id}
// KEYS[2] = recent-loss:{entity_id}
// ARGV[1] = expected dark_since_ms (string)
// ARGV[2] = candidate_id
//
// Finds whichever of the two keys currently has a matching dark_since_ms
// (never assumes which one). Succeeds when that episode's composite_issued
// is '0' and its claim is empty or already equals this candidate_id -- the
// same candidate can retry a CLAIM it already holds (redelivery-safe); a
// different candidate cannot take it. On success, sets
// composite_claim_candidate_id to candidate_id. Never touches any TTL --
// coordination is not a reason to extend eligibility retention.
const CLAIM_COMPOSITE_EPISODE_LUA = `
local function find_match(key)
	local dark_since_ms = redis.call('HGET', key, 'dark_since_ms')
	if dark_since_ms == ARGV[1] then
		return key
	end
	return nil
end
local matched_key = find_match(KEYS[1])
if not matched_key then
	matched_key = find_match(KEYS[2])
end
if not matched_key then
	return {0, 'NO_EPISODE'}
end
if redis.call('HGET', matched_key, 'composite_issued') == '1' then
	return {0, 'ALREADY_ISSUED'}
end
local claim = redis.call('HGET', matched_key, 'composite_claim_candidate_id')
if claim and claim ~= '' and claim ~= ARGV[2] then
	return {0, 'CLAIMED_BY_OTHER'}
end
redis.call('HSET', matched_key, 'composite_claim_candidate_id', ARGV[2])
return {1, 'SUCCESS'}
`;

// Same representation-independent lookup as CLAIM. Succeeds only for the
// candidate that already holds the claim, and is idempotent for that same
// candidate: if the episode is already composite_issued=1 under this
// candidate's own claim, this returns success without re-mutating anything.
// That idempotency is required for the real failure case it exists to
// cover -- publish COMPOSITE, FINALIZE succeeds, crash before the
// proximity.candidates input offset commits, Kafka redelivers -- the
// redelivered candidate must recognize its own operation already reached
// the finalized state, not fail. A different candidateId is always
// rejected, even one that happens to match the episode's dark_since_ms.
// Never touches any TTL.
const FINALIZE_COMPOSITE_EPISODE_LUA = `
local function find_match(key)
	local dark_since_ms = redis.call('HGET', key, 'dark_since_ms')
	if dark_since_ms == ARGV[1] then
		return key
	end
	return nil
end
local matched_key = find_match(KEYS[1])
if not matched_key then
	matched_key = find_match(KEYS[2])
end
if not matched_key then
	return {0, 'NO_EPISODE'}
end
local claim = redis.call('HGET', matched_key, 'composite_claim_candidate_id')
if claim ~= ARGV[2] then
	return {0, 'NOT_CLAIMED'}
end
if redis.call('HGET', matched_key, 'composite_issued') == '1' then
	return {1, 'SUCCESS'}
end
redis.call('HSET', matched_key, 'composite_issued', '1')
return {1, 'SUCCESS'}
`;

// Claims one signal-loss episode for candidateId so a different candidate
// cannot also turn it into a COMPOSITE. See the module-level comment above
// for why the episode is identified by entityId + expectedDarkSinceMs
// rather than by Redis representation, and why a same-candidate retry
// succeeds.
export async function claimCompositeEpisode(
	redis: Redis,
	entityId: string,
	expectedDarkSinceMs: number,
	candidateId: string,
): Promise<boolean> {
	const result = (await redis.eval(
		CLAIM_COMPOSITE_EPISODE_LUA,
		2,
		`alert-state:${entityId}`,
		`recent-loss:${entityId}`,
		String(expectedDarkSinceMs),
		candidateId,
	)) as [number, string];
	return result[0] === 1;
}

// Marks a claimed episode composite_issued=1, only for the candidate that
// already holds the claim. See the module-level comment above for why this
// is idempotent for that same candidate and representation-independent.
export async function finalizeCompositeEpisode(
	redis: Redis,
	entityId: string,
	expectedDarkSinceMs: number,
	candidateId: string,
): Promise<boolean> {
	const result = (await redis.eval(
		FINALIZE_COMPOSITE_EPISODE_LUA,
		2,
		`alert-state:${entityId}`,
		`recent-loss:${entityId}`,
		String(expectedDarkSinceMs),
		candidateId,
	)) as [number, string];
	return result[0] === 1;
}
