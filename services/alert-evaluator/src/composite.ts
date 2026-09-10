// CP2: read-only eligibility resolution for composite correlation.
//
// No Redis writes, no composite_issued mutation, no recent-loss deletion,
// no Kafka emission. See DATA_MODEL.md's "Composite eligibility rule" for
// the accepted formula and tie-break this implements. Wiring this into
// handleProximityCandidate and actually claiming/emitting a COMPOSITE are
// later checkpoints.
import type { Redis } from 'ioredis';
import type { ProximityCandidateMessage } from './evaluator.js';

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

// ---- Candidate decision record (CP3C) ---------------------------------------
//
// alert-decision:{pair_key}:{episode_start_ms} answers exactly one question:
// "for this exact proximity candidate, what alert-type decision was already
// made?" It does not resolve eligibility (CP2), claim a loss episode (CP3B),
// publish to Kafka, or finalize anything -- and it has no deletion logic
// yet; that depends on the input-offset lifecycle CP5 wires up. See
// DATA_MODEL.md's "Composite claim and decision protocol" for why this
// exists as a mechanism separate from loss-episode claiming: the claim
// alone only protects one direction of a redelivery decision flip, not both.
//
// For COMPOSITE, the record freezes the loss identity CP2/tie-break already
// selected -- entity, source representation, dark_since_ms,
// signal_loss_alert_id, resumed_at_ms -- so a later replay reconstructs the
// same decision without re-running CP2 against Redis state that may have
// changed since. It deliberately does not freeze a final Kafka alert
// payload; constructing the deterministic COMPOSITE alert is CP4's job.

export interface CompositeCandidateDecision {
	decision: 'COMPOSITE';
	candidate_id: string;
	selected_entity_id: string;
	loss_source: LossEpisodeSource;
	dark_since_ms: number;
	signal_loss_alert_id: string;
	resumed_at_ms: number | null;
}

export interface UnscheduledCandidateDecision {
	decision: 'UNSCHEDULED_PROXIMITY';
	candidate_id: string;
}

export type CandidateDecision = CompositeCandidateDecision | UnscheduledCandidateDecision;

// Thrown by writeCandidateDecisionIfAbsent when a decision already stored
// for this candidate_id is logically different from the one being written.
// This is an invariant violation, not a normal control-flow outcome: a
// single candidate_id should only ever be decided once, by construction
// (Kafka's own partitioning plus Pre-CP2A's leader-scoped consumption mean
// no two processes should be deciding the same candidate concurrently under
// normal operation). Surfacing it as a thrown error, rather than silently
// keeping the old decision or overwriting it, is deliberate: fail closed
// and make the corruption visible instead of guessing which side was right.
export class CandidateDecisionConflictError extends Error {
	constructor(
		public readonly requested: CandidateDecision,
		public readonly existing: CandidateDecision,
	) {
		super(
			`candidate decision conflict for ${requested.candidate_id}: ` +
				`requested ${requested.decision}, already recorded ${existing.decision}`,
		);
		this.name = 'CandidateDecisionConflictError';
	}
}

function decisionKey(candidateId: string): string {
	return `alert-decision:${candidateId}`;
}

// Flattens a CandidateDecision into the fixed seven-field ARGV shape both
// Lua scripts below expect, so a non-COMPOSITE decision still has a
// consistent field count to compare against (empty string for the fields
// that don't apply).
function decisionToArgs(
	d: CandidateDecision,
): [string, string, string, string, string, string, string] {
	if (d.decision === 'UNSCHEDULED_PROXIMITY') {
		return [d.decision, d.candidate_id, '', '', '', '', ''];
	}
	return [
		d.decision,
		d.candidate_id,
		d.selected_entity_id,
		String(d.dark_since_ms),
		d.loss_source,
		d.signal_loss_alert_id,
		d.resumed_at_ms === null ? '' : String(d.resumed_at_ms),
	];
}

// Inverse of decisionToArgs -- reconstructs a CandidateDecision from the
// seven flat fields as stored in (or returned by) Redis. Throws if the
// record is malformed: an unrecognized decision value, or a COMPOSITE
// record missing required fields. This is the "fail closed rather than
// silently re-deciding" guard -- a caller must never receive a decision
// object that looks valid but was reconstructed from corrupted data.
function argsToDecision(
	fields: [string, string, string, string, string, string, string],
): CandidateDecision {
	const [
		decision,
		candidateId,
		selectedEntityId,
		darkSinceMsRaw,
		lossSource,
		signalLossAlertId,
		resumedAtMsRaw,
	] = fields;

	if (decision === 'UNSCHEDULED_PROXIMITY') {
		return { decision, candidate_id: candidateId };
	}

	if (decision === 'COMPOSITE') {
		const darkSinceMs = Number(darkSinceMsRaw);
		if (
			!selectedEntityId ||
			!Number.isFinite(darkSinceMs) ||
			(lossSource !== 'ACTIVE' && lossSource !== 'RECENT') ||
			!signalLossAlertId
		) {
			throw new Error(`malformed alert-decision record for candidate_id ${candidateId}`);
		}
		return {
			decision,
			candidate_id: candidateId,
			selected_entity_id: selectedEntityId,
			loss_source: lossSource,
			dark_since_ms: darkSinceMs,
			signal_loss_alert_id: signalLossAlertId,
			resumed_at_ms: resumedAtMsRaw === '' ? null : Number(resumedAtMsRaw),
		};
	}

	throw new Error(
		`malformed alert-decision record for candidate_id ${candidateId}: unknown decision`,
	);
}

// KEYS[1] = alert-decision:{candidate_id}
// ARGV[1..7] = decision, candidate_id, selected_entity_id, dark_since_ms,
//              loss_source, signal_loss_alert_id, resumed_at_ms
//              (empty strings for the fields UNSCHEDULED_PROXIMITY has none of)
//
// Atomically checks absence and establishes the record in the same script --
// not GET-then-application-decides-then-HSET, which a concurrent writer
// could race. If the key already exists, compares decision + entity +
// dark_since_ms (the fields that define "the same logical decision") against
// what was requested: an exact match is idempotent success; anything else
// is a conflict. Always returns the record now canonically stored, so the
// caller never has to guess whether it got the value it asked for.
const WRITE_CANDIDATE_DECISION_IF_ABSENT_LUA = `
if redis.call('EXISTS', KEYS[1]) == 0 then
	redis.call('HSET', KEYS[1],
		'decision', ARGV[1],
		'candidate_id', ARGV[2],
		'selected_entity_id', ARGV[3],
		'dark_since_ms', ARGV[4],
		'loss_source', ARGV[5],
		'signal_loss_alert_id', ARGV[6],
		'resumed_at_ms', ARGV[7])
	return {1, ARGV[1], ARGV[2], ARGV[3], ARGV[4], ARGV[5], ARGV[6], ARGV[7]}
end

local existing_decision = redis.call('HGET', KEYS[1], 'decision') or ''
local existing_candidate_id = redis.call('HGET', KEYS[1], 'candidate_id') or ''
local existing_entity = redis.call('HGET', KEYS[1], 'selected_entity_id') or ''
local existing_dark_since = redis.call('HGET', KEYS[1], 'dark_since_ms') or ''
local existing_loss_source = redis.call('HGET', KEYS[1], 'loss_source') or ''
local existing_alert_id = redis.call('HGET', KEYS[1], 'signal_loss_alert_id') or ''
local existing_resumed_at = redis.call('HGET', KEYS[1], 'resumed_at_ms') or ''

if existing_decision == ARGV[1] and existing_entity == ARGV[3] and existing_dark_since == ARGV[4] then
	return {1, existing_decision, existing_candidate_id, existing_entity, existing_dark_since, existing_loss_source, existing_alert_id, existing_resumed_at}
end

return {0, existing_decision, existing_candidate_id, existing_entity, existing_dark_since, existing_loss_source, existing_alert_id, existing_resumed_at}
`;

// Returns the previously recorded decision for candidateId, or null if none
// exists yet. Throws if a record exists but is malformed (see
// argsToDecision) -- never returns an object reconstructed from corrupted
// data, and never treats "malformed" the same as "no decision yet" (that
// would let a caller silently re-decide via CP2 when the record's mere
// corruption is not evidence the original decision should be discarded).
export async function readCandidateDecision(
	redis: Redis,
	candidateId: string,
): Promise<CandidateDecision | null> {
	const fields = await redis.hgetall(decisionKey(candidateId));
	if (!fields || Object.keys(fields).length === 0) return null;

	return argsToDecision([
		fields['decision'] ?? '',
		fields['candidate_id'] ?? '',
		fields['selected_entity_id'] ?? '',
		fields['dark_since_ms'] ?? '',
		fields['loss_source'] ?? '',
		fields['signal_loss_alert_id'] ?? '',
		fields['resumed_at_ms'] ?? '',
	]);
}

// Establishes decision as the permanent, immutable record for its
// candidate_id if none exists yet. If one already exists and is logically
// identical, this is an idempotent success returning that existing record --
// safe for a Kafka-redelivered candidate to call again. If one already
// exists and differs, throws CandidateDecisionConflictError rather than
// overwriting: a single candidate_id must only ever be decided once. Never
// touches alert-state, recent-loss, or any TTL.
export async function writeCandidateDecisionIfAbsent(
	redis: Redis,
	decision: CandidateDecision,
): Promise<CandidateDecision> {
	const args = decisionToArgs(decision);
	const result = (await redis.eval(
		WRITE_CANDIDATE_DECISION_IF_ABSENT_LUA,
		1,
		decisionKey(decision.candidate_id),
		...args,
	)) as [number, string, string, string, string, string, string, string];

	const [success, ...storedArgs] = result;
	const stored = argsToDecision(
		storedArgs as [string, string, string, string, string, string, string],
	);

	if (success === 1) return stored;
	throw new CandidateDecisionConflictError(decision, stored);
}

// Composite alert builder
// Pure: no Redis, no Kafka, no config, no clock. Every operational field
// (entityType, detectedAtMs, correlationWindowMs) is a caller-supplied
// argument rather than read internally, so "same inputs -> same output" holds
// for the function itself -- it does not depend on this process's config or
// wall clock. Determinism of the eventual Kafka message additionally depends
// on the caller (CP5A) passing a stable detectedAtMs and correlationWindowMs
// across a redelivery, which is that checkpoint's responsibility, not this
// function's.
//
// DATA_MODEL.md specifies COMPOSITE's payload as "nested signal-loss +
// proximity evidence" -- taken literally as two distinct sub-objects, not
// flattened, even though UNSCHEDULED_PROXIMITY's payload happens to be flat.

export interface CompositeAlertPayload {
	signal_loss: {
		dark_since_ms: number;
		loss_source: LossEpisodeSource;
		resumed_at_ms: number | null;
		signal_loss_alert_id: string;
	};
	proximity: {
		pair_key: string;
		entity_a_id: string;
		entity_b_id: string;
		lat: number;
		lon: number;
		distance_metres: number;
		episode_start_ms: number;
	};
	correlation_window_ms: number;
	supersedes_alert_ids: string[];
}

export interface CompositeAlert {
	alert_id: string;
	entity_id: string;
	counterparty_entity_id: string;
	entity_type: string;
	alert_type: 'COMPOSITE';
	priority: 'STANDARD';
	status: 'NEW';
	detected_at_ms: number;
	payload: CompositeAlertPayload;
}

// decision.selected_entity_id is the primary entity_id -- it is the pair
// member whose signal-loss episode this composite is anchored to; the other
// pair member becomes counterparty_entity_id. Throws rather than guessing if
// selected_entity_id matches neither candidate pair member: that can only
// mean the decision record and the candidate message do not actually
// describe the same encounter, an invariant violation this function must not
// silently paper over (same fail-closed posture as CandidateDecisionConflictError above).
export function buildCompositeAlert(
	decision: CompositeCandidateDecision,
	candidate: ProximityCandidateMessage,
	entityType: string,
	detectedAtMs: number,
	correlationWindowMs: number,
): CompositeAlert {
	let counterpartyEntityId: string;
	if (decision.selected_entity_id === candidate.entity_a_id) {
		counterpartyEntityId = candidate.entity_b_id;
	} else if (decision.selected_entity_id === candidate.entity_b_id) {
		counterpartyEntityId = candidate.entity_a_id;
	} else {
		throw new Error(
			`composite decision entity ${decision.selected_entity_id} is not a member of ` +
				`candidate pair ${candidate.pair_key}`,
		);
	}

	return {
		alert_id: `${candidate.pair_key}:COMPOSITE:${decision.dark_since_ms}`,
		entity_id: decision.selected_entity_id,
		counterparty_entity_id: counterpartyEntityId,
		entity_type: entityType,
		alert_type: 'COMPOSITE',
		priority: 'STANDARD',
		status: 'NEW',
		detected_at_ms: detectedAtMs,
		payload: {
			signal_loss: {
				dark_since_ms: decision.dark_since_ms,
				loss_source: decision.loss_source,
				resumed_at_ms: decision.resumed_at_ms,
				signal_loss_alert_id: decision.signal_loss_alert_id,
			},
			proximity: {
				pair_key: candidate.pair_key,
				entity_a_id: candidate.entity_a_id,
				entity_b_id: candidate.entity_b_id,
				lat: candidate.lat,
				lon: candidate.lon,
				distance_metres: candidate.distance_at_detection,
				episode_start_ms: candidate.episode_start_ms,
			},
			correlation_window_ms: correlationWindowMs,
			supersedes_alert_ids: [decision.signal_loss_alert_id],
		},
	};
}
