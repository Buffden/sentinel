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
