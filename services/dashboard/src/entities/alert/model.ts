// Frontend domain model for an open alert. Not the database row and not
// the wire DTO — the canonical representation all React components consume.

export interface Alert {
	id: string
	alertType: string
	entityId: string
	// Second entity for a proximity/composite alert; null for SIGNAL_LOSS.
	counterpartyEntityId: string | null
	entityType: string
	status: string
	priority: string
	// alert_id of the COMPOSITE that superseded this alert, if any (CP5B).
	supersededBy: string | null
	// Processing time: when the Alert Evaluator's scan noticed the condition,
	// not when it actually started (see darkSinceMs below for SIGNAL_LOSS).
	detectedAtMs: number
	// Type-specific evidence. SIGNAL_LOSS carries dark_since_ms and
	// last_known_* position fields (see docs/DATA_MODEL.md).
	payload: Record<string, unknown>
}

// SIGNAL_LOSS is the only alert_type implemented so far (route deviation,
// proximity, and composite are later phases). Reads the source-event-time
// anchor from the payload rather than falling back to detectedAtMs, since
// those two timestamps can differ by up to SCAN_INTERVAL_MS.
export function signalLossDarkSinceMs(alert: Alert): number | null {
	const raw = alert.payload['dark_since_ms']
	return typeof raw === 'number' ? raw : null
}

// RESOLVED and SUPERSEDED are terminal per docs/DATA_MODEL.md's alert
// lifecycle — once reached, a given alert_id's row never changes again.
const TERMINAL_STATUSES = new Set(['RESOLVED', 'SUPERSEDED'])

// Pure reducer: merge an incoming alert into the current map by alert_id.
// A redelivered Kafka message (e.g. after a crash before offset commit)
// republishes the exact same content; overwriting by key is what makes that
// redelivery safe to receive twice without creating a duplicate panel entry.
//
// Monotonic by status rank (DATA_MODEL.md's Pre-CP5B(f)): a COMPOSITE and
// the individual alert it supersedes are published from two separately
// committed API transactions with no cross-message ordering guarantee, so a
// client can observe a stale NEW/ACKNOWLEDGED frame for an alert it has
// already rendered as terminal. Once an alert_id is terminal, an incoming
// frame that would move it back to a non-terminal status is dropped rather
// than applied — the only case this merge is not a plain overwrite.
export function applyAlertUpdate(current: Map<string, Alert>, incoming: Alert): Map<string, Alert> {
	const existing = current.get(incoming.id)
	if (
		existing &&
		TERMINAL_STATUSES.has(existing.status) &&
		!TERMINAL_STATUSES.has(incoming.status)
	) {
		return current
	}
	const next = new Map(current)
	next.set(incoming.id, incoming)
	return next
}

// Every alert_id a COMPOSITE currently in view lists as evidence it
// supersedes. Rendered nested under that COMPOSITE rather than as its own
// top-level card — checked structurally from the COMPOSITE's own payload,
// not from the child's own status field, since that field can lag behind
// (see applyAlertUpdate above).
export function supersededEvidenceIds(alerts: Iterable<Alert>): Set<string> {
	const ids = new Set<string>()
	for (const alert of alerts) {
		if (alert.alertType !== 'COMPOSITE') continue
		const raw = alert.payload['supersedes_alert_ids']
		if (!Array.isArray(raw)) continue
		for (const id of raw) {
			if (typeof id === 'string') ids.add(id)
		}
	}
	return ids
}
