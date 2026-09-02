// Frontend domain model for an open alert. Not the database row and not
// the wire DTO — the canonical representation all React components consume.

export interface Alert {
	id: string
	alertType: string
	entityId: string
	entityType: string
	status: string
	priority: string
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

// Pure reducer: merge an incoming alert into the current map by alert_id.
// Unlike position updates, there's no staleness/monotonicity concept here —
// v1 has no acknowledge/resolve lifecycle (Phase 08), so a given alert_id's
// content never changes after it's first published. A redelivered Kafka
// message (e.g. after a crash before offset commit) republishes the exact
// same content; overwriting by key is what makes that redelivery safe to
// receive twice without creating a duplicate panel entry.
export function applyAlertUpdate(current: Map<string, Alert>, incoming: Alert): Map<string, Alert> {
	const next = new Map(current)
	next.set(incoming.id, incoming)
	return next
}
