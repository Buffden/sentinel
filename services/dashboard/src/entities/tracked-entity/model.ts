// Frontend domain model for a live tracked entity.
// This is NOT the database row and NOT the wire DTO — it is the canonical
// representation that all React components consume. Field names follow
// camelCase frontend conventions regardless of what the API sends.

export interface TrackedEntity {
	id: string
	lat: number
	lon: number
	altitudeM: number | null
	speedMps: number | null
	courseDeg: number | null
	// Source event time in milliseconds. Used for monotonicity — only accept
	// an update if its eventTimeMs is strictly newer than the stored value.
	eventTimeMs: number
	entityType: string | null
	entitySubtype: string | null
	callsign: string | null
	onGround: boolean | null
}

// Pure reducer: apply an incoming update to the current entity map.
// Stale frames (eventTimeMs ≤ stored) are silently discarded.
// Keyed by entity id so write-by-key is idempotent for duplicate frames.
export function applyPositionUpdate(
	current: Map<string, TrackedEntity>,
	incoming: TrackedEntity,
): Map<string, TrackedEntity> {
	const existing = current.get(incoming.id)
	if (existing && incoming.eventTimeMs <= existing.eventTimeMs) {
		// Stale or duplicate — discard without mutation
		return current
	}
	const next = new Map(current)
	next.set(incoming.id, incoming)
	return next
}
