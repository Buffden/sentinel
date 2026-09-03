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

// Movement fields every source (REST and WebSocket) always provides.
// Everything else is optional: a source that doesn't carry a field (the
// WebSocket position-updates frame has no entity_subtype/on_ground — see
// useLiveFeed.ts) simply omits the key rather than sending it as null, so a
// merge preserves whatever value REST hydration or an earlier frame set.
export type TrackedEntityUpdate = Pick<TrackedEntity, 'id' | 'lat' | 'lon' | 'eventTimeMs'> &
	Partial<Omit<TrackedEntity, 'id' | 'lat' | 'lon' | 'eventTimeMs'>>

// Pure reducer: apply an incoming update to the current entity map.
// Stale frames (eventTimeMs ≤ stored) are silently discarded.
// Keyed by entity id so write-by-key is idempotent for duplicate frames.
//
// Merges rather than replaces: a key absent from `incoming` keeps the
// existing entity's value instead of being wiped. Without this, a partial
// WebSocket update (which never carries entitySubtype/onGround) would erase
// those fields on every single live tick, even though REST hydration
// established real values for them moments earlier.
export function applyPositionUpdate(
	current: Map<string, TrackedEntity>,
	incoming: TrackedEntityUpdate,
): Map<string, TrackedEntity> {
	const existing = current.get(incoming.id)
	if (existing && incoming.eventTimeMs <= existing.eventTimeMs) {
		// Stale or duplicate — discard without mutation
		return current
	}
	const merged: TrackedEntity = existing
		? { ...existing, ...incoming }
		: {
				entityType: null,
				entitySubtype: null,
				altitudeM: null,
				speedMps: null,
				courseDeg: null,
				callsign: null,
				onGround: null,
				...incoming,
			}
	const next = new Map(current)
	next.set(incoming.id, merged)
	return next
}
