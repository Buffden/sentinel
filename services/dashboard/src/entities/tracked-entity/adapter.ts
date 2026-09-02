// Network boundary adapter: wire DTO → TrackedEntity domain model.
// This is the only place in the frontend that knows the API field names.
// All React components and hooks receive TrackedEntity, never raw JSON.

import type { TrackedEntity } from './model'

// Shape of the JSON object returned by GET /entities/live
export interface WireEntityDto {
	entity_id: string
	lat: number
	lon: number
	altitude_m: number | null
	speed_mps: number | null
	course_deg: number | null
	last_seen_ms: number
	entity_type: string | null
	entity_subtype: string | null
	callsign: string | null
	on_ground: boolean | null
}

export function wireToTrackedEntity(dto: WireEntityDto): TrackedEntity {
	return {
		id: dto.entity_id,
		lat: dto.lat,
		lon: dto.lon,
		altitudeM: dto.altitude_m,
		speedMps: dto.speed_mps,
		courseDeg: dto.course_deg,
		eventTimeMs: dto.last_seen_ms,
		entityType: dto.entity_type,
		entitySubtype: dto.entity_subtype,
		callsign: dto.callsign,
		onGround: dto.on_ground,
	}
}

// Guard: reject objects that are clearly not valid wire DTOs.
// Returns false for malformed frames so callers can discard silently.
export function isValidWireEntityDto(val: unknown): val is WireEntityDto {
	if (!val || typeof val !== 'object') return false
	const d = val as Record<string, unknown>
	return (
		typeof d['entity_id'] === 'string' &&
		typeof d['lat'] === 'number' &&
		typeof d['lon'] === 'number' &&
		typeof d['last_seen_ms'] === 'number'
	)
}
