// Network boundary adapter: wire DTO -> HistoryPoint domain model.
// This is the only place in the frontend that knows
// GET /entities/:entity_id/history's field names.

import type { HistoryPoint } from './model'

// Shape of one element in the JSON array returned by
// GET /entities/:entity_id/history. heading_deg/on_ground/callsign/
// entity_subtype exist on the wire but aren't needed by the History tab's
// altitude-over-time chart -- omitted here rather than carried through
// unused; add them if a future consumer needs them.
export interface WireHistoryPointDto {
	entity_id: string
	timestamp_ms: number
	lat: number
	lon: number
	altitude_m: number | null
	speed_mps: number | null
	course_deg: number | null
}

export function wireToHistoryPoint(dto: WireHistoryPointDto): HistoryPoint {
	return {
		timestampMs: dto.timestamp_ms,
		lat: dto.lat,
		lon: dto.lon,
		altitudeM: dto.altitude_m,
		speedMps: dto.speed_mps,
		courseDeg: dto.course_deg,
	}
}

// Guard: reject entries that are clearly not valid wire DTOs.
// Returns false for malformed points so callers can discard silently.
export function isValidWireHistoryPointDto(val: unknown): val is WireHistoryPointDto {
	if (!val || typeof val !== 'object') return false
	const d = val as Record<string, unknown>
	return (
		typeof d['entity_id'] === 'string' &&
		typeof d['timestamp_ms'] === 'number' &&
		typeof d['lat'] === 'number' &&
		typeof d['lon'] === 'number'
	)
}
