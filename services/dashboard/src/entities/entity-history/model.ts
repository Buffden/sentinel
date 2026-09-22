// Frontend domain model for GET /entities/:entity_id/history's response
// (Phase 09 CP3). Not the database row and not the wire DTO.

export interface HistoryPoint {
	timestampMs: number
	lat: number
	lon: number
	altitudeM: number | null
	speedMps: number | null
	courseDeg: number | null
}
