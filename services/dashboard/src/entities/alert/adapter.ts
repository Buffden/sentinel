// Network boundary adapter: wire DTO → Alert domain model.
// This is the only place in the frontend that knows GET /alerts' field names.

import type { Alert } from './model'

// Shape of one element in the JSON array returned by GET /alerts.
// detected_at (and the other *_at fields) are ISO 8601 strings — node-postgres
// serializes TIMESTAMPTZ columns to JS Date objects, and Express's res.json()
// stringifies those to ISO strings, not epoch milliseconds like the Redis-backed
// entity endpoints use.
export interface WireAlertDto {
	alert_id: string
	entity_id: string
	entity_type: string
	alert_type: string
	priority: string
	status: string
	payload: Record<string, unknown>
	detected_at: string
	updated_at: string
	acknowledged_at: string | null
	resolved_at: string | null
}

export function wireToAlert(dto: WireAlertDto): Alert {
	return {
		id: dto.alert_id,
		alertType: dto.alert_type,
		entityId: dto.entity_id,
		entityType: dto.entity_type,
		status: dto.status,
		priority: dto.priority,
		detectedAtMs: new Date(dto.detected_at).getTime(),
		payload: dto.payload,
	}
}

// Guard: reject objects that are clearly not valid wire DTOs.
// Returns false for malformed entries so callers can discard silently.
export function isValidWireAlertDto(val: unknown): val is WireAlertDto {
	if (!val || typeof val !== 'object') return false
	const d = val as Record<string, unknown>
	return (
		typeof d['alert_id'] === 'string' &&
		typeof d['entity_id'] === 'string' &&
		typeof d['alert_type'] === 'string' &&
		typeof d['status'] === 'string' &&
		typeof d['detected_at'] === 'string'
	)
}
