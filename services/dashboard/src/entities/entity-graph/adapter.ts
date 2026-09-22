// Network boundary adapter: wire DTO -> GraphEdge domain model.
// This is the only place in the frontend that knows
// GET /entities/:entity_id/graph's field names.

import type { GraphEdge, GraphEdgeType } from './model'

// Shape of one element in the `edges` array returned by
// GET /entities/:entity_id/graph.
export interface WireGraphEdgeDto {
	edge_type: string
	other_entity_id: string
	other_entity_type: string | null
	episode_start_ms: number | null
	last_seen_ms: number | null
	min_distance_metres: number | null
	established_at: string | null
	known_associate_type: string | null
}

export interface WireEntityGraphDto {
	entity_id: string
	edges: WireGraphEdgeDto[]
}

function isGraphEdgeType(val: string): val is GraphEdgeType {
	return val === 'PROXIMITY_EVENT' || val === 'KNOWN_ASSOCIATE'
}

export function wireToGraphEdge(dto: WireGraphEdgeDto): GraphEdge | null {
	if (!isGraphEdgeType(dto.edge_type)) return null
	return {
		edgeType: dto.edge_type,
		otherEntityId: dto.other_entity_id,
		otherEntityType: dto.other_entity_type,
		episodeStartMs: dto.episode_start_ms,
		lastSeenMs: dto.last_seen_ms,
		minDistanceMetres: dto.min_distance_metres,
		establishedAt: dto.established_at,
		knownAssociateType: dto.known_associate_type,
	}
}

// Guard: reject entries that are clearly not valid wire DTOs.
// Returns false for malformed edges so callers can discard silently.
export function isValidWireGraphEdgeDto(val: unknown): val is WireGraphEdgeDto {
	if (!val || typeof val !== 'object') return false
	const d = val as Record<string, unknown>
	return typeof d['edge_type'] === 'string' && typeof d['other_entity_id'] === 'string'
}

export function isValidWireEntityGraphDto(val: unknown): val is WireEntityGraphDto {
	if (!val || typeof val !== 'object') return false
	const d = val as Record<string, unknown>
	return typeof d['entity_id'] === 'string' && Array.isArray(d['edges'])
}
