'use client'

// Client-side call for GET /entities/:entity_id/graph (Phase 09 CP4).
// Mirrors entities/entity-history/api.ts's pattern: a thin typed wrapper
// around fetchApi, not a generic API client layer.

import { fetchApi } from '@/features/auth/apiClient'
import { wireToGraphEdge, isValidWireEntityGraphDto, isValidWireGraphEdgeDto } from './adapter'
import type { GraphEdge } from './model'

// Same fail-closed-by-scope semantics as the other by-id entity endpoints: a
// 404 means either the id doesn't exist or (operator session) it's outside
// the caller's saved scope, indistinguishable by design.
export class EntityGraphNotFoundError extends Error {
	constructor(entityId: string) {
		super(`entity not found or outside scope: ${entityId}`)
		this.name = 'EntityGraphNotFoundError'
	}
}

export async function fetchEntityGraph(entityId: string): Promise<GraphEdge[]> {
	const res = await fetchApi(`/api/entities/${entityId}/graph`)
	if (res.status === 404) {
		throw new EntityGraphNotFoundError(entityId)
	}
	if (!res.ok) {
		throw new Error(`failed to load graph for ${entityId}: ${res.status}`)
	}
	const dto: unknown = await res.json()
	if (!isValidWireEntityGraphDto(dto)) {
		throw new Error(`malformed entity graph response for ${entityId}`)
	}
	return dto.edges
		.filter(isValidWireGraphEdgeDto)
		.map(wireToGraphEdge)
		.filter((edge): edge is GraphEdge => edge !== null)
}
