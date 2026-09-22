'use client'

// Client-side call for GET /entities/:entity_id (Phase 09 CP2).
// Mirrors entities/alert/api.ts's pattern: a thin typed wrapper around
// fetchApi, not a generic API client layer.

import { fetchApi } from '@/features/auth/apiClient'
import { wireToEntityDetail, isValidWireEntityDetailDto } from './adapter'
import type { EntityDetail } from './model'

export class EntityDetailNotFoundError extends Error {
	constructor(entityId: string) {
		super(`entity not found or outside scope: ${entityId}`)
		this.name = 'EntityDetailNotFoundError'
	}
}

// Throws EntityDetailNotFoundError on a 404 -- either the id truly doesn't
// exist, or (operator session) it's outside the caller's saved workspace
// scope. The API deliberately returns the same 404 for both so the response
// never confirms an out-of-scope entity exists; the frontend can't and
// shouldn't try to distinguish them either.
export async function fetchEntityDetail(entityId: string): Promise<EntityDetail> {
	const res = await fetchApi(`/api/entities/${entityId}`)
	if (res.status === 404) {
		throw new EntityDetailNotFoundError(entityId)
	}
	if (!res.ok) {
		throw new Error(`failed to load entity ${entityId}: ${res.status}`)
	}
	const dto: unknown = await res.json()
	if (!isValidWireEntityDetailDto(dto)) {
		throw new Error(`malformed entity detail response for ${entityId}`)
	}
	return wireToEntityDetail(entityId, dto)
}
