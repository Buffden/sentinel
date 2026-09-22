'use client'

// Client-side call for GET /entities/:entity_id/history (Phase 09 CP3).
// Mirrors entities/entity-detail/api.ts's pattern: a thin typed wrapper
// around fetchApi, not a generic API client layer.

import { fetchApi } from '@/features/auth/apiClient'
import { wireToHistoryPoint, isValidWireHistoryPointDto, type WireHistoryPointDto } from './adapter'
import type { HistoryPoint } from './model'

// Same fail-closed-by-scope semantics as fetchEntityDetail: a 404 here means
// either the id doesn't exist or (operator session) it's outside the
// caller's saved scope, and the two are indistinguishable by design.
export class EntityHistoryNotFoundError extends Error {
	constructor(entityId: string) {
		super(`entity not found or outside scope: ${entityId}`)
		this.name = 'EntityHistoryNotFoundError'
	}
}

export async function fetchEntityHistory(
	entityId: string,
	fromMs: number,
	toMs: number,
): Promise<HistoryPoint[]> {
	const res = await fetchApi(`/api/entities/${entityId}/history?from_ms=${fromMs}&to_ms=${toMs}`)
	if (res.status === 404) {
		throw new EntityHistoryNotFoundError(entityId)
	}
	if (!res.ok) {
		throw new Error(`failed to load history for ${entityId}: ${res.status}`)
	}
	const dto: unknown = await res.json()
	if (!Array.isArray(dto)) {
		throw new Error(`malformed history response for ${entityId}`)
	}
	return (dto as unknown[])
		.filter((row): row is WireHistoryPointDto => isValidWireHistoryPointDto(row))
		.map(wireToHistoryPoint)
}
