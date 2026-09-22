// Network boundary adapter: wire DTO -> EntityDetail domain model.
// This is the only place in the frontend that knows GET /entities/:entity_id's
// envelope shape. The nested entity/alert shapes are adapted by the
// already-existing tracked-entity and alert adapters, not re-implemented here.

import {
	wireToTrackedEntity,
	isValidWireEntityDto,
	type WireEntityDto,
} from '@/entities/tracked-entity/adapter'
import { wireToAlert, isValidWireAlertDto, type WireAlertDto } from '@/entities/alert/adapter'
import type { EntityDetail } from './model'

export interface WireEntityDetailDto {
	entity: WireEntityDto | null
	alerts: WireAlertDto[]
}

export function wireToEntityDetail(entityId: string, dto: WireEntityDetailDto): EntityDetail {
	return {
		entityId,
		entity: dto.entity && isValidWireEntityDto(dto.entity) ? wireToTrackedEntity(dto.entity) : null,
		alerts: dto.alerts.filter(isValidWireAlertDto).map(wireToAlert),
	}
}

// Guard: reject objects that are clearly not a valid envelope. `entity` may
// legitimately be null (a dark entity), so its own shape is checked by
// isValidWireEntityDto only when present, inside wireToEntityDetail above.
export function isValidWireEntityDetailDto(val: unknown): val is WireEntityDetailDto {
	if (!val || typeof val !== 'object') return false
	const d = val as Record<string, unknown>
	return (d['entity'] === null || typeof d['entity'] === 'object') && Array.isArray(d['alerts'])
}
