'use client'

// Client-side call for the alert lifecycle transition (Phase 08).
// Mirrors features/workspace/workspaceApi.ts's pattern: a thin typed
// wrapper around fetchApi, not a generic API client layer.

import { fetchApi } from '@/features/auth/apiClient'
import type { Alert } from './model'

export type LifecycleTargetStatus = 'ACKNOWLEDGED' | 'RESOLVED'

// Wire shape of a PATCH /alerts/:alert_id response. Identical to an
// alert-events WS frame's data field (PublishedAlert on the API side), not
// WireAlertDto (the GET /alerts REST shape): detected_at_ms is a number,
// there's no updated_at/acknowledged_at/resolved_at here — see
// docs/DATA_MODEL.md's alert-events contract.
interface PatchedAlertDto {
	alert_id: string
	entity_id: string
	counterparty_entity_id: string | null
	entity_type: string
	alert_type: string
	priority: string
	status: string
	superseded_by: string | null
	detected_at_ms: number
	payload: Record<string, unknown>
}

function patchedAlertToAlert(dto: PatchedAlertDto): Alert {
	return {
		id: dto.alert_id,
		alertType: dto.alert_type,
		entityId: dto.entity_id,
		counterpartyEntityId: dto.counterparty_entity_id,
		entityType: dto.entity_type,
		status: dto.status,
		priority: dto.priority,
		supersededBy: dto.superseded_by,
		detectedAtMs: dto.detected_at_ms,
		payload: dto.payload,
	}
}

export class AlertTransitionError extends Error {
	constructor(
		message: string,
		public readonly status: number,
	) {
		super(message)
		this.name = 'AlertTransitionError'
	}
}

// PATCHes an alert's lifecycle status. A 200 (applied or idempotent replay)
// and a 409 (illegal transition) both return the alert's real current
// state as the same shape — both are parsed and returned so the caller
// converges the UI to reality either way, not just the happy path (see
// services/api/src/routes/alerts.ts's PATCH handler). Any other non-2xx
// status throws.
export async function patchAlertStatus(
	alertId: string,
	status: LifecycleTargetStatus,
): Promise<Alert> {
	const res = await fetchApi(`/api/alerts/${alertId}`, {
		method: 'PATCH',
		headers: { 'Content-Type': 'application/json' },
		body: JSON.stringify({ status }),
	})
	if (res.ok || res.status === 409) {
		const dto = (await res.json()) as PatchedAlertDto
		return patchedAlertToAlert(dto)
	}
	throw new AlertTransitionError(`failed to update alert: ${res.status}`, res.status)
}
