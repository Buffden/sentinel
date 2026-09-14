'use client'

// Client-side wrapper for the operator's saved workspace scope (CP1-CP5).
// Called by the CP5 scope editor (WorkspaceScopeControl/WorkspaceScopeModal).

import { fetchApi } from '@/features/auth/apiClient'
import { forceReconnect } from '@/shared/realtime/liveSocket'

export interface GeoBounds {
	min_lat: number
	max_lat: number
	min_lon: number
	max_lon: number
}

export interface WorkspaceScope {
	geo_region: { name: string | null; bounds: GeoBounds }
	entity_types: string[]
	alert_types: string[]
}

export interface PredefinedRegion {
	name: string
	bounds: GeoBounds
}

// Static catalog, not user-specific -- a plain GET, per the deliberate
// exception to the POST-for-new-reads convention (see ADR-012's
// Consequences and services/api/src/routes/workspace.ts).
export async function getRegions(): Promise<PredefinedRegion[]> {
	const res = await fetchApi('/api/users/me/workspace/regions')
	if (!res.ok) throw new Error(`failed to load region catalog: ${res.status}`)
	return (await res.json()) as PredefinedRegion[]
}

// Returns null when the operator has no saved workspace yet (404) -- per
// ADR-012, callers show the scope setup prompt in that case, not an error.
export async function getWorkspaceScope(): Promise<WorkspaceScope | null> {
	const res = await fetchApi('/api/users/me/workspace', { method: 'POST' })
	if (res.status === 404) return null
	if (!res.ok) throw new Error(`failed to load workspace scope: ${res.status}`)
	return (await res.json()) as WorkspaceScope
}

// Saves the scope, then reconnects the live WebSocket so the change is
// picked up server-side -- per ADR-012, a saved scope is only ever applied
// at connection open, never hot-swapped on a live connection (see the
// workspace-reconnect-flow concept doc). Does not reconnect on failure: an
// invalid or rejected save must leave the existing connection, and whatever
// scope it already loaded, untouched.
export async function saveWorkspaceScope(scope: WorkspaceScope): Promise<WorkspaceScope> {
	const res = await fetchApi('/api/users/me/workspace', {
		method: 'PUT',
		headers: { 'Content-Type': 'application/json' },
		body: JSON.stringify(scope),
	})
	if (!res.ok) throw new Error(`failed to save workspace scope: ${res.status}`)
	const saved = (await res.json()) as WorkspaceScope
	forceReconnect()
	return saved
}
