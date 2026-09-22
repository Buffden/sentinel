'use client'

// First React context in this codebase. Justified now, not speculatively:
// it's the mechanism that lets any component (AlertWidget, MapWidget,
// RelationshipsTab) change what the single, always-visible Entity Detail
// widget is showing, without threading a callback through every
// intermediate component.
//
// Originally (Phase 09 FE-CP1) this opened a new Dockview panel per entity,
// supporting multiple simultaneous investigation panels. Replaced with a
// single shared "selected entity" slot per an explicit product decision: one
// always-visible panel, updated in place, in the same fixed layout slot
// FlightInfoWidget used to occupy -- not a side-by-side multi-panel
// investigation view. The call sites (AlertWidget, MapWidget,
// RelationshipsTab) needed no changes: they already only ever called
// `openEntityDetail(entityId, options)`, never touched a panel/Dockview API
// directly.

import { createContext, useContext, useMemo, useState, type ReactNode } from 'react'

export interface SelectedEntity {
	entityId: string
	// Source-event-time anchor for the History tab's default window -- an
	// alert's own detected_at, or a graph edge's last_seen_ms. See
	// EntityDetailWidget/HistoryTab.
	anchorMs?: number
}

interface WorkspacePanelApi {
	selectedEntity: SelectedEntity | null
	// Selects (or re-selects) an entity for the single Entity Detail widget
	// to display. Calling this for an entity that's already selected is a
	// harmless no-op re-render, not an error.
	openEntityDetail: (entityId: string, options?: { anchorMs?: number }) => void
}

const WorkspacePanelContext = createContext<WorkspacePanelApi | null>(null)

export function WorkspacePanelProvider({ children }: { children: ReactNode }) {
	const [selectedEntity, setSelectedEntity] = useState<SelectedEntity | null>(null)

	const value = useMemo<WorkspacePanelApi>(
		() => ({
			selectedEntity,
			openEntityDetail: (entityId, options) => {
				setSelectedEntity({ entityId, anchorMs: options?.anchorMs })
			},
		}),
		[selectedEntity],
	)

	return <WorkspacePanelContext.Provider value={value}>{children}</WorkspacePanelContext.Provider>
}

// Returns null outside the provider (e.g. a widget rendered standalone in a
// test) rather than throwing -- callers already treat a null-returning hook
// as "not available here" (same convention as optional params elsewhere in
// this codebase, e.g. MapWidgetProps).
export function useWorkspacePanel(): WorkspacePanelApi | null {
	return useContext(WorkspacePanelContext)
}
