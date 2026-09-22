'use client'

// First React context in this codebase. Justified now, not speculatively:
// it's the mechanism that lets a deeply-nested component (AlertWidget) open
// a new top-level Dockview panel without threading the outer DockviewApi
// through every intermediate component -- and it has two real consumers
// within this same feature: AlertWidget's entity_id click-through (this
// checkpoint) and the Relationships tab's graph-pivot (a later checkpoint,
// same "click an entity_id, open its detail panel" mechanism).

import { createContext, useContext, useMemo, type ReactNode } from 'react'
import type { DockviewApi } from 'dockview-react'

interface WorkspacePanelApi {
	// Opens (or, if already open, focuses) an Entity Detail panel for this
	// entity_id. Deterministic panel id keyed by entity_id is what makes a
	// repeat click idempotent -- it can never spawn a duplicate panel for the
	// same entity, only ever surface the one that already exists.
	openEntityDetail: (entityId: string) => void
}

const WorkspacePanelContext = createContext<WorkspacePanelApi | null>(null)

function entityDetailPanelId(entityId: string): string {
	return `entity-detail-${entityId}`
}

export function WorkspacePanelProvider({
	api,
	children,
}: {
	api: DockviewApi | null
	children: ReactNode
}) {
	const value = useMemo<WorkspacePanelApi>(
		() => ({
			openEntityDetail: (entityId: string) => {
				if (!api) return
				const id = entityDetailPanelId(entityId)
				const existing = api.getPanel(id)
				if (existing) {
					existing.api.setActive()
					return
				}
				api.addPanel({
					id,
					component: 'entity-detail-widget',
					title: entityId,
					params: { entityId },
					position: { direction: 'right' },
				})
			},
		}),
		[api],
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
