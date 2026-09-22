'use client'

import { useRef, useState } from 'react'
import {
	DockviewReact,
	type DockviewApi,
	type DockviewReadyEvent,
	type IDockviewPanelProps,
} from 'dockview-react'
import MapWidget from '@/widgets/map-widget/MapWidget'
import EntityDetailWidget from '@/widgets/entity-detail-widget/EntityDetailWidget'
import { WorkspacePanelProvider } from '@/features/workspace/WorkspacePanelContext'
import WidgetPanel from './WidgetPanel'

type DvFC = React.FunctionComponent<IDockviewPanelProps>

const COMPONENTS: Record<string, DvFC> = {
	'map-widget': MapWidget as unknown as DvFC,
	'widget-panel': WidgetPanel as unknown as DvFC,
	'entity-detail-widget': EntityDetailWidget as unknown as DvFC,
}

function hideHeaders(api: DockviewApi) {
	const m = api.getPanel('map')
	const w = api.getPanel('widgets')
	if (m) m.group.header.hidden = true
	if (w) w.group.header.hidden = true
}

interface WorkspaceProps {
	onDemoExpired?: () => void
}

export default function Workspace({ onDemoExpired }: WorkspaceProps) {
	const apiRef = useRef<DockviewApi | null>(null)
	const swappedRef = useRef(false)
	// State (not just the ref above) so WorkspacePanelProvider's context value
	// actually updates once Dockview becomes ready -- a ref alone wouldn't
	// trigger the re-render context consumers need to stop seeing a null api.
	const [api, setApi] = useState<DockviewApi | null>(null)

	function handleToggleLayout() {
		const api = apiRef.current
		if (!api) return
		const mapPanel = api.getPanel('map')
		const widgetsPanel = api.getPanel('widgets')
		if (!mapPanel || !widgetsPanel) return

		swappedRef.current = !swappedRef.current
		// moveTo with 'right'/'left' position relative to the other group
		// effectively swaps which side the map lives on.
		mapPanel.api.moveTo({
			group: widgetsPanel.group,
			position: swappedRef.current ? 'right' : 'left',
		})
		// Dockview resets header visibility after a move — restore immediately.
		setTimeout(() => hideHeaders(api), 0)
	}

	function handleReady({ api }: DockviewReadyEvent) {
		apiRef.current = api
		setApi(api)
		const map = api.addPanel({
			id: 'map',
			component: 'map-widget',
			title: 'Global Map',
			params: { onToggleLayout: handleToggleLayout, onDemoExpired },
			minimumWidth: 420,
		})
		const widgets = api.addPanel({
			id: 'widgets',
			component: 'widget-panel',
			title: 'Widgets',
			position: { direction: 'right', referencePanel: 'map' },
			initialWidth: 680,
			minimumWidth: 240,
		})
		map.group.header.hidden = true
		widgets.group.header.hidden = true
	}

	return (
		<div
			className="sentinel-workspace"
			style={{ flex: 1, minHeight: 0, overflow: 'hidden', width: '100%', height: '100%' }}
		>
			<WorkspacePanelProvider api={api}>
				<DockviewReact
					className="dockview-theme-dark"
					components={COMPONENTS}
					onReady={handleReady}
				/>
			</WorkspacePanelProvider>
		</div>
	)
}
