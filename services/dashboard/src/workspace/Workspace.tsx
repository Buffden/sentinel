'use client'

import { useRef } from 'react'
import { DockviewReact, type DockviewApi, type DockviewReadyEvent, type IDockviewPanelProps } from 'dockview-react'
import MapWidget from '@/widgets/map-widget/MapWidget'
import WidgetPanel from './WidgetPanel'

type DvFC = React.FunctionComponent<IDockviewPanelProps>

const COMPONENTS: Record<string, DvFC> = {
	'map-widget': MapWidget as unknown as DvFC,
	'widget-panel': WidgetPanel as unknown as DvFC,
}

function hideHeaders(api: DockviewApi) {
	const m = api.getPanel('map')
	const w = api.getPanel('widgets')
	if (m) m.group.header.hidden = true
	if (w) w.group.header.hidden = true
}

export default function Workspace() {
	const apiRef = useRef<DockviewApi | null>(null)
	const swappedRef = useRef(false)

	function handleToggleLayout() {
		const api = apiRef.current
		if (!api) return
		const mapPanel = api.getPanel('map')
		const widgetsPanel = api.getPanel('widgets')
		if (!mapPanel || !widgetsPanel) return

		swappedRef.current = !swappedRef.current
		// moveTo with 'right'/'left' position relative to the other group
		// effectively swaps which side the map lives on.
		mapPanel.api.moveTo({ group: widgetsPanel.group, position: swappedRef.current ? 'right' : 'left' })
		// Dockview resets header visibility after a move — restore immediately.
		setTimeout(() => hideHeaders(api), 0)
	}

	function handleReady({ api }: DockviewReadyEvent) {
		apiRef.current = api
		const map = api.addPanel({
			id: 'map',
			component: 'map-widget',
			title: 'Global Map',
			params: { onToggleLayout: handleToggleLayout },
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
		<div className="sentinel-workspace" style={{ flex: 1, minHeight: 0, overflow: 'hidden', width: '100%', height: '100%' }}>
			<DockviewReact
				className="dockview-theme-dark"
				components={COMPONENTS}
				onReady={handleReady}
			/>
		</div>
	)
}
