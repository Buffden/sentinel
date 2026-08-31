'use client'

import { useState } from 'react'
import PanelGrid from '@/shell/panel-grid/PanelGrid'
import ResizablePanel from '@/shell/panel-grid/ResizablePanel'
import AddWidgetCard from '@/shell/panel-grid/AddWidgetCard'
import AddWidgetModal from '@/shell/panel-grid/AddWidgetModal'
import AlertWidget from '@/widgets/alert-widget/AlertWidget'
import FlightInfoWidget from '@/widgets/flight-info-widget/FlightInfoWidget'
import RouteStatusWidget from '@/widgets/route-status-widget/RouteStatusWidget'

const DEFAULT_ACTIVE = new Set(['flight-info', 'alerts', 'route-status'])

export default function Workspace() {
	const [activeWidgetIds, setActiveWidgetIds] = useState<Set<string>>(DEFAULT_ACTIVE)
	const [modalOpen, setModalOpen] = useState(false)

	function handleSave(ids: Set<string>) {
		setActiveWidgetIds(ids)
		setModalOpen(false)
	}

	function removeWidget(id: string) {
		setActiveWidgetIds((prev) => {
			const next = new Set(prev)
			next.delete(id)
			return next
		})
	}

	return (
		<>
			<PanelGrid>
				{activeWidgetIds.has('flight-info') && (
					<ResizablePanel defaultRowSpan={2} defaultColSpan={1} onClose={() => removeWidget('flight-info')}>
						<FlightInfoWidget />
					</ResizablePanel>
				)}

				{activeWidgetIds.has('alerts') && (
					<ResizablePanel defaultRowSpan={2} defaultColSpan={1} onClose={() => removeWidget('alerts')}>
						<AlertWidget />
					</ResizablePanel>
				)}

				{activeWidgetIds.has('route-status') && (
					<ResizablePanel defaultRowSpan={2} defaultColSpan={1} onClose={() => removeWidget('route-status')}>
						<RouteStatusWidget />
					</ResizablePanel>
				)}

				<AddWidgetCard onClick={() => setModalOpen(true)} />
			</PanelGrid>

			{modalOpen && (
				<AddWidgetModal
					activeWidgetIds={activeWidgetIds}
					onSave={handleSave}
					onClose={() => setModalOpen(false)}
				/>
			)}
		</>
	)
}
