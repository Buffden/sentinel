'use client'

import { useState } from 'react'
import PanelGrid from '@/shell/panel-grid/PanelGrid'
import ResizablePanel from '@/shell/panel-grid/ResizablePanel'
import AddWidgetCard from '@/shell/panel-grid/AddWidgetCard'
import AddWidgetModal from '@/shell/panel-grid/AddWidgetModal'
import AlertWidget from '@/widgets/alert-widget/AlertWidget'
import EntityDetailWidget from '@/widgets/entity-detail-widget/EntityDetailWidget'
import RouteStatusWidget from '@/widgets/route-status-widget/RouteStatusWidget'

const DEFAULT_ACTIVE = new Set(['entity-detail', 'alerts', 'route-status'])

export default function WidgetPanel() {
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
				{activeWidgetIds.has('entity-detail') && (
					<ResizablePanel
						defaultRowSpan={2}
						defaultColSpan={1}
						onClose={() => removeWidget('entity-detail')}
					>
						<EntityDetailWidget />
					</ResizablePanel>
				)}

				{activeWidgetIds.has('alerts') && (
					<ResizablePanel
						defaultRowSpan={2}
						defaultColSpan={1}
						onClose={() => removeWidget('alerts')}
					>
						<AlertWidget />
					</ResizablePanel>
				)}

				{activeWidgetIds.has('route-status') && (
					<ResizablePanel
						defaultRowSpan={2}
						defaultColSpan={1}
						onClose={() => removeWidget('route-status')}
					>
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
