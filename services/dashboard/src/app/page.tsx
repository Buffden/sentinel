'use client'

import { useState } from 'react'
import TopNav from '@/widgets/top-nav/TopNav'
import Footer from '@/shell/Footer'
import SplitLayout from '@/shell/SplitLayout'
import MapWidget from '@/widgets/map-widget/MapWidget'
import Workspace from '@/workspace/Workspace'

export default function Page() {
	const [swapped, setSwapped] = useState(false)
	// mapPct is always the map's share of the total width (25–75%)
	const [mapPct, setMapPct] = useState(65)

	function handleToggleLayout() {
		setSwapped((s) => !s)
	}

	// When map is on the left, leftPct === mapPct.
	// When map is on the right, the left panel (workspace) gets (100 - mapPct).
	const leftPct = swapped ? 100 - mapPct : mapPct

	function handleLeftPctChange(pct: number) {
		// Convert whatever the left panel width is back to the map's width
		setMapPct(swapped ? 100 - pct : pct)
	}

	const mapWidget = <MapWidget onToggleLayout={handleToggleLayout} />
	const workspace = <Workspace />

	return (
		<div style={{ display: 'flex', flexDirection: 'column', height: '100vh', width: '100vw', overflow: 'hidden' }}>
			<TopNav />

			<SplitLayout
				left={swapped ? workspace : mapWidget}
				right={swapped ? mapWidget : workspace}
				leftPct={leftPct}
				onLeftPctChange={handleLeftPctChange}
			/>

			<Footer />
		</div>
	)
}
