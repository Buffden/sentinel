'use client'

import { useEffect, useRef, useState } from 'react'
import { Map as MLMap, setWorkerUrl } from 'maplibre-gl'
import type { MapboxOverlay } from '@deck.gl/mapbox'
import { MAP_STYLE_PRIMARY, MAP_STYLE_FALLBACK } from './mapStyle'
import { aviationLayer, type AircraftPosition } from './layers/aviationLayer'
import WidgetHeader from '@/shared/ui/WidgetHeader'

// Sample aircraft positions supplied from outside the layer definition.
// Replace this array with live REST/WebSocket data in a later checkpoint
// without touching the MapLibre or deck.gl lifecycle.
const SAMPLE_AIRCRAFT: AircraftPosition[] = [
	{ id: 'UAE204', lon: 16.3743, lat: 48.2124 },
	{ id: 'QTR571', lon: -0.1278, lat: 51.5074 },
	{ id: 'BAW442', lon: -73.7781, lat: 40.6413 },
	{ id: 'SIA321', lon: 103.8198, lat: 1.3521 },
	{ id: 'DLH411', lon: 8.6821, lat: 50.0379 },
]

// Point the GL worker at the static copy in /public so Turbopack
// does not need to bundle the worker file as a module chunk.
setWorkerUrl('/maplibre-gl-worker.mjs')

const BTN: React.CSSProperties = {
	display: 'flex',
	alignItems: 'center',
	justifyContent: 'center',
	height: 22,
	padding: '0 6px',
	background: 'transparent',
	borderWidth: 1,
	borderStyle: 'solid',
	borderColor: 'var(--color-border)',
	borderRadius: 3,
	color: 'var(--color-text-secondary)',
	cursor: 'pointer',
	fontFamily: 'var(--font-mono)',
	fontSize: 'var(--font-size-xs)',
	fontWeight: 600,
}

const BTN_ACTIVE: React.CSSProperties = {
	...BTN,
	background: 'var(--color-status-info)',
	borderColor: 'var(--color-status-info)',
	color: '#fff',
}

const ICON_BTN: React.CSSProperties = {
	...BTN,
	width: 24,
	padding: 0,
}

interface MapWidgetProps {
	// Direct prop when used outside Dockview; params fallback when Dockview renders this as a panel.
	onToggleLayout?: () => void
	params?: { onToggleLayout?: () => void }
}

export default function MapWidget({ onToggleLayout, params }: MapWidgetProps) {
	const toggleFn = onToggleLayout ?? params?.onToggleLayout
	const outerRef = useRef<HTMLDivElement>(null)
	const containerRef = useRef<HTMLDivElement>(null)
	const mapRef = useRef<MLMap | null>(null)
	const overlayRef = useRef<MapboxOverlay | null>(null)
	const [is3D, setIs3D] = useState(false)

	useEffect(() => {
		const container = containerRef.current
		// Guard: never create a second live instance. In React Strict Mode,
		// cleanup clears mapRef before the second setup runs, so this check
		// prevents only genuine concurrent duplicates, not the sequential
		// setup → cleanup → setup cycle that Strict Mode intentionally exercises.
		if (!container || mapRef.current) return

		// destroyed flag stops the load callback from running if cleanup fires
		// before the style finishes loading (can happen in Strict Mode dev)
		let destroyed = false

		const map = new MLMap({
			container,
			style: MAP_STYLE_PRIMARY,
			center: [20, 30],
			zoom: 2,
			pitch: 0,
			bearing: 0,
			pixelRatio: window.devicePixelRatio,
			canvasContextAttributes: {
				antialias: true,
				powerPreference: 'high-performance',
			},
			renderWorldCopies: false,
			attributionControl: false,
		})

		mapRef.current = map

		// Fall back to OpenFreeMap if the primary style cannot load
		let usedFallback = false
		map.on('error', (e) => {
			if (usedFallback || destroyed) return
			const msg = String((e as unknown as { error?: { message?: string } }).error?.message ?? '')
			if (msg.includes('style') || msg.includes('Unable to load')) {
				usedFallback = true
				map.setStyle(MAP_STYLE_FALLBACK)
			}
		})

		map.on('load', async () => {
			if (destroyed) return

			// Dynamic import ensures luma.gl initializes only once even when
			// Turbopack evaluates this module in multiple bundle contexts (Dockview).
			const { MapboxOverlay } = await import('@deck.gl/mapbox')

			// Attach deck.gl overlay once — all future layer updates use setProps
			const overlay = new MapboxOverlay({
				interleaved: false,
				useDevicePixels: true,
				layers: [aviationLayer.createLayer(SAMPLE_AIRCRAFT, {})],
			})

			map.addControl(overlay as unknown as import('maplibre-gl').IControl)
			overlayRef.current = overlay
		})

		// Notify MapLibre when container dimensions change (SplitLayout drag)
		// so it repaints correctly without reinitialising
		const resizeObserver = new ResizeObserver(() => {
			mapRef.current?.resize()
		})
		resizeObserver.observe(container)

		return () => {
			destroyed = true
			resizeObserver.disconnect()
			// map.remove() removes all attached controls including the deck.gl overlay
			overlayRef.current = null
			map.remove()
			mapRef.current = null
		}
	}, [])

	function toggle3D() {
		const next = !is3D
		setIs3D(next)
		mapRef.current?.setProjection({ type: next ? 'globe' : 'mercator' })
	}

	function toggleFullscreen() {
		if (!outerRef.current) return
		if (document.fullscreenElement) {
			document.exitFullscreen()
		} else {
			outerRef.current.requestFullscreen()
		}
	}

	const actions = (
		<>
			{/* 2D / 3D projection toggle */}
			<div style={{ display: 'flex', borderRadius: 3, overflow: 'hidden', border: '1px solid var(--color-border)' }}>
				<button
					style={{ ...BTN, borderWidth: 0, borderRadius: 0, ...(is3D ? {} : BTN_ACTIVE) }}
					onClick={() => !is3D || toggle3D()}
				>
					2D
				</button>
				<button
					style={{
						...BTN,
						borderWidth: 0,
						borderLeftWidth: 1,
						borderLeftStyle: 'solid',
						borderLeftColor: 'var(--color-border)',
						borderRadius: 0,
						...(is3D ? BTN_ACTIVE : {}),
					}}
					onClick={() => is3D || toggle3D()}
				>
					3D
				</button>
			</div>

			{/* Swap map/workspace sides */}
			<button style={ICON_BTN} onClick={toggleFn} title="Toggle map side">
				<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
					<rect x="3" y="3" width="18" height="18" rx="2" />
					<path d="M15 3v18" />
				</svg>
			</button>

			{/* Fullscreen */}
			<button style={ICON_BTN} onClick={toggleFullscreen} title="Fullscreen">
				<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
					<path d="M8 3H5a2 2 0 0 0-2 2v3" />
					<path d="M21 8V5a2 2 0 0 0-2-2h-3" />
					<path d="M3 16v3a2 2 0 0 0 2 2h3" />
					<path d="M16 21h3a2 2 0 0 0 2-2v-3" />
				</svg>
			</button>
		</>
	)

	return (
		<div ref={outerRef} style={{ width: '100%', height: '100%', display: 'flex', flexDirection: 'column' }}>
			<WidgetHeader title="Global Map" actions={actions} />
			<div style={{ flex: 1, position: 'relative' }}>
				<div ref={containerRef} style={{ width: '100%', height: '100%' }} />
				<div
					style={{
						position: 'absolute',
						bottom: 4,
						right: 6,
						fontSize: 11,
						color: 'rgba(255,255,255,0.45)',
						pointerEvents: 'none',
						userSelect: 'none',
					}}
				>
					© CARTO, © OpenStreetMap
				</div>
			</div>
		</div>
	)
}
