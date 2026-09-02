'use client'

// Floating, draggable, collapsible filter island rendered inside the Map
// widget's canvas area. Owns no entity data — it only reports filter
// changes via onFiltersChange; MapWidget applies them to the render layer.
// Drag position and collapsed state are local UI state, not persisted.

import { useRef, useState } from 'react'
import { flushSync } from 'react-dom'
import {
	type AviationFilters,
	type AviationStatusFilter,
	DEFAULT_AVIATION_FILTERS,
} from '@/widgets/map-widget/layers/aviationLayer'

interface FilterPanelProps {
	filters: AviationFilters
	onFiltersChange: (filters: AviationFilters) => void
	shownCount: number
	totalCount: number
}

const PANEL_WIDTH = 212

interface DragState {
	startX: number
	startY: number
	originX: number
	originY: number
	// Container and panel bounds captured once at drag start (not re-measured
	// on every mousemove) so the panel can never be dragged fully or partially
	// out of the map widget's visible area.
	maxX: number
	maxY: number
}

export default function FilterPanel({ filters, onFiltersChange, shownCount, totalCount }: FilterPanelProps) {
	const [collapsed, setCollapsed] = useState(false)
	const [position, setPosition] = useState({ x: 16, y: 16 })
	const panelRef = useRef<HTMLDivElement>(null)
	const dragRef = useRef<DragState | null>(null)

	// Measures the panel's own current size (expanded or collapsed) against
	// its container and returns how far the panel may sit from the
	// container's top-left edge without any part of it overflowing.
	function measureMaxOffset(): { maxX: number; maxY: number } | null {
		const panel = panelRef.current
		const container = panel?.parentElement
		if (!panel || !container) return null
		const containerRect = container.getBoundingClientRect()
		return {
			maxX: Math.max(0, containerRect.width - panel.offsetWidth),
			maxY: Math.max(0, containerRect.height - panel.offsetHeight),
		}
	}

	// Collapsing only shrinks the panel, which can't overflow — but expanding
	// grows it back, and if the panel was previously dragged near an edge
	// while collapsed, the larger expanded body can push past the container
	// boundary. flushSync forces the collapsed-state DOM update to commit
	// synchronously so panelRef already reflects the new size by the time we
	// measure and clamp — done here in the event handler, not an effect,
	// since an effect can't call setState synchronously without risking
	// cascading renders (react-hooks/set-state-in-effect).
	function toggleCollapsed() {
		flushSync(() => setCollapsed((c) => !c))
		const bounds = measureMaxOffset()
		if (!bounds) return
		setPosition((prev) => ({
			x: clamp(prev.x, 0, bounds.maxX),
			y: clamp(prev.y, 0, bounds.maxY),
		}))
	}

	function onHeaderMouseDown(e: React.MouseEvent) {
		const bounds = measureMaxOffset()
		if (!bounds) return

		dragRef.current = {
			startX: e.clientX,
			startY: e.clientY,
			originX: position.x,
			originY: position.y,
			maxX: bounds.maxX,
			maxY: bounds.maxY,
		}

		function onMouseMove(ev: MouseEvent) {
			if (!dragRef.current) return
			const { startX, startY, originX, originY, maxX, maxY } = dragRef.current
			const nextX = clamp(originX + (ev.clientX - startX), 0, maxX)
			const nextY = clamp(originY + (ev.clientY - startY), 0, maxY)
			setPosition({ x: nextX, y: nextY })
		}
		function onMouseUp() {
			dragRef.current = null
			window.removeEventListener('mousemove', onMouseMove)
			window.removeEventListener('mouseup', onMouseUp)
		}
		window.addEventListener('mousemove', onMouseMove)
		window.addEventListener('mouseup', onMouseUp)
	}

	return (
		<div
			ref={panelRef}
			style={{
				position: 'absolute',
				left: position.x,
				top: position.y,
				width: PANEL_WIDTH,
				background: 'var(--color-bg-panel)',
				border: '1px solid #3a3a3a',
				boxShadow: '0 4px 12px rgba(0,0,0,0.5)',
				zIndex: 10,
				userSelect: 'none',
			}}
		>
			{/* Panel's own header: drag handle + title + collapse toggle */}
			<div
				onMouseDown={onHeaderMouseDown}
				style={{
					display: 'flex',
					alignItems: 'center',
					gap: 'var(--space-2)',
					height: 30,
					padding: '0 var(--space-2)',
					background: 'var(--color-bg-elevated)',
					borderBottom: collapsed ? 'none' : '1px solid var(--color-border)',
					cursor: 'grab',
				}}
			>
				<DragHandleIcon />
				<span
					style={{
						flex: 1,
						fontFamily: 'var(--font-mono)',
						fontSize: 'var(--font-size-xs)',
						fontWeight: 700,
						letterSpacing: '0.07em',
						color: 'var(--color-text-secondary)',
					}}
				>
					AVIATION FILTERS
				</span>
				<button
					onClick={toggleCollapsed}
					title={collapsed ? 'Expand' : 'Collapse'}
					style={{
						display: 'flex',
						alignItems: 'center',
						justifyContent: 'center',
						width: 20,
						height: 20,
						background: 'transparent',
						border: '1px solid var(--color-border)',
						borderRadius: 2,
						color: 'var(--color-text-secondary)',
						cursor: 'pointer',
					}}
				>
					<ChevronIcon direction={collapsed ? 'down' : 'up'} />
				</button>
			</div>

			{!collapsed && (
				<div style={{ padding: 'var(--space-2) var(--space-2) var(--space-3)' }}>
					<Field label="CALLSIGN">
						<input
							type="text"
							value={filters.callsignSearch}
							onChange={(e) => onFiltersChange({ ...filters, callsignSearch: e.target.value })}
							placeholder="Search callsign…"
							style={inputStyle}
						/>
					</Field>

					<Field label="STATUS">
						<select
							value={filters.status}
							onChange={(e) =>
								onFiltersChange({ ...filters, status: e.target.value as AviationStatusFilter })
							}
							style={{ ...inputStyle, cursor: 'pointer' }}
						>
							<option value="all">All</option>
							<option value="airborne">Airborne</option>
							<option value="grounded">Grounded</option>
						</select>
					</Field>

					<button
						onClick={() => onFiltersChange(DEFAULT_AVIATION_FILTERS)}
						style={{
							width: '100%',
							padding: 'var(--space-1) 0',
							marginTop: 'var(--space-1)',
							background: 'transparent',
							border: '1px solid var(--color-border)',
							borderRadius: 3,
							color: 'var(--color-text-secondary)',
							fontFamily: 'var(--font-mono)',
							fontSize: 'var(--font-size-xs)',
							fontWeight: 600,
							letterSpacing: '0.05em',
							cursor: 'pointer',
						}}
					>
						CLEAR FILTERS
					</button>

					<div
						style={{
							marginTop: 'var(--space-2)',
							paddingTop: 'var(--space-2)',
							borderTop: '1px solid var(--color-border-subtle)',
							fontSize: 'var(--font-size-xs)',
							color: 'var(--color-text-muted)',
							fontStyle: 'italic',
						}}
					>
						{shownCount} of {totalCount} aircraft shown
					</div>
				</div>
			)}
		</div>
	)
}

function Field({ label, children }: { label: string; children: React.ReactNode }) {
	return (
		<div style={{ marginBottom: 'var(--space-2)' }}>
			<label
				style={{
					display: 'block',
					marginBottom: 'var(--space-1)',
					fontFamily: 'var(--font-mono)',
					fontSize: 'var(--font-size-xs)',
					fontWeight: 600,
					letterSpacing: '0.03em',
					color: 'var(--color-text-secondary)',
				}}
			>
				{label}
			</label>
			{children}
		</div>
	)
}

const inputStyle: React.CSSProperties = {
	width: '100%',
	height: 26,
	padding: '0 var(--space-2)',
	background: 'var(--color-bg-app)',
	border: '1px solid var(--color-border)',
	borderRadius: 3,
	color: 'var(--color-text-primary)',
	fontFamily: 'var(--font-mono)',
	fontSize: 'var(--font-size-sm)',
	boxSizing: 'border-box',
}

function DragHandleIcon() {
	return (
		<svg width="10" height="14" viewBox="0 0 10 14" fill="var(--color-text-muted)">
			<circle cx="2" cy="2" r="1.4" />
			<circle cx="2" cy="7" r="1.4" />
			<circle cx="2" cy="12" r="1.4" />
			<circle cx="7" cy="2" r="1.4" />
			<circle cx="7" cy="7" r="1.4" />
			<circle cx="7" cy="12" r="1.4" />
		</svg>
	)
}

function clamp(value: number, min: number, max: number): number {
	return Math.min(Math.max(value, min), max)
}

function ChevronIcon({ direction }: { direction: 'up' | 'down' }) {
	return (
		<svg
			width="10"
			height="10"
			viewBox="0 0 10 10"
			fill="none"
			stroke="currentColor"
			strokeWidth="1.5"
			style={{ transform: direction === 'down' ? 'rotate(180deg)' : undefined }}
		>
			<path d="M1 7 L5 3 L9 7" />
		</svg>
	)
}
