'use client'

// Floating, draggable, collapsible filter island rendered inside the Map
// widget's canvas area. Owns no entity data — it only reports filter
// changes via onFiltersChange; MapWidget applies them to the render layer.
// Drag position and collapsed state are local UI state, not persisted.

import { useRef, useState } from 'react'
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

export default function FilterPanel({ filters, onFiltersChange, shownCount, totalCount }: FilterPanelProps) {
	const [collapsed, setCollapsed] = useState(false)
	const [position, setPosition] = useState({ x: 16, y: 16 })
	const dragRef = useRef<{ startX: number; startY: number; originX: number; originY: number } | null>(null)

	function onHeaderMouseDown(e: React.MouseEvent) {
		dragRef.current = { startX: e.clientX, startY: e.clientY, originX: position.x, originY: position.y }

		function onMouseMove(ev: MouseEvent) {
			if (!dragRef.current) return
			const { startX, startY, originX, originY } = dragRef.current
			setPosition({ x: originX + (ev.clientX - startX), y: originY + (ev.clientY - startY) })
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
					onClick={() => setCollapsed((c) => !c)}
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
