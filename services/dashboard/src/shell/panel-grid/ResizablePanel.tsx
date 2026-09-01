'use client'

import { useCallback, useRef, useState } from 'react'

const ROW_STEP_PX = 80
const COL_STEP_PX = 80
const MAX_ROW_SPAN = 4
const MAX_COL_SPAN = 3

function deltaToSpan(start: number, delta: number, step: number, max: number): number {
	const steps = delta > 0 ? Math.floor(delta / step) : Math.ceil(delta / step)
	return Math.max(1, Math.min(max, start + steps))
}

interface ResizablePanelProps {
	children: React.ReactNode
	defaultRowSpan?: number
	defaultColSpan?: number
	onClose?: () => void
}

export default function ResizablePanel({ children, defaultRowSpan = 2, defaultColSpan = 1, onClose }: ResizablePanelProps) {
	const [rowSpan, setRowSpan] = useState(defaultRowSpan)
	const [colSpan, setColSpan] = useState(defaultColSpan)

	const rowDrag = useRef({ active: false, startY: 0, startSpan: 1 })
	const colDrag = useRef({ active: false, startX: 0, startSpan: 1 })

	const onRowMouseDown = useCallback((e: React.MouseEvent) => {
		e.preventDefault()
		rowDrag.current = { active: true, startY: e.clientY, startSpan: rowSpan }
		document.body.style.cursor = 'ns-resize'
		document.body.style.userSelect = 'none'

		function onMove(ev: MouseEvent) {
			if (!rowDrag.current.active) return
			setRowSpan(deltaToSpan(rowDrag.current.startSpan, ev.clientY - rowDrag.current.startY, ROW_STEP_PX, MAX_ROW_SPAN))
		}
		function onUp() {
			rowDrag.current.active = false
			document.body.style.cursor = ''
			document.body.style.userSelect = ''
			document.removeEventListener('mousemove', onMove)
			document.removeEventListener('mouseup', onUp)
		}
		document.addEventListener('mousemove', onMove)
		document.addEventListener('mouseup', onUp)
	}, [rowSpan])

	const onColMouseDown = useCallback((e: React.MouseEvent) => {
		e.preventDefault()
		colDrag.current = { active: true, startX: e.clientX, startSpan: colSpan }
		document.body.style.cursor = 'ew-resize'
		document.body.style.userSelect = 'none'

		function onMove(ev: MouseEvent) {
			if (!colDrag.current.active) return
			setColSpan(deltaToSpan(colDrag.current.startSpan, ev.clientX - colDrag.current.startX, COL_STEP_PX, MAX_COL_SPAN))
		}
		function onUp() {
			colDrag.current.active = false
			document.body.style.cursor = ''
			document.body.style.userSelect = ''
			document.removeEventListener('mousemove', onMove)
			document.removeEventListener('mouseup', onUp)
		}
		document.addEventListener('mousemove', onMove)
		document.addEventListener('mouseup', onUp)
	}, [colSpan])

	return (
		<div
			style={{
				gridRow: `span ${rowSpan}`,
				gridColumn: `span ${colSpan}`,
				position: 'relative',
				background: 'var(--color-bg-panel)',
				border: '1px solid var(--color-border)',
				overflow: 'hidden',
				display: 'flex',
				flexDirection: 'column',
				minWidth: 240,
				minHeight: 0,
			}}
		>
			<div style={{ flex: 1, overflow: 'hidden', minHeight: 0 }}>
				{children}
			</div>

			{onClose && (
				<button
					onClick={onClose}
					className="panel-close-btn"
					aria-label="Close widget"
					style={{
						position: 'absolute',
						top: 10,
						right: 10,
						zIndex: 20,
						background: 'none',
						border: 'none',
						cursor: 'pointer',
						color: 'var(--color-text-muted)',
						fontSize: 12,
						lineHeight: 1,
						padding: '2px 4px',
					}}
					onMouseEnter={(e) => { (e.currentTarget as HTMLButtonElement).style.color = 'var(--color-status-critical)' }}
					onMouseLeave={(e) => { (e.currentTarget as HTMLButtonElement).style.color = 'var(--color-text-muted)' }}
				>
					✕
				</button>
			)}

			{/* Bottom resize handle */}
			<div
				onMouseDown={onRowMouseDown}
				onDoubleClick={() => setRowSpan(defaultRowSpan)}
				style={{
					position: 'absolute',
					bottom: 0,
					left: 0,
					right: 0,
					height: 16,
					cursor: 'ns-resize',
					zIndex: 10,
					display: 'flex',
					alignItems: 'flex-end',
					justifyContent: 'center',
					paddingBottom: 2,
					background: 'linear-gradient(to top, rgba(59,130,246,0.12), transparent)',
				}}
				onMouseEnter={(e) => { (e.currentTarget as HTMLDivElement).style.background = 'linear-gradient(to top, rgba(59,130,246,0.45), transparent)' }}
				onMouseLeave={(e) => { (e.currentTarget as HTMLDivElement).style.background = 'linear-gradient(to top, rgba(59,130,246,0.12), transparent)' }}
			>
				<span style={{ color: 'var(--color-text-muted)', fontSize: 10, letterSpacing: 2, lineHeight: 1 }}>⋯</span>
			</div>

			{/* Right resize handle */}
			<div
				onMouseDown={onColMouseDown}
				onDoubleClick={() => setColSpan(defaultColSpan)}
				style={{
					position: 'absolute',
					top: 0,
					right: 0,
					bottom: 0,
					width: 16,
					cursor: 'ew-resize',
					zIndex: 10,
					display: 'flex',
					alignItems: 'center',
					justifyContent: 'flex-end',
					paddingRight: 2,
					background: 'linear-gradient(to left, rgba(59,130,246,0.12), transparent)',
				}}
				onMouseEnter={(e) => { (e.currentTarget as HTMLDivElement).style.background = 'linear-gradient(to left, rgba(59,130,246,0.45), transparent)' }}
				onMouseLeave={(e) => { (e.currentTarget as HTMLDivElement).style.background = 'linear-gradient(to left, rgba(59,130,246,0.12), transparent)' }}
			>
				<span style={{ color: 'var(--color-text-muted)', fontSize: 10, letterSpacing: 2, lineHeight: 1, writingMode: 'vertical-rl' }}>⋯</span>
			</div>
		</div>
	)
}
