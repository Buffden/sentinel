'use client'

import { useCallback, useRef } from 'react'

interface SplitLayoutProps {
	left: React.ReactNode
	right: React.ReactNode
	leftPct: number
	onLeftPctChange: (pct: number) => void
}

const MIN_LEFT_PCT = 25
const MAX_LEFT_PCT = 75

export default function SplitLayout({ left, right, leftPct, onLeftPctChange }: SplitLayoutProps) {
	const containerRef = useRef<HTMLDivElement>(null)
	const dragging = useRef(false)

	const onMouseDown = useCallback(() => {
		dragging.current = true
		document.body.style.cursor = 'col-resize'
		document.body.style.userSelect = 'none'
	}, [])

	const onMouseMove = useCallback((e: React.MouseEvent) => {
		if (!dragging.current || !containerRef.current) return
		const rect = containerRef.current.getBoundingClientRect()
		const pct = ((e.clientX - rect.left) / rect.width) * 100
		onLeftPctChange(Math.min(MAX_LEFT_PCT, Math.max(MIN_LEFT_PCT, pct)))
	}, [])

	const onMouseUp = useCallback(() => {
		dragging.current = false
		document.body.style.cursor = ''
		document.body.style.userSelect = ''
	}, [])

	return (
		<div
			ref={containerRef}
			onMouseMove={onMouseMove}
			onMouseUp={onMouseUp}
			onMouseLeave={onMouseUp}
			style={{ display: 'flex', flex: 1, overflow: 'hidden', minHeight: 0 }}
		>
			{/* Left — map only */}
			<div style={{ width: `${leftPct}%`, minWidth: 0, overflow: 'hidden', display: 'flex', flexDirection: 'column' }}>
				{left}
			</div>

			{/* Resize handle */}
			<div
				onMouseDown={onMouseDown}
				style={{
					width: 4,
					flexShrink: 0,
					background: 'var(--color-border)',
					cursor: 'col-resize',
					transition: 'background 0.15s',
				}}
				onMouseEnter={(e) => { (e.currentTarget as HTMLDivElement).style.background = 'var(--color-status-info)' }}
				onMouseLeave={(e) => { (e.currentTarget as HTMLDivElement).style.background = 'var(--color-border)' }}
			/>

			{/* Right — all widgets */}
			<div style={{ flex: 1, minWidth: 0, overflow: 'hidden', display: 'flex', flexDirection: 'column' }}>
				{right}
			</div>
		</div>
	)
}
