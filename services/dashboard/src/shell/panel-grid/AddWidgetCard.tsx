'use client'

import { useState } from 'react'

interface AddWidgetCardProps {
	onClick: () => void
}

const BORDER_COLOR_IDLE = '#4b5563'
const BORDER_COLOR_HOVER = '#3b82f6'

export default function AddWidgetCard({ onClick }: AddWidgetCardProps) {
	const [hovered, setHovered] = useState(false)
	const borderColor = hovered ? BORDER_COLOR_HOVER : BORDER_COLOR_IDLE
	const contentColor = hovered ? BORDER_COLOR_HOVER : 'var(--color-text-muted)'

	return (
		<button
			onClick={onClick}
			onMouseEnter={() => setHovered(true)}
			onMouseLeave={() => setHovered(false)}
			style={{
				gridRow: 'span 1',
				gridColumn: 'span 1',
				position: 'relative',
				display: 'flex',
				flexDirection: 'column',
				alignItems: 'center',
				justifyContent: 'center',
				gap: 6,
				background: 'transparent',
				border: 'none',
				cursor: 'pointer',
				color: contentColor,
				fontFamily: 'var(--font-mono)',
				fontSize: 'var(--font-size-xs)',
				letterSpacing: '0.08em',
				textTransform: 'uppercase',
				padding: 0,
				minHeight: 0,
				transition: 'color 0.15s',
			}}
			aria-label="Add widget"
		>
			{/* SVG dashed border: dasharray controls dash length + gap */}
			<svg
				style={{ position: 'absolute', inset: 0, width: '100%', height: '100%', pointerEvents: 'none' }}
				xmlns="http://www.w3.org/2000/svg"
			>
				<rect
					x="2" y="2"
					width="calc(100% - 4px)" height="calc(100% - 4px)"
					fill="none"
					stroke={borderColor}
					strokeWidth="2.5"
					strokeDasharray="10 6"
					style={{ transition: 'stroke 0.15s', width: 'calc(100% - 4px)', height: 'calc(100% - 4px)' }}
				/>
			</svg>

			<span style={{ fontSize: 22, lineHeight: 1, fontWeight: 300 }}>+</span>
			<span>Add Widget</span>
		</button>
	)
}
