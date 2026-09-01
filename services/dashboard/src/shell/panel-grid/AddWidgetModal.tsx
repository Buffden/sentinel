'use client'

import { useEffect, useRef, useState } from 'react'

export interface WidgetDefinition {
	id: string
	label: string
	category: string
}

export const AVAILABLE_WIDGETS: WidgetDefinition[] = [
	{ id: 'flight-info', label: 'Flight Info', category: 'Aviation' },
	{ id: 'alerts', label: 'Alerts', category: 'Aviation' },
	{ id: 'route-status', label: 'Route Status', category: 'Aviation' },
]

interface AddWidgetModalProps {
	activeWidgetIds: Set<string>
	onSave: (activeIds: Set<string>) => void
	onClose: () => void
}

export default function AddWidgetModal({ activeWidgetIds, onSave, onClose }: AddWidgetModalProps) {
	const [draft, setDraft] = useState<Set<string>>(new Set(activeWidgetIds))
	const [search, setSearch] = useState('')
	const overlayRef = useRef<HTMLDivElement>(null)

	// Close on Escape
	useEffect(() => {
		function onKey(e: KeyboardEvent) {
			if (e.key === 'Escape') onClose()
		}
		document.addEventListener('keydown', onKey)
		return () => document.removeEventListener('keydown', onKey)
	}, [onClose])

	function toggle(id: string) {
		setDraft((prev) => {
			const next = new Set(prev)
			if (next.has(id)) {
				next.delete(id)
			} else {
				next.add(id)
			}
			return next
		})
	}

	const filtered = AVAILABLE_WIDGETS.filter((w) =>
		w.label.toLowerCase().includes(search.toLowerCase())
	)

	const categories = Array.from(new Set(filtered.map((w) => w.category)))

	return (
		<div
			ref={overlayRef}
			onClick={(e) => { if (e.target === overlayRef.current) onClose() }}
			style={{
				position: 'fixed',
				inset: 0,
				zIndex: 1000,
				display: 'flex',
				alignItems: 'center',
				justifyContent: 'center',
				background: 'rgba(0,0,0,0.55)',
			}}
		>
			<div
				style={{
					background: 'var(--color-bg-elevated)',
					border: '1px solid var(--color-border)',
					width: 420,
					maxHeight: '70vh',
					display: 'flex',
					flexDirection: 'column',
					overflow: 'hidden',
				}}
			>
				{/* Header */}
				<div
					style={{
						display: 'flex',
						alignItems: 'center',
						justifyContent: 'space-between',
						padding: '10px var(--space-4)',
						borderBottom: '1px solid var(--color-border)',
						flexShrink: 0,
					}}
				>
					<span
						style={{
							fontFamily: 'var(--font-mono)',
							fontSize: 'var(--font-size-xs)',
							fontWeight: 700,
							letterSpacing: '0.1em',
							textTransform: 'uppercase',
							color: 'var(--color-text-primary)',
						}}
					>
						Widgets
					</span>
					<button
						onClick={onClose}
						style={{
							background: 'none',
							border: 'none',
							cursor: 'pointer',
							color: 'var(--color-text-muted)',
							fontSize: 16,
							lineHeight: 1,
							padding: '2px 4px',
						}}
						aria-label="Close"
					>
						✕
					</button>
				</div>

				{/* Search */}
				<div style={{ padding: 'var(--space-3) var(--space-4)', flexShrink: 0, borderBottom: '1px solid var(--color-border-subtle)' }}>
					<input
						type="text"
						placeholder="Search widgets..."
						value={search}
						onChange={(e) => setSearch(e.target.value)}
						autoFocus
						style={{
							width: '100%',
							background: 'var(--color-bg-panel)',
							border: '1px solid var(--color-border)',
							color: 'var(--color-text-primary)',
							fontFamily: 'var(--font-mono)',
							fontSize: 'var(--font-size-xs)',
							padding: '5px var(--space-3)',
							outline: 'none',
							boxSizing: 'border-box',
						}}
					/>
				</div>

				{/* Widget list */}
				<div style={{ flex: 1, overflowY: 'auto', padding: 'var(--space-2) 0' }}>
					{categories.map((cat) => (
						<div key={cat}>
							<div
								style={{
									padding: '4px var(--space-4) 2px',
									fontFamily: 'var(--font-mono)',
									fontSize: 'var(--font-size-xs)',
									letterSpacing: '0.1em',
									textTransform: 'uppercase',
									color: 'var(--color-text-muted)',
								}}
							>
								{cat}
							</div>
							{filtered
								.filter((w) => w.category === cat)
								.map((w) => {
									const active = draft.has(w.id)
									return (
										<button
											key={w.id}
											onClick={() => toggle(w.id)}
											style={{
												width: '100%',
												display: 'flex',
												alignItems: 'center',
												justifyContent: 'space-between',
												padding: '7px var(--space-4)',
												background: 'none',
												border: 'none',
												cursor: 'pointer',
												color: active ? 'var(--color-text-primary)' : 'var(--color-text-secondary)',
												fontFamily: 'var(--font-mono)',
												fontSize: 'var(--font-size-xs)',
												textAlign: 'left',
											}}
											onMouseEnter={(e) => {
												(e.currentTarget as HTMLButtonElement).style.background = 'var(--color-bg-panel)'
											}}
											onMouseLeave={(e) => {
												(e.currentTarget as HTMLButtonElement).style.background = 'none'
											}}
										>
											<span>{w.label}</span>
											<span
												style={{
													width: 14,
													height: 14,
													border: `1.5px solid ${active ? 'var(--color-status-info)' : 'var(--color-border)'}`,
													background: active ? 'var(--color-status-info)' : 'transparent',
													display: 'flex',
													alignItems: 'center',
													justifyContent: 'center',
													flexShrink: 0,
													fontSize: 9,
													color: 'var(--color-bg-app)',
												}}
											>
												{active ? '✓' : ''}
											</span>
										</button>
									)
								})}
						</div>
					))}
					{filtered.length === 0 && (
						<div
							style={{
								padding: 'var(--space-4)',
								color: 'var(--color-text-muted)',
								fontFamily: 'var(--font-mono)',
								fontSize: 'var(--font-size-xs)',
								textAlign: 'center',
							}}
						>
							No widgets match.
						</div>
					)}
				</div>

				{/* Footer */}
				<div
					style={{
						display: 'flex',
						gap: 'var(--space-2)',
						padding: 'var(--space-3) var(--space-4)',
						borderTop: '1px solid var(--color-border)',
						flexShrink: 0,
					}}
				>
					<button
						onClick={() => onSave(draft)}
						style={{
							flex: 1,
							padding: '6px 0',
							background: 'var(--color-status-info)',
							border: 'none',
							cursor: 'pointer',
							color: '#fff',
							fontFamily: 'var(--font-mono)',
							fontSize: 'var(--font-size-xs)',
							fontWeight: 700,
							letterSpacing: '0.08em',
							textTransform: 'uppercase',
						}}
					>
						Save
					</button>
					<button
						onClick={onClose}
						style={{
							padding: '6px var(--space-4)',
							background: 'none',
							border: '1px solid var(--color-border)',
							cursor: 'pointer',
							color: 'var(--color-text-muted)',
							fontFamily: 'var(--font-mono)',
							fontSize: 'var(--font-size-xs)',
							letterSpacing: '0.08em',
							textTransform: 'uppercase',
						}}
					>
						Cancel
					</button>
				</div>
			</div>
		</div>
	)
}
