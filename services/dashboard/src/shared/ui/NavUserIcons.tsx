'use client'

import { useEffect, useState } from 'react'

async function logout() {
	await fetch('/api/auth/logout', { method: 'POST' }).catch(() => null)
	window.location.replace('/')
}

export default function NavUserIcons() {
	const [name, setName] = useState<string | null>(null)
	const [tooltipVisible, setTooltipVisible] = useState(false)

	useEffect(() => {
		fetch('/api/auth/me')
			.then((r) => r.ok ? r.json() : null)
			.then((data: { name?: string; email?: string } | null) => {
				if (data) setName(data.name ?? data.email ?? null)
			})
			.catch(() => null)
	}, [])

	return (
		<div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
			{/* Profile icon with hover tooltip */}
			<div
				style={{ position: 'relative', display: 'flex', alignItems: 'center' }}
				onMouseEnter={() => setTooltipVisible(true)}
				onMouseLeave={() => setTooltipVisible(false)}
			>
				<div
					title={name ?? 'Signed in'}
					style={{
						width: 20,
						height: 20,
						borderRadius: '50%',
						border: '1px solid var(--color-border)',
						display: 'flex',
						alignItems: 'center',
						justifyContent: 'center',
						cursor: 'default',
					}}
				>
					<svg width="11" height="11" viewBox="0 0 24 24" fill="none">
						<circle cx="12" cy="8" r="4" stroke="var(--color-text-secondary)" strokeWidth="1.5" />
						<path d="M4 20c0-4 3.6-7 8-7s8 3 8 7" stroke="var(--color-text-secondary)" strokeWidth="1.5" strokeLinecap="round" />
					</svg>
				</div>

				{tooltipVisible && name && (
					<div
						style={{
							position: 'absolute',
							top: '100%',
							right: 0,
							marginTop: 6,
							background: 'var(--color-bg-elevated)',
							border: '1px solid var(--color-border)',
							borderRadius: 4,
							padding: '4px 10px',
							whiteSpace: 'nowrap',
							fontSize: 11,
							color: 'var(--color-text-secondary)',
							fontFamily: 'var(--font-sans)',
							pointerEvents: 'none',
							zIndex: 200,
						}}
					>
						{name}
					</div>
				)}
			</div>

			{/* Logout icon */}
			<button
				onClick={logout}
				title="Sign out"
				style={{
					background: 'transparent',
					border: 'none',
					padding: 0,
					cursor: 'pointer',
					display: 'flex',
					alignItems: 'center',
				}}
			>
				<svg width="20" height="20" viewBox="0 0 24 24" fill="none">
					<path d="M9 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h4" stroke="var(--color-text-muted)" strokeWidth="1.5" strokeLinecap="round" />
					<polyline points="16 17 21 12 16 7" stroke="var(--color-text-muted)" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
					<line x1="21" y1="12" x2="9" y2="12" stroke="var(--color-text-muted)" strokeWidth="1.5" strokeLinecap="round" />
				</svg>
			</button>
		</div>
	)
}
