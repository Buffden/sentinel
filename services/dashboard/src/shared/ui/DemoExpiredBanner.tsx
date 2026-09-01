'use client'

// Shown when the WebSocket closes with code 4401 (demo session expired).
// Triggered by the live-feed hook (CP7f); this component is dormant until then.

interface DemoExpiredBannerProps {
	onDismiss: () => void
}

export default function DemoExpiredBanner({ onDismiss }: DemoExpiredBannerProps) {
	return (
		<div
			role="alert"
			style={{
				position: 'fixed',
				top: 'var(--topnav-height)',
				left: 0,
				right: 0,
				zIndex: 1000,
				background: 'var(--color-status-warning)',
				color: '#000',
				display: 'flex',
				alignItems: 'center',
				justifyContent: 'center',
				gap: 'var(--space-4)',
				padding: 'var(--space-2) var(--space-4)',
				fontFamily: 'var(--font-mono)',
				fontSize: 'var(--font-size-sm)',
				fontWeight: 600,
			}}
		>
			<span>Demo session ended. Sign in with Google for full access.</span>
			<a
				href="/login"
				style={{
					color: '#000',
					textDecoration: 'underline',
					fontWeight: 700,
					cursor: 'pointer',
				}}
			>
				Sign in
			</a>
			<button
				onClick={onDismiss}
				aria-label="Dismiss"
				style={{
					background: 'none',
					border: 'none',
					color: '#000',
					cursor: 'pointer',
					fontFamily: 'var(--font-mono)',
					fontSize: 'var(--font-size-sm)',
					fontWeight: 700,
					padding: '0 var(--space-1)',
					marginLeft: 'var(--space-2)',
				}}
			>
				✕
			</button>
		</div>
	)
}
