export default function TopNav() {
	return (
		<header
			style={{
				height: 'var(--topnav-height)',
				background: 'var(--color-bg-panel)',
				borderBottom: '1px solid var(--color-border)',
				display: 'flex',
				alignItems: 'center',
				padding: '0 var(--space-4)',
				flexShrink: 0,
			}}
		>
			<span
				style={{
					color: 'var(--color-text-primary)',
					fontFamily: 'var(--font-mono)',
					fontSize: 'var(--font-size-sm)',
					fontWeight: 700,
					letterSpacing: '0.1em',
					textTransform: 'uppercase',
				}}
			>
				Sentinel
			</span>
		</header>
	)
}
