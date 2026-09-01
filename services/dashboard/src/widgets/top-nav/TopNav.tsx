export default function TopNav({
	right,
	background,
	borderless,
}: {
	right?: React.ReactNode
	background?: string
	borderless?: boolean
}) {
	return (
		<header
			style={{
				height: 'var(--topnav-height)',
				background: background ?? 'var(--color-bg-panel)',
				borderBottom: borderless ? 'none' : '1px solid var(--color-border)',
				display: 'flex',
				alignItems: 'center',
				justifyContent: 'space-between',
				padding: '0 var(--space-4)',
				flexShrink: 0,
			}}
		>
			<span
				style={{
					color: 'var(--color-text-primary)',
					fontFamily: 'var(--font-mono)',
					fontSize: 15,
					fontWeight: 700,
					letterSpacing: '0.1em',
					textTransform: 'uppercase',
				}}
			>
				Sentinel
			</span>
			{right ?? <div />}
		</header>
	)
}
