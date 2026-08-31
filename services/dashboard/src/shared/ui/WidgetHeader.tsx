interface WidgetHeaderProps {
	title: string
	badge?: string | number
	badgeColor?: string
	meta?: string
	actions?: React.ReactNode
}

export default function WidgetHeader({ title, badge, badgeColor, meta, actions }: WidgetHeaderProps) {
	return (
		<div
			style={{
				height: 36,
				display: 'flex',
				alignItems: 'center',
				padding: '0 var(--space-3)',
				gap: 'var(--space-2)',
				borderBottom: '1px solid var(--color-border)',
				background: 'var(--color-bg-elevated)',
				flexShrink: 0,
			}}
		>
			<span
				style={{
					fontFamily: 'var(--font-mono)',
					fontSize: 'var(--font-size-xs)',
					color: 'var(--color-text-secondary)',
					textTransform: 'uppercase',
					letterSpacing: '0.07em',
					fontWeight: 600,
				}}
			>
				{title}
			</span>

			{badge !== undefined && (
				<span
					style={{
						fontFamily: 'var(--font-mono)',
						fontSize: 'var(--font-size-xs)',
						color: badgeColor ?? 'var(--color-text-muted)',
						fontWeight: 600,
					}}
				>
					{badge}
				</span>
			)}

			{meta && (
				<span
					style={{
						fontFamily: 'var(--font-mono)',
						fontSize: 'var(--font-size-xs)',
						color: 'var(--color-text-muted)',
					}}
				>
					{meta}
				</span>
			)}

			<div style={{ flex: 1 }} />

			{actions}
		</div>
	)
}
