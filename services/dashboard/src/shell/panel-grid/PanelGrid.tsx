interface PanelGridProps {
	children: React.ReactNode
}

export default function PanelGrid({ children }: PanelGridProps) {
	return (
		<div
			style={{
				display: 'grid',
				gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))',
				gridAutoRows: '160px',
				gap: 5,
				padding: 4,
				width: '100%',
				boxSizing: 'border-box',
				overflowY: 'auto',
				overflowX: 'hidden',
				height: '100%',
				background: 'var(--color-border-subtle)',
				alignContent: 'start',
			}}
		>
			{children}
		</div>
	)
}
