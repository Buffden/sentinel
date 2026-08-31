'use client'

import WidgetHeader from '@/shared/ui/WidgetHeader'

export default function MapWidget() {
	return (
		<div style={{ width: '100%', height: '100%', display: 'flex', flexDirection: 'column', background: '#0d1117' }}>
			<WidgetHeader title="Global Map" />

			<div style={{ flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
				<span style={{ fontFamily: 'var(--font-mono)', fontSize: 'var(--font-size-xs)', color: 'var(--color-text-muted)', letterSpacing: '0.08em', textTransform: 'uppercase' }}>
					Map — coming next
				</span>
			</div>
		</div>
	)
}
