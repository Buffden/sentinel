'use client'

import WidgetHeader from '@/shared/ui/WidgetHeader'

const SAMPLE_ROUTES = [
	{ id: 'r1', callsign: 'SIA321', origin: 'SIN', dest: 'LHR', status: 'on-route', deviation: null },
	{ id: 'r2', callsign: 'DLH411', origin: 'FRA', dest: 'JFK', status: 'deviation', deviation: '+4.2 nm' },
	{ id: 'r3', callsign: 'AAL100', origin: 'ORD', dest: 'LAX', status: 'on-route', deviation: null },
	{ id: 'r4', callsign: 'THY003', origin: 'IST', dest: 'GRU', status: 'deviation', deviation: '+11.8 nm' },
]

const statusColor: Record<string, string> = {
	'on-route': 'var(--color-status-live)',
	deviation: 'var(--color-status-warning)',
}

export default function RouteStatusWidget() {
	const deviationCount = SAMPLE_ROUTES.filter((r) => r.status === 'deviation').length

	return (
		<div style={{ height: '100%', display: 'flex', flexDirection: 'column', background: 'var(--color-bg-panel)', overflow: 'hidden' }}>
			<WidgetHeader
				title="Route Status"
				badge={deviationCount}
				badgeColor="var(--color-status-warning)"
				meta="Aviation"
			/>

			<div style={{ flex: 1, overflowY: 'auto' }}>
				{SAMPLE_ROUTES.map((route) => (
					<div
						key={route.id}
						style={{
							padding: 'var(--space-2) var(--space-3)',
							borderBottom: '1px solid var(--color-border-subtle)',
							borderLeft: `3px solid ${statusColor[route.status]}`,
							cursor: 'pointer',
						}}
					>
						<div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 'var(--space-1)' }}>
							<span style={{ fontFamily: 'var(--font-mono)', fontSize: 'var(--font-size-sm)', color: 'var(--color-text-primary)', fontWeight: 600 }}>
								{route.callsign}
							</span>
							<span style={{ fontFamily: 'var(--font-mono)', fontSize: 'var(--font-size-xs)', color: statusColor[route.status] }}>
								{route.status === 'deviation' ? route.deviation : 'On Route'}
							</span>
						</div>
						<div style={{ fontFamily: 'var(--font-mono)', fontSize: 'var(--font-size-xs)', color: 'var(--color-text-muted)' }}>
							{route.origin} → {route.dest}
						</div>
					</div>
				))}
			</div>
		</div>
	)
}
