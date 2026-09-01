'use client'

import WidgetHeader from '@/shared/ui/WidgetHeader'

const SAMPLE_ALERTS = [
	{ id: 'a1', callsign: 'UAE204', type: 'SIGNAL_LOSS', since: '14:32 UTC', priority: 'critical' },
	{ id: 'a2', callsign: 'QTR571', type: 'SIGNAL_LOSS', since: '14:28 UTC', priority: 'critical' },
	{ id: 'a3', callsign: 'BAW442', type: 'SIGNAL_LOSS', since: '14:19 UTC', priority: 'warning' },
]

const priorityColor: Record<string, string> = {
	critical: 'var(--color-status-critical)',
	warning: 'var(--color-status-warning)',
}

export default function AlertWidget() {
	return (
		<div style={{ height: '100%', display: 'flex', flexDirection: 'column', background: 'var(--color-bg-panel)', overflow: 'hidden' }}>
			<WidgetHeader
				title="Alerts"
				badge={SAMPLE_ALERTS.length}
				badgeColor="var(--color-status-critical)"
			/>

			<div style={{ flex: 1, overflowY: 'auto' }}>
				{SAMPLE_ALERTS.map((alert) => (
					<div
						key={alert.id}
						style={{
							padding: 'var(--space-2) var(--space-3)',
							borderBottom: '1px solid var(--color-border-subtle)',
							borderLeft: `3px solid ${priorityColor[alert.priority]}`,
							cursor: 'pointer',
						}}
					>
						<div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 'var(--space-1)' }}>
							<span style={{ fontFamily: 'var(--font-mono)', fontSize: 'var(--font-size-sm)', color: 'var(--color-text-primary)', fontWeight: 600 }}>
								{alert.callsign}
							</span>
							<span style={{ fontSize: 'var(--font-size-xs)', color: priorityColor[alert.priority], fontFamily: 'var(--font-mono)' }}>
								{alert.type.replace('_', ' ')}
							</span>
						</div>
						<div style={{ fontSize: 'var(--font-size-xs)', color: 'var(--color-text-muted)', fontFamily: 'var(--font-mono)' }}>
							Dark since {alert.since}
						</div>
					</div>
				))}
			</div>
		</div>
	)
}
