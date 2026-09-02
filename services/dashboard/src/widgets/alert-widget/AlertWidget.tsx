'use client'

import { useEffect, useState } from 'react'
import WidgetHeader from '@/shared/ui/WidgetHeader'
import { fetchApi } from '@/features/auth/apiClient'
import { formatUtcTime } from '@/shared/lib/formatTime'
import { type Alert, signalLossDarkSinceMs } from '@/entities/alert/model'
import { wireToAlert, isValidWireAlertDto } from '@/entities/alert/adapter'

// STANDARD is the only priority the Alert Evaluator currently emits — every
// SIGNAL_LOSS alert in v1 is STANDARD. ELEVATED is a real DATA_MODEL.md enum
// value (docs/DATA_MODEL.md), reserved for a later phase/alert type, mapped
// here so the widget doesn't silently render uncolored the day it appears.
const priorityColor: Record<string, string> = {
	STANDARD: 'var(--color-status-warning)',
	ELEVATED: 'var(--color-status-critical)',
}

export default function AlertWidget() {
	// Keyed by alert_id: idempotent hydration, and the same shape CP7i's live
	// feed will merge into (duplicate alert_id must not create a second entry).
	const [alerts, setAlerts] = useState<Map<string, Alert>>(new Map())

	useEffect(() => {
		let cancelled = false
		fetchApi('/api/alerts')
			.then((res) => {
				if (!res.ok) return
				return res.json() as Promise<unknown[]>
			})
			.then((raw) => {
				if (cancelled || !raw) return
				const hydrated = raw.filter(isValidWireAlertDto).map(wireToAlert)
				setAlerts(new Map(hydrated.map((a) => [a.id, a])))
			})
			.catch(() => {
				// fetchApi throws on 401 (already redirects). Other errors:
				// panel stays empty; CP7i's live feed will populate it.
			})
		return () => {
			cancelled = true
		}
	}, [])

	const list = Array.from(alerts.values())

	return (
		<div
			style={{
				height: '100%',
				display: 'flex',
				flexDirection: 'column',
				background: 'var(--color-bg-panel)',
				overflow: 'hidden',
			}}
		>
			<WidgetHeader title="Alerts" badge={list.length} badgeColor="var(--color-status-critical)" />

			<div style={{ flex: 1, overflowY: 'auto' }}>
				{list.length === 0 && (
					<div
						style={{
							padding: 'var(--space-3)',
							fontSize: 'var(--font-size-xs)',
							color: 'var(--color-text-muted)',
							fontFamily: 'var(--font-mono)',
							fontStyle: 'italic',
						}}
					>
						No open alerts
					</div>
				)}

				{list.map((alert) => {
					const color = priorityColor[alert.priority] ?? 'var(--color-text-muted)'
					const darkSinceMs = signalLossDarkSinceMs(alert)
					return (
						<div
							key={alert.id}
							style={{
								padding: 'var(--space-2) var(--space-3)',
								borderBottom: '1px solid var(--color-border-subtle)',
								borderLeft: `3px solid ${color}`,
								cursor: 'pointer',
							}}
						>
							<div
								style={{
									display: 'flex',
									justifyContent: 'space-between',
									alignItems: 'center',
									marginBottom: 'var(--space-1)',
								}}
							>
								<span
									style={{
										fontFamily: 'var(--font-mono)',
										fontSize: 'var(--font-size-sm)',
										color: 'var(--color-text-primary)',
										fontWeight: 600,
									}}
								>
									{alert.entityId}
								</span>
								<span
									style={{
										fontSize: 'var(--font-size-xs)',
										color,
										fontFamily: 'var(--font-mono)',
									}}
								>
									{alert.alertType.replace('_', ' ')}
								</span>
							</div>
							<div
								style={{
									fontSize: 'var(--font-size-xs)',
									color: 'var(--color-text-muted)',
									fontFamily: 'var(--font-mono)',
								}}
							>
								{darkSinceMs !== null
									? `Dark since ${formatUtcTime(darkSinceMs)}`
									: `Detected ${formatUtcTime(alert.detectedAtMs)}`}
							</div>
						</div>
					)
				})}
			</div>
		</div>
	)
}
