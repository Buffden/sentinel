'use client'

import { useCallback, useEffect, useRef, useState } from 'react'
import WidgetHeader from '@/shared/ui/WidgetHeader'
import { fetchApi } from '@/features/auth/apiClient'
import { formatUtcTime } from '@/shared/lib/formatTime'
import { type Alert, signalLossDarkSinceMs, applyAlertUpdate } from '@/entities/alert/model'
import { wireToAlert, isValidWireAlertDto } from '@/entities/alert/adapter'
import { useLiveFeed } from '@/features/live-feed/useLiveFeed'

// STANDARD is the only priority the Alert Evaluator currently emits — every
// SIGNAL_LOSS alert in v1 is STANDARD. ELEVATED is a real DATA_MODEL.md enum
// value (docs/DATA_MODEL.md), reserved for a later phase/alert type, mapped
// here so the widget doesn't silently render uncolored the day it appears.
const priorityColor: Record<string, string> = {
	STANDARD: 'var(--color-status-warning)',
	ELEVATED: 'var(--color-status-critical)',
}

// Renders one label/value row inside an expanded alert card. Mirrors the
// Row pattern in FlightInfoWidget — not extracted to a shared component
// since these two callers are the only consumers so far.
function DetailRow({ label, value }: { label: string; value: string }) {
	return (
		<div
			style={{
				display: 'flex',
				justifyContent: 'space-between',
				alignItems: 'center',
				padding: '3px 0',
			}}
		>
			<span style={{ fontSize: 'var(--font-size-xs)', color: 'var(--color-text-muted)', fontFamily: 'var(--font-mono)' }}>
				{label}
			</span>
			<span style={{ fontSize: 'var(--font-size-xs)', color: 'var(--color-text-primary)', fontFamily: 'var(--font-mono)' }}>
				{value}
			</span>
		</div>
	)
}

// payload numeric fields come from Redis via the API as number | null
// (docs/DATA_MODEL.md — SIGNAL_LOSS evidence). Anything else is unexpected.
function formatPayloadNumber(value: unknown, unit: string, digits = 0): string {
	return typeof value === 'number' ? `${value.toFixed(digits)}${unit}` : '—'
}

// Card header identifier: the flight callsign (e.g. "EZY92XM") reads far
// better than the entity_id icao24 hex it's keyed on. Older alert rows
// (persisted before callsign was added to the payload) and entities OpenSky
// never reported one for both fall back to the hex — still unique, just less
// readable.
function flightLabel(alert: Alert): string {
	const callsign = alert.payload['callsign']
	return typeof callsign === 'string' && callsign !== '' ? callsign : alert.entityId
}

export default function AlertWidget() {
	// Keyed by alert_id: idempotent hydration, and the same shape the live
	// feed below merges into (duplicate alert_id must not create a second
	// entry — see applyAlertUpdate).
	const [alerts, setAlerts] = useState<Map<string, Alert>>(new Map())
	// Which single card is expanded, if any. Re-clicking the open card (or
	// clicking a different one) collapses it — see toggleExpanded below.
	const [expandedId, setExpandedId] = useState<string | null>(null)
	const unmountedRef = useRef(false)

	// CP7h (initial mount) and CP7k (reconnect) both re-run this same fetch —
	// on reconnect, any alert published during the disconnected window was
	// never delivered over the (now-reopened) WebSocket, so REST is the only
	// way to recover it.
	const hydrateAlerts = useCallback(async () => {
		try {
			const res = await fetchApi('/api/alerts')
			if (!res.ok) return
			const raw = (await res.json()) as unknown[]
			if (unmountedRef.current) return
			const hydrated = raw.filter(isValidWireAlertDto).map(wireToAlert)
			setAlerts(new Map(hydrated.map((a) => [a.id, a])))
		} catch {
			// fetchApi throws on 401 (already redirects). Other errors:
			// panel stays empty; the live feed below can still populate it.
		}
	}, [])

	useEffect(() => {
		unmountedRef.current = false
		queueMicrotask(() => void hydrateAlerts())
		return () => {
			unmountedRef.current = true
		}
	}, [hydrateAlerts])

	// CP7i: new alerts appear without a page refresh. No ordering dependency
	// between hydration and the live feed — both paths upsert by alert_id, so
	// whichever arrives first, the final state converges the same either way.
	useLiveFeed({
		onAlertUpdate: (alert) => {
			setAlerts((prev) => applyAlertUpdate(prev, alert))
		},
		onReconnect: () => {
			void hydrateAlerts()
		},
	})

	const list = Array.from(alerts.values())

	const toggleExpanded = (alertId: string) => {
		setExpandedId((prev) => (prev === alertId ? null : alertId))
	}

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
					const isExpanded = alert.id === expandedId
					return (
						<div
							key={alert.id}
							role="button"
							tabIndex={0}
							aria-expanded={isExpanded}
							onClick={() => toggleExpanded(alert.id)}
							onKeyDown={(e) => {
								if (e.key === 'Enter' || e.key === ' ') {
									e.preventDefault()
									toggleExpanded(alert.id)
								}
							}}
							style={{
								padding: 'var(--space-2) var(--space-3)',
								borderBottom: '1px solid var(--color-border-subtle)',
								borderLeft: `${isExpanded ? 4 : 3}px solid ${color}`,
								background: isExpanded ? 'var(--color-bg-elevated)' : 'transparent',
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
									{flightLabel(alert)}
								</span>
								<span style={{ display: 'flex', alignItems: 'center', gap: 'var(--space-2)' }}>
									<span
										style={{
											fontSize: 'var(--font-size-xs)',
											color,
											fontFamily: 'var(--font-mono)',
										}}
									>
										{alert.alertType.replace('_', ' ')}
									</span>
									<span
										style={{
											fontSize: 'var(--font-size-xs)',
											color: 'var(--color-text-muted)',
											fontFamily: 'var(--font-mono)',
										}}
									>
										{isExpanded ? '▾' : '▸'}
									</span>
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

							{isExpanded && (
								<div
									style={{
										marginTop: 'var(--space-2)',
										paddingTop: 'var(--space-2)',
										borderTop: '1px solid var(--color-border-subtle)',
									}}
								>
									<DetailRow label="ALERT ID" value={alert.id} />
									<DetailRow label="ENTITY ID (ICAO24)" value={alert.entityId} />
									<DetailRow label="ENTITY TYPE" value={alert.entityType} />
									<DetailRow label="PRIORITY" value={alert.priority} />
									<DetailRow label="STATUS" value={alert.status} />
									<DetailRow label="DETECTED" value={formatUtcTime(alert.detectedAtMs)} />
									<DetailRow label="LAST KNOWN LAT" value={formatPayloadNumber(alert.payload['last_known_lat'], '°', 4)} />
									<DetailRow label="LAST KNOWN LON" value={formatPayloadNumber(alert.payload['last_known_lon'], '°', 4)} />
									<DetailRow label="ALTITUDE" value={formatPayloadNumber(alert.payload['last_known_altitude_m'], ' m')} />
									<DetailRow label="SPEED" value={formatPayloadNumber(alert.payload['last_known_speed_mps'], ' m/s', 1)} />
									<DetailRow label="COURSE" value={formatPayloadNumber(alert.payload['last_known_course_deg'], '°')} />
								</div>
							)}
						</div>
					)
				})}
			</div>
		</div>
	)
}
