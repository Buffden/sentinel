'use client'

import { useCallback, useEffect, useRef, useState } from 'react'
import WidgetHeader from '@/shared/ui/WidgetHeader'
import { fetchApi } from '@/features/auth/apiClient'
import { formatUtcTime } from '@/shared/lib/formatTime'
import {
	type Alert,
	signalLossDarkSinceMs,
	applyAlertUpdate,
	supersededEvidenceIds,
} from '@/entities/alert/model'
import { wireToAlert, isValidWireAlertDto } from '@/entities/alert/adapter'
import { useLiveFeed } from '@/features/live-feed/useLiveFeed'

// STANDARD and ELEVATED are the only priorities the Alert Evaluator emits
// (docs/DATA_MODEL.md's priority-by-alert_type mapping): SIGNAL_LOSS and
// UNSCHEDULED_PROXIMITY are STANDARD, COMPOSITE is ELEVATED.
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
			<span
				style={{
					fontSize: 'var(--font-size-xs)',
					color: 'var(--color-text-muted)',
					fontFamily: 'var(--font-mono)',
				}}
			>
				{label}
			</span>
			<span
				style={{
					fontSize: 'var(--font-size-xs)',
					color: 'var(--color-text-primary)',
					fontFamily: 'var(--font-mono)',
				}}
			>
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

// Safe accessor for a nested plain-object field inside an alert payload
// (COMPOSITE's payload nests signal_loss/proximity sub-objects rather than
// flattening them — docs/DATA_MODEL.md). Never throws on an unexpected shape.
function payloadObject(payload: Record<string, unknown>, key: string): Record<string, unknown> {
	const value = payload[key]
	return typeof value === 'object' && value !== null ? (value as Record<string, unknown>) : {}
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

// A COMPOSITE's supersedes_alert_ids, validated — never fabricated when
// missing or malformed, just an empty list.
function compositeChildIds(alert: Alert): string[] {
	const raw = alert.payload['supersedes_alert_ids']
	return Array.isArray(raw) ? raw.filter((id): id is string => typeof id === 'string') : []
}

// COMPOSITE's own payload carries no callsign (its evidence is nested
// signal_loss/proximity sub-objects, not a flat SIGNAL_LOSS-shaped payload).
// Its primary entity is always the signal-loss episode's own entity
// (buildCompositeAlert anchors on decision.selected_entity_id), so the
// superseded child alert — when known locally — supplies a readable
// callsign for the same id instead of falling back to the raw hex.
function compositeLabel(alert: Alert, alerts: Map<string, Alert>): string {
	const primaryChild = compositeChildIds(alert)
		.map((id) => alerts.get(id))
		.find((a) => a?.entityId === alert.entityId)
	const primaryLabel = primaryChild ? flightLabel(primaryChild) : alert.entityId
	return alert.counterpartyEntityId
		? `${primaryLabel} ↔ ${alert.counterpartyEntityId}`
		: primaryLabel
}

export default function AlertWidget() {
	// Keyed by alert_id: idempotent hydration, and the same shape the live
	// feed below merges into (duplicate alert_id must not create a second
	// entry — see applyAlertUpdate).
	const [alerts, setAlerts] = useState<Map<string, Alert>>(new Map())
	// Which single card is expanded, if any (top-level or nested). Re-clicking
	// the open card (or clicking a different one) collapses it — see
	// toggleExpanded below.
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

	const toggleExpanded = (alertId: string) => {
		setExpandedId((prev) => (prev === alertId ? null : alertId))
	}

	// CP5C: a COMPOSITE's superseded evidence renders nested underneath it,
	// not as its own top-level card. Membership is derived from the
	// COMPOSITE's own payload.supersedes_alert_ids (structural, checked every
	// render), not from the child's own status field — that field can lag
	// behind the COMPOSITE's publish (see applyAlertUpdate's monotonic merge).
	const supersededIds = supersededEvidenceIds(alerts.values())
	const topLevel = Array.from(alerts.values()).filter((a) => !supersededIds.has(a.id))

	// One alert card, expanded in place on click. `nested` renders it as
	// superseded evidence tucked under its COMPOSITE parent: gray border
	// regardless of priority, a SUPERSEDED badge, indented, no bottom divider
	// of its own (the parent's divider closes the group).
	function renderAlert(alert: Alert, nested = false) {
		const color = nested
			? 'var(--color-text-muted)'
			: (priorityColor[alert.priority] ?? 'var(--color-text-muted)')
		const darkSinceMs = signalLossDarkSinceMs(alert)
		const isExpanded = alert.id === expandedId
		const isComposite = alert.alertType === 'COMPOSITE'
		const childIds = isComposite ? compositeChildIds(alert) : []

		const signalLoss = isComposite ? payloadObject(alert.payload, 'signal_loss') : {}
		const proximity = isComposite ? payloadObject(alert.payload, 'proximity') : {}
		const distanceMetres = proximity['distance_metres']

		return (
			<div key={alert.id}>
				<div
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
						marginLeft: nested ? 'var(--space-3)' : 0,
						padding: 'var(--space-2) var(--space-3)',
						borderBottom: '1px solid var(--color-border-subtle)',
						borderLeft: `${isExpanded ? 4 : 3}px solid ${color}`,
						background: nested
							? 'var(--color-bg-panel)'
							: isExpanded
								? 'var(--color-bg-elevated)'
								: 'transparent',
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
								color: nested ? 'var(--color-text-secondary)' : 'var(--color-text-primary)',
								fontWeight: 600,
							}}
						>
							{isComposite ? compositeLabel(alert, alerts) : flightLabel(alert)}
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
							{nested && (
								<span
									style={{
										fontSize: '9px',
										color: 'var(--color-text-muted)',
										fontFamily: 'var(--font-mono)',
										border: '1px solid var(--color-border)',
										borderRadius: 'var(--panel-border-radius)',
										padding: '1px 5px',
										letterSpacing: '0.04em',
									}}
								>
									SUPERSEDED
								</span>
							)}
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
						{isComposite
							? `Correlated ${formatUtcTime(alert.detectedAtMs)} · ${formatPayloadNumber(distanceMetres, 'm apart')}`
							: darkSinceMs !== null
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
							{isComposite ? (
								<>
									<DetailRow label="ALERT ID" value={alert.id} />
									<DetailRow label="PRIORITY" value={alert.priority} />
									<DetailRow
										label="DARK SINCE"
										value={
											typeof signalLoss['dark_since_ms'] === 'number'
												? formatUtcTime(signalLoss['dark_since_ms'] as number)
												: '—'
										}
									/>
									<DetailRow
										label="LOSS SOURCE"
										value={
											typeof signalLoss['loss_source'] === 'string'
												? (signalLoss['loss_source'] as string)
												: '—'
										}
									/>
									<DetailRow label="COUNTERPARTY" value={alert.counterpartyEntityId ?? '—'} />
									<DetailRow label="DISTANCE" value={formatPayloadNumber(distanceMetres, ' m')} />
									<DetailRow
										label="EPISODE START"
										value={
											typeof proximity['episode_start_ms'] === 'number'
												? formatUtcTime(proximity['episode_start_ms'] as number)
												: '—'
										}
									/>
									<DetailRow
										label="CORRELATION WINDOW"
										value={
											typeof alert.payload['correlation_window_ms'] === 'number'
												? `${Math.round((alert.payload['correlation_window_ms'] as number) / 1000)} s`
												: '—'
										}
									/>
								</>
							) : (
								<>
									<DetailRow label="ALERT ID" value={alert.id} />
									<DetailRow label="ENTITY ID (ICAO24)" value={alert.entityId} />
									<DetailRow label="ENTITY TYPE" value={alert.entityType} />
									<DetailRow label="PRIORITY" value={alert.priority} />
									<DetailRow label="STATUS" value={alert.status} />
									<DetailRow label="DETECTED" value={formatUtcTime(alert.detectedAtMs)} />
									<DetailRow
										label="LAST KNOWN LAT"
										value={formatPayloadNumber(alert.payload['last_known_lat'], '°', 4)}
									/>
									<DetailRow
										label="LAST KNOWN LON"
										value={formatPayloadNumber(alert.payload['last_known_lon'], '°', 4)}
									/>
									<DetailRow
										label="ALTITUDE"
										value={formatPayloadNumber(alert.payload['last_known_altitude_m'], ' m')}
									/>
									<DetailRow
										label="SPEED"
										value={formatPayloadNumber(alert.payload['last_known_speed_mps'], ' m/s', 1)}
									/>
									<DetailRow
										label="COURSE"
										value={formatPayloadNumber(alert.payload['last_known_course_deg'], '°')}
									/>
								</>
							)}
						</div>
					)}
				</div>

				{childIds.map((childId) => {
					const child = alerts.get(childId)
					// Not fabricated when the child isn't known locally (e.g. the
					// page loaded after the supersession happened — GET /alerts
					// only returns NEW/ACKNOWLEDGED rows): the COMPOSITE renders
					// alone rather than inventing a placeholder row.
					return child ? renderAlert(child, true) : null
				})}
			</div>
		)
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
			<WidgetHeader
				title="Alerts"
				badge={topLevel.length}
				badgeColor="var(--color-status-critical)"
			/>

			<div style={{ flex: 1, overflowY: 'auto' }}>
				{topLevel.length === 0 && (
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

				{topLevel.map((alert) => renderAlert(alert))}
			</div>
		</div>
	)
}
