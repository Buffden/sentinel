'use client'

import { useCallback, useEffect, useRef, useState, type CSSProperties } from 'react'
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
import {
	patchAlertStatus,
	AlertTransitionError,
	type LifecycleTargetStatus,
} from '@/entities/alert/api'
import { useLiveFeed } from '@/features/live-feed/useLiveFeed'
import { useWorkspacePanel } from '@/features/workspace/WorkspacePanelContext'

// STANDARD and ELEVATED are the only priorities the Alert Evaluator emits
// (docs/DATA_MODEL.md's priority-by-alert_type mapping): SIGNAL_LOSS and
// UNSCHEDULED_PROXIMITY are STANDARD, COMPOSITE is ELEVATED.
const priorityColor: Record<string, string> = {
	STANDARD: 'var(--color-status-warning)',
	ELEVATED: 'var(--color-status-critical)',
}

// NEW/ACKNOWLEDGED get real semantic color (CLAUDE.md's status-color rule:
// blue = informational/interactive, amber = warning/elevated). Anything
// else (SUPERSEDED, or a status this widget doesn't otherwise expect)
// falls back to the same neutral look the nested-evidence badge already
// used before Phase 08 — RESOLVED never reaches this component at all,
// filtered out of topLevel below.
function statusBadgeColors(status: string): { border: string; text: string } {
	switch (status) {
		case 'NEW':
			return { border: 'var(--color-status-info)', text: 'var(--color-status-info)' }
		case 'ACKNOWLEDGED':
			return { border: 'var(--color-status-warning)', text: 'var(--color-status-warning)' }
		default:
			return { border: 'var(--color-border)', text: 'var(--color-text-muted)' }
	}
}

function StatusBadge({ status }: { status: string }) {
	const { border, text } = statusBadgeColors(status)
	return (
		<span
			style={{
				fontSize: '9px',
				color: text,
				fontFamily: 'var(--font-mono)',
				border: `1px solid ${border}`,
				borderRadius: 'var(--panel-border-radius)',
				padding: '1px 6px',
				letterSpacing: '0.04em',
				whiteSpace: 'nowrap',
			}}
		>
			{status}
		</span>
	)
}

// Acknowledge = blue (interactive action, matches NEW's badge color).
// Resolve = green (healthy/complete conclusion), regardless of current status.
function actionButtonStyle(color: string, disabled: boolean): CSSProperties {
	return {
		background: 'transparent',
		border: `1px solid ${color}`,
		borderRadius: 'var(--panel-border-radius)',
		color,
		fontSize: 'var(--font-size-xs)',
		fontFamily: 'var(--font-mono)',
		padding: '5px 12px',
		cursor: disabled ? 'default' : 'pointer',
		opacity: disabled ? 0.5 : 1,
	}
}

// Renders one label/value row inside an expanded alert card. Mirrors the
// same Row pattern EntityDetailWidget uses — not extracted to a shared
// component since these two callers are the only consumers so far.
//
// onClick (Phase 09 FE-CP1): when given, the value opens that entity_id's
// Entity Detail panel instead of rendering as plain text — the entry point
// AlertWidget provides into entity investigation.
function DetailRow({
	label,
	value,
	onClick,
}: {
	label: string
	value: string
	onClick?: () => void
}) {
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
				onClick={
					onClick
						? (e) => {
								e.stopPropagation()
								onClick()
							}
						: undefined
				}
				style={{
					fontSize: 'var(--font-size-xs)',
					color: onClick ? 'var(--color-status-info)' : 'var(--color-text-primary)',
					fontFamily: 'var(--font-mono)',
					cursor: onClick ? 'pointer' : 'default',
					textDecoration: onClick ? 'underline' : 'none',
				}}
			>
				{value}
			</span>
		</div>
	)
}

// Same layout as DetailRow, badge instead of plain text for the STATUS row.
function StatusDetailRow({ label, status }: { label: string; status: string }) {
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
			<StatusBadge status={status} />
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
	// null outside a WorkspacePanelProvider (e.g. this widget rendered
	// standalone in a test) -- entity_id values then render as plain,
	// non-clickable text instead of throwing.
	const workspacePanel = useWorkspacePanel()
	// Keyed by alert_id: idempotent hydration, and the same shape the live
	// feed below merges into (duplicate alert_id must not create a second
	// entry — see applyAlertUpdate).
	const [alerts, setAlerts] = useState<Map<string, Alert>>(new Map())
	// Which single card is expanded, if any (top-level or nested). Re-clicking
	// the open card (or clicking a different one) collapses it — see
	// toggleExpanded below.
	const [expandedId, setExpandedId] = useState<string | null>(null)
	const unmountedRef = useRef(false)
	// Demo sessions have no lifecycle authority (services/api's
	// requireOperatorRole) -- action buttons render only for 'operator'.
	// Fetched once, same pattern as WorkspaceScopeControl.
	const [role, setRole] = useState<'operator' | 'demo' | 'unknown'>('unknown')
	// alert_id currently mid-PATCH, disables that card's buttons and swaps
	// their label to a pending indicator.
	const [pendingIds, setPendingIds] = useState<Set<string>>(new Set())
	// alert_id -> last transition error message, cleared on the next attempt.
	const [actionErrors, setActionErrors] = useState<Map<string, string>>(new Map())

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

	useEffect(() => {
		fetch('/api/auth/me')
			.then((r) => (r.ok ? r.json() : null))
			.then((data: { role?: string } | null) => {
				setRole(data?.role === 'operator' ? 'operator' : 'demo')
			})
			.catch(() => setRole('demo'))
	}, [])

	// PATCHes the transition, then merges the API's own returned state (which
	// is authoritative on both success and a 409 conflict — see
	// entities/alert/api.ts) the same way a live alert-events frame would.
	// The WS broadcast this PATCH also triggers will arrive separately and
	// merge again, harmlessly — applyAlertUpdate is idempotent by alert_id.
	const handleTransition = useCallback(async (alertId: string, target: LifecycleTargetStatus) => {
		setPendingIds((prev) => new Set(prev).add(alertId))
		setActionErrors((prev) => {
			if (!prev.has(alertId)) return prev
			const next = new Map(prev)
			next.delete(alertId)
			return next
		})
		try {
			const updated = await patchAlertStatus(alertId, target)
			setAlerts((prev) => applyAlertUpdate(prev, updated))
		} catch (err) {
			const message = err instanceof AlertTransitionError ? err.message : 'Update failed'
			setActionErrors((prev) => new Map(prev).set(alertId, message))
		} finally {
			setPendingIds((prev) => {
				const next = new Set(prev)
				next.delete(alertId)
				return next
			})
		}
	}, [])

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
	// RESOLVED is terminal and removed from view immediately (Phase 08): GET
	// /alerts already only returns NEW/ACKNOWLEDGED rows, so lingering here
	// with a RESOLVED badge would only diverge from what a page reload shows.
	const topLevel = Array.from(alerts.values()).filter(
		(a) => !supersededIds.has(a.id) && a.status !== 'RESOLVED',
	)

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
							onClick={
								!isComposite && workspacePanel
									? (e) => {
											e.stopPropagation()
											workspacePanel.openEntityDetail(alert.entityId, {
												anchorMs: alert.detectedAtMs,
											})
										}
									: undefined
							}
							style={{
								fontFamily: 'var(--font-mono)',
								fontSize: 'var(--font-size-sm)',
								color: nested ? 'var(--color-text-secondary)' : 'var(--color-text-primary)',
								fontWeight: 600,
								cursor: !isComposite && workspacePanel ? 'pointer' : 'default',
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
							{/* nested is always shown as SUPERSEDED regardless of its own
							status field, which can lag behind the COMPOSITE's publish —
							see the supersededIds comment above. */}
							<StatusBadge status={nested ? 'SUPERSEDED' : alert.status} />
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
									<StatusDetailRow label="STATUS" status={alert.status} />
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
									<DetailRow
										label="COUNTERPARTY"
										value={alert.counterpartyEntityId ?? '—'}
										onClick={
											alert.counterpartyEntityId && workspacePanel
												? () =>
														workspacePanel.openEntityDetail(alert.counterpartyEntityId!, {
															anchorMs: alert.detectedAtMs,
														})
												: undefined
										}
									/>
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
									<DetailRow
										label="ENTITY ID (ICAO24)"
										value={alert.entityId}
										onClick={
											workspacePanel
												? () =>
														workspacePanel.openEntityDetail(alert.entityId, {
															anchorMs: alert.detectedAtMs,
														})
												: undefined
										}
									/>
									<DetailRow label="ENTITY TYPE" value={alert.entityType} />
									<DetailRow label="PRIORITY" value={alert.priority} />
									<StatusDetailRow label="STATUS" status={alert.status} />
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

							{/* Never for nested/superseded evidence -- terminal, system-owned.
							Only for NEW/ACKNOWLEDGED: RESOLVED is filtered out of topLevel
							before it ever reaches here, and SUPERSEDED/RESOLVED are terminal
							per US-13 either way. */}
							{!nested &&
								role === 'operator' &&
								(alert.status === 'NEW' || alert.status === 'ACKNOWLEDGED') && (
									<div
										style={{
											marginTop: 'var(--space-2)',
											paddingTop: 'var(--space-2)',
											borderTop: '1px solid var(--color-border-subtle)',
											display: 'flex',
											gap: 'var(--space-2)',
										}}
									>
										{alert.status === 'NEW' && (
											<button
												onClick={(e) => {
													e.stopPropagation()
													void handleTransition(alert.id, 'ACKNOWLEDGED')
												}}
												disabled={pendingIds.has(alert.id)}
												style={actionButtonStyle(
													'var(--color-status-info)',
													pendingIds.has(alert.id),
												)}
											>
												{pendingIds.has(alert.id) ? '...' : 'Acknowledge'}
											</button>
										)}
										<button
											onClick={(e) => {
												e.stopPropagation()
												void handleTransition(alert.id, 'RESOLVED')
											}}
											disabled={pendingIds.has(alert.id)}
											style={actionButtonStyle(
												'var(--color-status-live)',
												pendingIds.has(alert.id),
											)}
										>
											{pendingIds.has(alert.id) ? '...' : 'Resolve'}
										</button>
									</div>
								)}

							{actionErrors.has(alert.id) && (
								<div
									style={{
										marginTop: 'var(--space-1)',
										fontSize: 'var(--font-size-xs)',
										color: 'var(--color-status-critical)',
										fontFamily: 'var(--font-mono)',
									}}
								>
									{actionErrors.get(alert.id)}
								</div>
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
