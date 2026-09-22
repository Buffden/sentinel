'use client'

import { useEffect, useState } from 'react'
import WidgetHeader from '@/shared/ui/WidgetHeader'
import { formatUtcTime } from '@/shared/lib/formatTime'
import { fetchEntityDetail, EntityDetailNotFoundError } from '@/entities/entity-detail/api'
import type { EntityDetail } from '@/entities/entity-detail/model'
import type { Alert } from '@/entities/alert/model'

// Overview tab only (Phase 09 FE-CP1). HISTORY and RELATIONSHIPS render as
// inert tabs -- matching the approved mockup's own annotation that those are
// later checkpoints (TimescaleDB track, Neo4j graph pivot respectively), not
// a broken or half-built feature.
type Tab = 'overview' | 'history' | 'relationships'

type LoadState =
	| { kind: 'loading' }
	| { kind: 'not_found' }
	| { kind: 'error'; message: string }
	| { kind: 'ready'; detail: EntityDetail }

interface EntityDetailWidgetProps {
	// Direct prop when used standalone/in tests; params fallback when Dockview
	// renders this as a panel -- same convention as MapWidgetProps.
	entityId?: string
	params?: { entityId?: string }
}

const TAB_LABELS: Record<Tab, string> = {
	overview: 'OVERVIEW',
	history: 'HISTORY',
	relationships: 'RELATIONSHIPS',
}

function TabBar({ active }: { active: Tab }) {
	return (
		<div style={{ display: 'flex', borderBottom: '1px solid var(--color-border)' }}>
			{(Object.keys(TAB_LABELS) as Tab[]).map((tab) => (
				<div
					key={tab}
					style={{
						flex: 1,
						textAlign: 'center',
						padding: 'var(--space-2) 0',
						fontSize: 'var(--font-size-xs)',
						fontFamily: 'var(--font-mono)',
						fontWeight: tab === active ? 600 : 400,
						color: tab === active ? 'var(--color-text-primary)' : 'var(--color-text-muted)',
						background: tab === active ? 'var(--color-bg-elevated)' : 'transparent',
						cursor: tab === active ? 'default' : 'not-allowed',
					}}
					title={tab === active ? undefined : 'Not yet implemented'}
				>
					{TAB_LABELS[tab]}
				</div>
			))}
		</div>
	)
}

function Row({ label, value }: { label: string; value: string }) {
	return (
		<div
			style={{
				display: 'flex',
				justifyContent: 'space-between',
				alignItems: 'center',
				padding: 'var(--space-2) var(--space-3)',
				borderBottom: '1px solid var(--color-border-subtle)',
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

// Same badge colors AlertWidget's own StatusBadge uses -- NEW=info,
// ACKNOWLEDGED=warning, everything else (RESOLVED/SUPERSEDED) neutral.
function AlertStatusBadge({ status }: { status: string }) {
	const color =
		status === 'NEW'
			? 'var(--color-status-info)'
			: status === 'ACKNOWLEDGED'
				? 'var(--color-status-warning)'
				: 'var(--color-text-muted)'
	return (
		<span
			style={{
				fontSize: '8px',
				color,
				fontFamily: 'var(--font-mono)',
				border: `1px solid ${color}`,
				borderRadius: 'var(--panel-border-radius)',
				padding: '1px 5px',
				letterSpacing: '0.04em',
				whiteSpace: 'nowrap',
			}}
		>
			{status}
		</span>
	)
}

function AlertRow({ alert }: { alert: Alert }) {
	const priorityColor =
		alert.priority === 'ELEVATED' ? 'var(--color-status-critical)' : 'var(--color-status-warning)'
	return (
		<div
			style={{
				padding: 'var(--space-2) var(--space-3)',
				borderLeft: `3px solid ${priorityColor}`,
				borderBottom: '1px solid var(--color-border-subtle)',
				background: 'var(--color-bg-elevated)',
			}}
		>
			<div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
				<span
					style={{
						fontSize: 'var(--font-size-xs)',
						color: 'var(--color-text-primary)',
						fontFamily: 'var(--font-mono)',
					}}
				>
					{alert.alertType}
				</span>
				<AlertStatusBadge status={alert.status} />
			</div>
			<div
				style={{
					fontSize: '10px',
					color: 'var(--color-text-muted)',
					fontFamily: 'var(--font-mono)',
					marginTop: 2,
				}}
			>
				{formatUtcTime(alert.detectedAtMs)}
				{alert.counterpartyEntityId && ` · with ${alert.counterpartyEntityId}`}
			</div>
		</div>
	)
}

export default function EntityDetailWidget({
	entityId: entityIdProp,
	params,
}: EntityDetailWidgetProps) {
	const entityId = entityIdProp ?? params?.entityId
	const [state, setState] = useState<LoadState>({ kind: 'loading' })

	useEffect(() => {
		if (!entityId) return
		let cancelled = false
		// No synchronous setState here: entityId is fixed for this widget
		// instance's whole lifetime (a new entity gets its own new panel, per
		// WorkspacePanelContext's deterministic per-entity panel id), so the
		// initial 'loading' state above already covers this effect's one run.
		fetchEntityDetail(entityId)
			.then((detail) => {
				if (!cancelled) setState({ kind: 'ready', detail })
			})
			.catch((err: unknown) => {
				if (cancelled) return
				if (err instanceof EntityDetailNotFoundError) {
					setState({ kind: 'not_found' })
				} else {
					setState({
						kind: 'error',
						message: err instanceof Error ? err.message : 'failed to load',
					})
				}
			})
		return () => {
			cancelled = true
		}
	}, [entityId])

	if (!entityId) {
		return null
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
			<WidgetHeader title={entityId} />
			<TabBar active="overview" />

			<div style={{ flex: 1, overflowY: 'auto' }}>
				{state.kind === 'loading' && (
					<div
						style={{
							padding: 'var(--space-3)',
							fontSize: 'var(--font-size-xs)',
							color: 'var(--color-text-muted)',
							fontFamily: 'var(--font-mono)',
						}}
					>
						Loading…
					</div>
				)}

				{state.kind === 'not_found' && (
					<div
						style={{
							padding: 'var(--space-3)',
							fontSize: 'var(--font-size-xs)',
							color: 'var(--color-text-muted)',
							fontFamily: 'var(--font-mono)',
						}}
					>
						Entity not found, or outside your workspace scope.
					</div>
				)}

				{state.kind === 'error' && (
					<div
						style={{
							padding: 'var(--space-3)',
							fontSize: 'var(--font-size-xs)',
							color: 'var(--color-status-critical)',
							fontFamily: 'var(--font-mono)',
						}}
					>
						{state.message}
					</div>
				)}

				{state.kind === 'ready' && (
					<>
						<div
							style={{
								display: 'flex',
								justifyContent: 'space-between',
								alignItems: 'center',
								padding: 'var(--space-2) var(--space-3)',
								borderBottom: '1px solid var(--color-border-subtle)',
							}}
						>
							<span
								style={{
									fontSize: 'var(--font-size-sm)',
									fontWeight: 600,
									color: 'var(--color-text-primary)',
									fontFamily: 'var(--font-mono)',
								}}
							>
								{state.detail.entity?.callsign ?? entityId}
							</span>
							<span style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
								<span
									style={{
										width: 8,
										height: 8,
										borderRadius: '50%',
										background: state.detail.entity
											? 'var(--color-status-live)'
											: 'var(--color-status-critical)',
										display: 'inline-block',
									}}
								/>
								<span
									style={{
										fontSize: '9px',
										fontFamily: 'var(--font-mono)',
										color: state.detail.entity
											? 'var(--color-status-live)'
											: 'var(--color-status-critical)',
									}}
								>
									{state.detail.entity ? 'LIVE' : 'DARK'}
								</span>
							</span>
						</div>

						{state.detail.entity ? (
							<>
								<Row
									label="LAT / LON"
									value={`${state.detail.entity.lat.toFixed(4)}, ${state.detail.entity.lon.toFixed(4)}`}
								/>
								<Row
									label="ALTITUDE"
									value={
										state.detail.entity.altitudeM !== null
											? `${state.detail.entity.altitudeM} m`
											: '—'
									}
								/>
								<Row
									label="SPEED"
									value={
										state.detail.entity.speedMps !== null
											? `${state.detail.entity.speedMps} m/s`
											: '—'
									}
								/>
								<Row
									label="COURSE"
									value={
										state.detail.entity.courseDeg !== null
											? `${state.detail.entity.courseDeg}°`
											: '—'
									}
								/>
								<Row label="LAST SEEN" value={formatUtcTime(state.detail.entity.eventTimeMs)} />
							</>
						) : (
							<div
								style={{
									padding: 'var(--space-2) var(--space-3)',
									fontSize: 'var(--font-size-xs)',
									color: 'var(--color-text-muted)',
									fontFamily: 'var(--font-mono)',
									fontStyle: 'italic',
								}}
							>
								No current live state -- see alerts below for last known position.
							</div>
						)}

						<div
							style={{
								padding: 'var(--space-2) var(--space-3) var(--space-1)',
								fontSize: '10px',
								fontWeight: 700,
								color: 'var(--color-text-secondary)',
								fontFamily: 'var(--font-mono)',
							}}
						>
							RECENT ALERTS
						</div>
						{state.detail.alerts.length === 0 && (
							<div
								style={{
									padding: 'var(--space-2) var(--space-3)',
									fontSize: 'var(--font-size-xs)',
									color: 'var(--color-text-muted)',
									fontFamily: 'var(--font-mono)',
									fontStyle: 'italic',
								}}
							>
								No alerts on record
							</div>
						)}
						{state.detail.alerts.map((alert) => (
							<AlertRow key={alert.id} alert={alert} />
						))}
					</>
				)}
			</div>
		</div>
	)
}
