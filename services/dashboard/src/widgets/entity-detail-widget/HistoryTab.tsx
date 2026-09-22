'use client'

import { useEffect, useState } from 'react'
import { formatUtcTime } from '@/shared/lib/formatTime'
import { fetchEntityHistory, EntityHistoryNotFoundError } from '@/entities/entity-history/api'
import type { HistoryPoint } from '@/entities/entity-history/model'

// Preset window widths, ending at anchorMs (or now, with no anchor). Matches
// the approved mockup's "last 2h" default -- GET /entities/:entity_id/history
// requires an explicit from_ms/to_ms (no server-side default), so the
// frontend always has to supply a concrete window; these are that window's
// only adjustable dimension for this checkpoint (a manual date-range picker
// is not part of this pass -- presets are the smallest useful control).
const WINDOW_PRESETS: { label: string; ms: number }[] = [
	{ label: '30m', ms: 30 * 60_000 },
	{ label: '2h', ms: 2 * 60 * 60_000 },
	{ label: '6h', ms: 6 * 60 * 60_000 },
	{ label: '24h', ms: 24 * 60 * 60_000 },
]
const DEFAULT_PRESET_INDEX = 1 // '2h'

type LoadState =
	| { kind: 'loading' }
	| { kind: 'not_found' }
	| { kind: 'error'; message: string }
	| { kind: 'ready'; points: HistoryPoint[] }

interface HistoryTabProps {
	entityId: string
	// Source-event-time anchor (an opening alert's own detected_at) for the
	// window's end -- see WorkspacePanelContext's OpenEntityDetailOptions.
	// Falls back to "now" with no anchor (opened generically, not from an
	// alert).
	anchorMs?: number
}

// Maps [min,max] -> [0,range], clamping so a flat/degenerate domain (all
// points at the same altitude, or a single point) doesn't divide by zero.
function scale(value: number, min: number, max: number, range: number): number {
	if (max <= min) return range / 2
	return ((value - min) / (max - min)) * range
}

const CHART_WIDTH = 360
const CHART_HEIGHT = 160

function AltitudeChart({ points }: { points: HistoryPoint[] }) {
	const withAltitude = points.filter(
		(p): p is HistoryPoint & { altitudeM: number } => p.altitudeM !== null,
	)

	if (withAltitude.length < 2) {
		return (
			<div
				style={{
					padding: 'var(--space-3)',
					fontSize: 'var(--font-size-xs)',
					color: 'var(--color-text-muted)',
					fontFamily: 'var(--font-mono)',
					fontStyle: 'italic',
				}}
			>
				Not enough altitude data in this window to plot a track.
			</div>
		)
	}

	const times = withAltitude.map((p) => p.timestampMs)
	const altitudes = withAltitude.map((p) => p.altitudeM)
	const minTime = Math.min(...times)
	const maxTime = Math.max(...times)
	const minAlt = Math.min(...altitudes)
	const maxAlt = Math.max(...altitudes)

	const coords = withAltitude.map((p) => ({
		x: scale(p.timestampMs, minTime, maxTime, CHART_WIDTH),
		// SVG y grows downward -- invert so higher altitude renders higher up.
		y: CHART_HEIGHT - scale(p.altitudeM, minAlt, maxAlt, CHART_HEIGHT),
	}))
	const pointsAttr = coords.map((c) => `${c.x.toFixed(1)},${c.y.toFixed(1)}`).join(' ')
	const last = coords[coords.length - 1]!

	return (
		<div style={{ padding: 'var(--space-3)' }}>
			<svg width={CHART_WIDTH} height={CHART_HEIGHT} style={{ display: 'block' }}>
				<polyline
					points={pointsAttr}
					fill="none"
					stroke="var(--color-status-info)"
					strokeWidth={2}
				/>
				<circle cx={coords[0]!.x} cy={coords[0]!.y} r={3} fill="var(--color-text-muted)" />
				<circle cx={last.x} cy={last.y} r={4} fill="var(--color-status-live)" />
			</svg>
			<div
				style={{
					display: 'flex',
					justifyContent: 'space-between',
					fontSize: '9px',
					color: 'var(--color-text-muted)',
					fontFamily: 'var(--font-mono)',
				}}
			>
				<span>{formatUtcTime(minTime)}</span>
				<span>{formatUtcTime(maxTime)}</span>
			</div>
			<div
				style={{
					fontSize: '9px',
					color: 'var(--color-text-muted)',
					fontFamily: 'var(--font-mono)',
					marginTop: 'var(--space-1)',
				}}
			>
				ALTITUDE: {minAlt.toFixed(0)}–{maxAlt.toFixed(0)} m
			</div>
		</div>
	)
}

// Owns the actual fetch for one fixed (entityId, fromMs, toMs) triple.
// Mounted fresh (via `key` in HistoryTab below) whenever any of those change,
// rather than resetting `state` synchronously inside an effect -- same fix
// EntityDetailWidget already applies for the same lint rule
// (react-hooks/set-state-in-effect), and for the same underlying reason: a
// fresh mount's own initial useState already covers "show loading for this
// fetch," so there is nothing to reset mid-lifetime.
function HistoryPoints({
	entityId,
	fromMs,
	toMs,
}: {
	entityId: string
	fromMs: number
	toMs: number
}) {
	const [state, setState] = useState<LoadState>({ kind: 'loading' })

	useEffect(() => {
		let cancelled = false
		fetchEntityHistory(entityId, fromMs, toMs)
			.then((points) => {
				if (!cancelled) setState({ kind: 'ready', points })
			})
			.catch((err: unknown) => {
				if (cancelled) return
				if (err instanceof EntityHistoryNotFoundError) {
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
	}, [entityId, fromMs, toMs])

	if (state.kind === 'loading') {
		return (
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
		)
	}
	if (state.kind === 'not_found') {
		return (
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
		)
	}
	if (state.kind === 'error') {
		return (
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
		)
	}
	if (state.points.length === 0) {
		return (
			<div
				style={{
					padding: 'var(--space-3)',
					fontSize: 'var(--font-size-xs)',
					color: 'var(--color-text-muted)',
					fontFamily: 'var(--font-mono)',
					fontStyle: 'italic',
				}}
			>
				No position history in this window.
			</div>
		)
	}
	return <AltitudeChart points={state.points} />
}

export default function HistoryTab({ entityId, anchorMs }: HistoryTabProps) {
	const [presetIndex, setPresetIndex] = useState(DEFAULT_PRESET_INDEX)
	// Captured once at mount, not re-read on every render: Date.now() is
	// impure and reading it directly during render would make this
	// component's output nondeterministic from React's point of view. No
	// anchor means "now" is fixed for this panel's lifetime, same as an
	// anchor would be.
	const [mountMs] = useState(() => Date.now())
	const toMs = anchorMs ?? mountMs
	const fromMs = toMs - WINDOW_PRESETS[presetIndex]!.ms

	return (
		<div>
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
					WINDOW
				</span>
				<select
					value={presetIndex}
					onChange={(e) => setPresetIndex(Number(e.target.value))}
					style={{
						background: 'var(--color-bg-elevated)',
						color: 'var(--color-text-primary)',
						border: '1px solid var(--color-border)',
						borderRadius: 'var(--panel-border-radius)',
						fontSize: 'var(--font-size-xs)',
						fontFamily: 'var(--font-mono)',
						padding: '2px 6px',
					}}
				>
					{WINDOW_PRESETS.map((preset, i) => (
						<option key={preset.label} value={i}>
							last {preset.label}
						</option>
					))}
				</select>
			</div>
			{anchorMs !== undefined && (
				<div
					style={{
						padding: 'var(--space-1) var(--space-3)',
						fontSize: '9px',
						color: 'var(--color-text-muted)',
						fontFamily: 'var(--font-mono)',
						fontStyle: 'italic',
					}}
				>
					Window ends at the alert that opened this panel.
				</div>
			)}

			<HistoryPoints
				key={`${entityId}-${fromMs}-${toMs}`}
				entityId={entityId}
				fromMs={fromMs}
				toMs={toMs}
			/>
		</div>
	)
}
