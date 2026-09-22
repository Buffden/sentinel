'use client'

import { useEffect, useState } from 'react'
import { useWorkspacePanel } from '@/features/workspace/WorkspacePanelContext'
import { fetchEntityGraph, EntityGraphNotFoundError } from '@/entities/entity-graph/api'
import type { GraphEdge } from '@/entities/entity-graph/model'

interface RelationshipsTabProps {
	entityId: string
}

type LoadState =
	| { kind: 'loading' }
	| { kind: 'not_found' }
	| { kind: 'error'; message: string }
	| { kind: 'ready'; edges: GraphEdge[] }

// Rendering more than this many neighbors in a circular layout stops being
// readable (a real entity in this dev environment already has ~10 real
// PROXIMITY_EVENT edges, and the backend's own cap is 200) -- cap the layout
// and say so explicitly rather than either rendering all of them or silently
// dropping the rest with no indication more exist.
const MAX_RENDERED_NEIGHBORS = 12

const GRAPH_WIDTH = 380
const GRAPH_HEIGHT = 280
const CENTER_X = GRAPH_WIDTH / 2
const CENTER_Y = GRAPH_HEIGHT / 2 - 10
const NEIGHBOR_RADIUS = 95

function neighborPosition(index: number, total: number): { x: number; y: number } {
	const angle = (2 * Math.PI * index) / total - Math.PI / 2
	return {
		x: CENTER_X + NEIGHBOR_RADIUS * Math.cos(angle),
		y: CENTER_Y + NEIGHBOR_RADIUS * Math.sin(angle),
	}
}

function edgeTooltip(edge: GraphEdge): string {
	if (edge.edgeType === 'KNOWN_ASSOCIATE') {
		return edge.knownAssociateType
			? `Known associate: ${edge.knownAssociateType}`
			: 'Known associate'
	}
	const parts: string[] = []
	if (edge.minDistanceMetres !== null) parts.push(`${edge.minDistanceMetres.toFixed(0)} m`)
	if (edge.lastSeenMs !== null) parts.push(new Date(edge.lastSeenMs).toISOString())
	return parts.length > 0 ? `Proximity event: ${parts.join(', ')}` : 'Proximity event'
}

function RelationshipGraph({
	entityId,
	edges,
	onPivot,
}: {
	entityId: string
	edges: GraphEdge[]
	onPivot: (neighborId: string, anchorMs?: number) => void
}) {
	if (edges.length === 0) {
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
				No recorded relationships.
			</div>
		)
	}

	const shown = edges.slice(0, MAX_RENDERED_NEIGHBORS)
	const hiddenCount = edges.length - shown.length

	return (
		<div style={{ padding: 'var(--space-3)' }}>
			<svg width={GRAPH_WIDTH} height={GRAPH_HEIGHT}>
				{shown.map((edge, i) => {
					const pos = neighborPosition(i, shown.length)
					return (
						<line
							key={`edge-${edge.otherEntityId}-${i}`}
							x1={CENTER_X}
							y1={CENTER_Y}
							x2={pos.x}
							y2={pos.y}
							stroke="var(--color-text-muted)"
							strokeDasharray={edge.edgeType === 'KNOWN_ASSOCIATE' ? '4,3' : undefined}
						/>
					)
				})}

				<circle
					cx={CENTER_X}
					cy={CENTER_Y}
					r={20}
					fill="var(--color-bg-elevated)"
					stroke="var(--color-status-info)"
					strokeWidth={2}
				/>
				<text
					x={CENTER_X}
					y={CENTER_Y + 3}
					fill="var(--color-text-primary)"
					fontSize={8}
					fontFamily="var(--font-mono)"
					textAnchor="middle"
				>
					{entityId}
				</text>

				{shown.map((edge, i) => {
					const pos = neighborPosition(i, shown.length)
					return (
						<g
							key={`node-${edge.otherEntityId}-${i}`}
							onClick={() => onPivot(edge.otherEntityId, edge.lastSeenMs ?? undefined)}
							style={{ cursor: 'pointer' }}
						>
							<title>{edgeTooltip(edge)}</title>
							<circle
								cx={pos.x}
								cy={pos.y}
								r={16}
								fill="var(--color-bg-elevated)"
								stroke="var(--color-text-secondary)"
								strokeWidth={1.5}
							/>
							<text
								x={pos.x}
								y={pos.y + 3}
								fill="var(--color-text-secondary)"
								fontSize={8}
								fontFamily="var(--font-mono)"
								textAnchor="middle"
							>
								{edge.otherEntityId}
							</text>
						</g>
					)
				})}
			</svg>

			<div
				style={{
					fontSize: '9px',
					color: 'var(--color-text-muted)',
					fontFamily: 'var(--font-mono)',
					fontStyle: 'italic',
				}}
			>
				solid = PROXIMITY_EVENT (hover for distance/time), dashed = KNOWN_ASSOCIATE. Click a
				neighbor to open its own panel.
			</div>
			{hiddenCount > 0 && (
				<div
					style={{
						fontSize: '9px',
						color: 'var(--color-text-muted)',
						fontFamily: 'var(--font-mono)',
						marginTop: 'var(--space-1)',
					}}
				>
					+{hiddenCount} more relationship{hiddenCount === 1 ? '' : 's'} not shown
				</div>
			)}
		</div>
	)
}

// Owns the actual fetch, mounted fresh (keyed by entityId in the parent's
// rendering) rather than resetting state synchronously inside an effect --
// same fix HistoryTab's HistoryPoints already applies for the identical
// react-hooks/set-state-in-effect lint rule.
function RelationshipsGraphLoader({ entityId }: { entityId: string }) {
	const workspacePanel = useWorkspacePanel()
	const [state, setState] = useState<LoadState>({ kind: 'loading' })

	useEffect(() => {
		let cancelled = false
		fetchEntityGraph(entityId)
			.then((edges) => {
				if (!cancelled) setState({ kind: 'ready', edges })
			})
			.catch((err: unknown) => {
				if (cancelled) return
				if (err instanceof EntityGraphNotFoundError) {
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
	return (
		<RelationshipGraph
			entityId={entityId}
			edges={state.edges}
			onPivot={(neighborId, anchorMs) => workspacePanel?.openEntityDetail(neighborId, { anchorMs })}
		/>
	)
}

export default function RelationshipsTab({ entityId }: RelationshipsTabProps) {
	return <RelationshipsGraphLoader key={entityId} entityId={entityId} />
}
