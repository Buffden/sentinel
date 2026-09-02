'use client'

// Live-feed hook: the one blessed entry point for real-time data from the
// API's WebSocket. Knows about position-updates, alert-events, and the
// subscribe(bbox) message — callers (MapWidget, AlertWidget) know nothing
// about frames or channels, only TrackedEntity/Alert updates.
//
// Responsibilities:
//   - Subscribes to the shared, page-level WebSocket connection
//     (shared/realtime/liveSocket.ts) on mount, unsubscribes on unmount.
//     Does not own the connection itself — MapWidget and AlertWidget are
//     independently-mounted Dockview panels, and this hook may be called
//     by both at once; the underlying connection is a singleton so that
//     stays one WebSocket, not two.
//   - Exposes subscribe(bbox) so callers can set/update the server-side
//     bbox filter. Safe to call before the connection is open — the message
//     is dropped silently if the socket is not yet ready.
//   - Parses position-updates frames and calls onPositionUpdate with the
//     mapped TrackedEntity update.
//   - Parses alert-events frames and calls onAlertUpdate with the mapped Alert.
//   - Calls onDemoExpired when the server closes the connection with code 4401.
//
// The hook uses refs for callbacks so the subscription is created once and
// never recreated when the parent re-renders with new inline functions.

import { useCallback, useEffect, useLayoutEffect, useRef } from 'react'
import { acquireLiveSocket, type LiveFrame } from '@/shared/realtime/liveSocket'
import type { TrackedEntityUpdate } from '@/entities/tracked-entity/model'
import type { Alert } from '@/entities/alert/model'

// Wire shape of the data field in a position-updates WS frame.
// Different from WireEntityDto (REST): uses timestamp_ms, no entity_subtype/on_ground.
interface WsPositionData {
	entity_id: string
	entity_type: string | null
	timestamp_ms: number
	lat: number
	lon: number
	altitude_m: number | null
	speed_mps: number | null
	course_deg: number | null
	callsign: string | null
}

// entitySubtype and onGround are intentionally absent from the return value,
// not set to null — the WS frame doesn't carry them, and applyPositionUpdate
// merges by key, so omitting them preserves whatever REST hydration (or an
// earlier frame) already established instead of erasing it every tick.
function parsePositionFrame(data: unknown): TrackedEntityUpdate | null {
	if (!data || typeof data !== 'object') return null
	const d = data as Record<string, unknown>
	if (
		typeof d['entity_id'] !== 'string' ||
		typeof d['lat'] !== 'number' ||
		typeof d['lon'] !== 'number' ||
		typeof d['timestamp_ms'] !== 'number'
	)
		return null

	const p = d as unknown as WsPositionData
	return {
		id: p.entity_id,
		lat: p.lat,
		lon: p.lon,
		altitudeM: typeof p.altitude_m === 'number' ? p.altitude_m : null,
		speedMps: typeof p.speed_mps === 'number' ? p.speed_mps : null,
		courseDeg: typeof p.course_deg === 'number' ? p.course_deg : null,
		eventTimeMs: p.timestamp_ms,
		entityType: typeof p.entity_type === 'string' ? p.entity_type : null,
		callsign: typeof p.callsign === 'string' ? p.callsign : null,
	}
}

// Wire shape of the data field in an alert-events WS frame: the Alert
// Evaluator's Kafka message, republished verbatim by the API's alert sink.
// Different from WireAlertDto (REST): detected_at_ms is a number (straight
// from Kafka), not detected_at as an ISO string (from the DB), and there is
// no updated_at/acknowledged_at/resolved_at — those are DB-only columns.
interface WsAlertData {
	alert_id: string
	entity_id: string
	entity_type: string
	alert_type: string
	priority: string
	status: string
	detected_at_ms: number
	payload: Record<string, unknown>
}

function parseAlertFrame(data: unknown): Alert | null {
	if (!data || typeof data !== 'object') return null
	const d = data as Record<string, unknown>
	if (
		typeof d['alert_id'] !== 'string' ||
		typeof d['entity_id'] !== 'string' ||
		typeof d['alert_type'] !== 'string' ||
		typeof d['status'] !== 'string' ||
		typeof d['detected_at_ms'] !== 'number'
	)
		return null

	const a = d as unknown as WsAlertData
	return {
		id: a.alert_id,
		alertType: a.alert_type,
		entityId: a.entity_id,
		entityType: a.entity_type,
		status: a.status,
		priority: a.priority,
		detectedAtMs: a.detected_at_ms,
		payload: a.payload ?? {},
	}
}

interface UseLiveFeedOptions {
	onPositionUpdate?: (entity: TrackedEntityUpdate) => void
	onAlertUpdate?: (alert: Alert) => void
	onDemoExpired?: () => void
}

export function useLiveFeed({
	onPositionUpdate,
	onAlertUpdate,
	onDemoExpired,
}: UseLiveFeedOptions) {
	// Refs so the subscription effect never needs to re-run when callbacks change.
	const onPositionUpdateRef = useRef(onPositionUpdate)
	const onAlertUpdateRef = useRef(onAlertUpdate)
	const onDemoExpiredRef = useRef(onDemoExpired)

	// Keep refs current after every render (useLayoutEffect runs synchronously,
	// satisfying the eslint-plugin-react-compiler rule about ref writes).
	useLayoutEffect(() => {
		onPositionUpdateRef.current = onPositionUpdate
		onAlertUpdateRef.current = onAlertUpdate
		onDemoExpiredRef.current = onDemoExpired
	})

	// Stable ref to the shared socket's send function.
	const sendRef = useRef<((data: string) => void) | null>(null)

	useEffect(() => {
		const wsUrl = process.env['NEXT_PUBLIC_WS_URL'] ?? 'ws://localhost:3000'
		const socket = acquireLiveSocket(wsUrl)
		sendRef.current = socket.send

		const unsubFrame = socket.onFrame((frame: LiveFrame) => {
			if (frame.channel === 'position-updates') {
				const entity = parsePositionFrame(frame.data)
				if (entity) onPositionUpdateRef.current?.(entity)
			} else if (frame.channel === 'alert-events') {
				const alert = parseAlertFrame(frame.data)
				if (alert) onAlertUpdateRef.current?.(alert)
			}
		})
		const unsubDemoExpired = socket.onDemoExpired(() => onDemoExpiredRef.current?.())

		return () => {
			unsubFrame()
			unsubDemoExpired()
			sendRef.current = null
			socket.release()
		}
	}, []) // Intentionally empty: subscribes once per mount, uses refs for callbacks.

	// Stable subscribe function. Sends a bbox subscription to the server.
	// Safe to call before the socket opens — silently dropped if not ready.
	const subscribe = useCallback((bbox: [number, number, number, number]) => {
		sendRef.current?.(JSON.stringify({ type: 'subscribe', bbox }))
	}, [])

	return { subscribe }
}
