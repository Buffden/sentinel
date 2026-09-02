'use client'

// Live-feed hook: manages the WebSocket connection to the API.
//
// Responsibilities:
//   - Opens a WebSocket on mount and closes it on unmount.
//   - Reconnects with a fixed 5 s delay on unexpected disconnection.
//   - Does NOT reconnect on close code 4401 (demo session expired).
//   - Exposes subscribe(bbox) so callers can set/update the server-side
//     bbox filter. Safe to call before the connection is open — the message
//     is dropped silently if the socket is not yet ready.
//   - Parses position-updates frames and calls onPositionUpdate with the
//     mapped TrackedEntity.
//   - Calls onDemoExpired when the server closes with code 4401.
//
// The hook uses refs for callbacks so the WebSocket is created once and
// never recreated when the parent re-renders with new inline functions.

import { useCallback, useEffect, useLayoutEffect, useRef } from 'react'
import { openWebSocket } from '@/shared/realtime/websocketClient'
import type { TrackedEntityUpdate } from '@/entities/tracked-entity/model'

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

// Reconnect delay in ms after an unexpected close.
const RECONNECT_DELAY_MS = 5_000

// Close code the API sends when a demo JWT expires.
const DEMO_EXPIRED_CODE = 4401

interface UseLiveFeedOptions {
	onPositionUpdate: (entity: TrackedEntityUpdate) => void
	onDemoExpired?: () => void
}

export function useLiveFeed({ onPositionUpdate, onDemoExpired }: UseLiveFeedOptions) {
	// Refs so the WebSocket effect never needs to re-run when callbacks change.
	const onPositionUpdateRef = useRef(onPositionUpdate)
	const onDemoExpiredRef = useRef(onDemoExpired)

	// Keep refs current after every render (useLayoutEffect runs synchronously,
	// satisfying the eslint-plugin-react-compiler rule about ref writes).
	useLayoutEffect(() => {
		onPositionUpdateRef.current = onPositionUpdate
		onDemoExpiredRef.current = onDemoExpired
	})

	// Stable ref to the active send function, set when the socket opens.
	const sendRef = useRef<((data: string) => void) | null>(null)

	useEffect(() => {
		let stopped = false
		let reconnectTimer: ReturnType<typeof setTimeout> | null = null
		let activeClose: (() => void) | null = null

		const wsUrl = process.env['NEXT_PUBLIC_WS_URL'] ?? 'ws://localhost:3000'

		function connect() {
			const handle = openWebSocket(wsUrl, {
				onOpen: () => {
					sendRef.current = handle.send
				},
				onMessage: (raw) => {
					let frame: { channel: string; data: unknown }
					try {
						frame = JSON.parse(raw) as { channel: string; data: unknown }
					} catch {
						return
					}
					if (frame.channel === 'position-updates') {
						const entity = parsePositionFrame(frame.data)
						if (entity) onPositionUpdateRef.current(entity)
					}
				},
				onClose: (code) => {
					sendRef.current = null
					activeClose = null
					if (stopped) return
					if (code === DEMO_EXPIRED_CODE) {
						onDemoExpiredRef.current?.()
						return // no reconnect after demo expiry
					}
					reconnectTimer = setTimeout(() => {
						if (!stopped) connect()
					}, RECONNECT_DELAY_MS)
				},
			})

			activeClose = handle.close
		}

		connect()

		return () => {
			stopped = true
			sendRef.current = null
			if (reconnectTimer !== null) clearTimeout(reconnectTimer)
			activeClose?.()
		}
	}, []) // Intentionally empty: runs once, uses refs for callbacks.

	// Stable subscribe function. Sends a bbox subscription to the server.
	// Safe to call before the socket opens — silently dropped if not ready.
	const subscribe = useCallback((bbox: [number, number, number, number]) => {
		sendRef.current?.(JSON.stringify({ type: 'subscribe', bbox }))
	}, [])

	return { subscribe }
}
