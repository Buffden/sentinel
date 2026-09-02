'use client'

// Page-level singleton WebSocket connection, reference-counted across every
// caller. One WebSocket per page — two connections would double
// position-update (and now alert-event) delivery. MapWidget and AlertWidget
// are independently-mounted Dockview panels with no parent/child relationship
// to prop-drill a shared connection through, so ownership lives here instead:
// the connection opens on the first subscriber and closes on the last one's
// unmount, not tied to any single component's lifecycle.
//
// Owns the whole connection lifecycle (connect, 5 s reconnect on unexpected
// close, no reconnect on demo-expiry) so subscribers never race each other
// trying to reconnect independently.

import { openWebSocket, type WsHandle } from './websocketClient'

export interface LiveFrame {
	channel: string
	data: unknown
}

type FrameListener = (frame: LiveFrame) => void
type DemoExpiredListener = () => void

const RECONNECT_DELAY_MS = 5_000
const DEMO_EXPIRED_CODE = 4401

let handle: WsHandle | null = null
let reconnectTimer: ReturnType<typeof setTimeout> | null = null
let refCount = 0

const frameListeners = new Set<FrameListener>()
const demoExpiredListeners = new Set<DemoExpiredListener>()

function connect(url: string): void {
	handle = openWebSocket(url, {
		onMessage: (raw) => {
			let frame: LiveFrame
			try {
				frame = JSON.parse(raw) as LiveFrame
			} catch {
				return
			}
			frameListeners.forEach((fn) => fn(frame))
		},
		onClose: (code) => {
			handle = null
			if (code === DEMO_EXPIRED_CODE) {
				demoExpiredListeners.forEach((fn) => fn())
				return // no reconnect after demo expiry
			}
			// Only reconnect while someone still wants the connection —
			// a release() during the gap before this fires must win.
			if (refCount > 0) {
				reconnectTimer = setTimeout(() => connect(url), RECONNECT_DELAY_MS)
			}
		},
	})
}

export interface LiveSocketSubscription {
	send: (data: string) => void
	onFrame: (fn: FrameListener) => () => void
	onDemoExpired: (fn: DemoExpiredListener) => () => void
	release: () => void
}

export function acquireLiveSocket(url: string): LiveSocketSubscription {
	refCount++
	if (!handle && !reconnectTimer) connect(url)

	return {
		send: (data) => handle?.send(data),
		onFrame: (fn) => {
			frameListeners.add(fn)
			return () => frameListeners.delete(fn)
		},
		onDemoExpired: (fn) => {
			demoExpiredListeners.add(fn)
			return () => demoExpiredListeners.delete(fn)
		},
		release: () => {
			refCount--
			if (refCount > 0) return
			if (reconnectTimer !== null) {
				clearTimeout(reconnectTimer)
				reconnectTimer = null
			}
			handle?.close()
			handle = null
		},
	}
}
