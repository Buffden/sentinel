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
type ReconnectListener = () => void

const RECONNECT_DELAY_MS = 5_000
const DEMO_EXPIRED_CODE = 4401

let handle: WsHandle | null = null
let reconnectTimer: ReturnType<typeof setTimeout> | null = null
let refCount = 0
// True once this page has ever completed a connection. Distinguishes the
// very first open (each widget's own mount-time hydration already covers
// that) from a genuine reconnect after a drop (nothing else tells anyone
// that happened — see onReconnect below).
let hasConnectedBefore = false
let currentUrl: string | null = null

// Bumped by every connect() call. A close handler captures the generation
// its own connection was created with and only acts (clears `handle`,
// schedules an auto-reconnect) if that generation is still current.
// forceReconnect() bumps this by calling connect() BEFORE closing the old
// socket, so the old socket's close event -- which fires asynchronously,
// after forceReconnect has already returned -- always sees a stale
// generation and does nothing. Without this, an intentional reconnect and
// the old connection's own drop-recovery logic would race, potentially
// scheduling a second, redundant reconnect a few seconds later.
let generation = 0

const frameListeners = new Set<FrameListener>()
const demoExpiredListeners = new Set<DemoExpiredListener>()
const reconnectListeners = new Set<ReconnectListener>()

function connect(url: string): void {
	currentUrl = url
	const myGeneration = ++generation
	handle = openWebSocket(url, {
		onOpen: () => {
			if (myGeneration !== generation) return
			if (hasConnectedBefore) {
				reconnectListeners.forEach((fn) => fn())
			}
			hasConnectedBefore = true
		},
		onMessage: (raw) => {
			if (myGeneration !== generation) return
			let frame: LiveFrame
			try {
				frame = JSON.parse(raw) as LiveFrame
			} catch {
				return
			}
			frameListeners.forEach((fn) => fn(frame))
		},
		onClose: (code) => {
			// A stale generation means a newer connection has already taken
			// over (forceReconnect) -- this close is the old socket catching
			// up, not something to react to.
			if (myGeneration !== generation) return
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

// Closes the current connection and immediately opens a new one against the
// same URL, bypassing the normal drop-and-wait delay entirely. For a
// deliberate change (e.g. a saved workspace scope), not a recovery from an
// unexpected drop -- see the workspace-reconnect-flow concept doc for why
// those are different situations needing different handling. A no-op if no
// connection has ever been opened (nothing to reconnect).
export function forceReconnect(): void {
	if (currentUrl === null) return
	if (reconnectTimer !== null) {
		clearTimeout(reconnectTimer)
		reconnectTimer = null
	}
	const old = handle
	handle = null
	connect(currentUrl) // bumps generation first, so `old`'s eventual close is a no-op
	old?.close()
}

export function getWsUrl(): string {
	return process.env['NEXT_PUBLIC_WS_URL'] ?? 'ws://localhost:3000'
}

export interface LiveSocketSubscription {
	send: (data: string) => void
	onFrame: (fn: FrameListener) => () => void
	onDemoExpired: (fn: DemoExpiredListener) => () => void
	// Fires when the connection re-opens after having been open before —
	// never on the initial connect. Callers use this to re-run REST
	// hydration and re-send subscribe(bbox), recovering anything that
	// happened during the disconnected window.
	onReconnect: (fn: ReconnectListener) => () => void
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
		onReconnect: (fn) => {
			reconnectListeners.add(fn)
			return () => reconnectListeners.delete(fn)
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
