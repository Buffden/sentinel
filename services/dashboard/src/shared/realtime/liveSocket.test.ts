// liveSocket.ts holds module-level singleton state by design (see its own
// header comment), so each test re-imports it fresh via vi.resetModules() +
// dynamic import, and stubs the global WebSocket with a fake we drive by
// hand -- this is pure connection-lifecycle control flow, not a
// distributed-systems guarantee that needs a real socket.
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

class FakeWebSocket {
	static OPEN = 1
	static CLOSED = 3
	static instances: FakeWebSocket[] = []

	readyState = 0
	url: string
	private listeners: Record<string, Array<(event: unknown) => void>> = {}

	constructor(url: string) {
		this.url = url
		FakeWebSocket.instances.push(this)
	}

	addEventListener(type: string, fn: (event: unknown) => void): void {
		;(this.listeners[type] ??= []).push(fn)
	}

	send(): void {}

	close(): void {
		this.triggerClose(1000)
	}

	triggerOpen(): void {
		this.readyState = FakeWebSocket.OPEN
		this.listeners['open']?.forEach((fn) => fn({}))
	}

	triggerMessage(data: string): void {
		this.listeners['message']?.forEach((fn) => fn({ data }))
	}

	triggerClose(code: number): void {
		this.readyState = FakeWebSocket.CLOSED
		this.listeners['close']?.forEach((fn) => fn({ code }))
	}
}

beforeEach(() => {
	vi.resetModules()
	vi.useFakeTimers()
	FakeWebSocket.instances.length = 0
	vi.stubGlobal('WebSocket', FakeWebSocket as unknown as typeof WebSocket)
})

afterEach(() => {
	vi.useRealTimers()
	vi.unstubAllGlobals()
})

describe('forceReconnect', () => {
	it('opens a new connection immediately, without waiting for the reconnect delay', async () => {
		const { acquireLiveSocket, forceReconnect } = await import('./liveSocket')
		const sub = acquireLiveSocket('ws://test')
		FakeWebSocket.instances[0]!.triggerOpen()

		forceReconnect()

		expect(FakeWebSocket.instances).toHaveLength(2)
		sub.release()
	})

	it("the old connection's own close event does not schedule a duplicate reconnect", async () => {
		const { acquireLiveSocket, forceReconnect } = await import('./liveSocket')
		const sub = acquireLiveSocket('ws://test')
		const first = FakeWebSocket.instances[0]!
		first.triggerOpen()

		forceReconnect()
		// Simulate the browser firing the old socket's close event AFTER
		// forceReconnect already started the new one -- the real ordering.
		first.triggerClose(1000)

		// If the stale close's handler had scheduled an auto-reconnect, a
		// third instance would appear once the normal delay elapses.
		vi.advanceTimersByTime(10_000)
		expect(FakeWebSocket.instances).toHaveLength(2)

		sub.release()
	})

	it('fires onReconnect listeners once the forced reconnect opens', async () => {
		const { acquireLiveSocket, forceReconnect } = await import('./liveSocket')
		const sub = acquireLiveSocket('ws://test')
		FakeWebSocket.instances[0]!.triggerOpen()

		const onReconnect = vi.fn()
		sub.onReconnect(onReconnect)

		forceReconnect()
		FakeWebSocket.instances[1]!.triggerOpen()

		expect(onReconnect).toHaveBeenCalledTimes(1)
		sub.release()
	})

	it('is a no-op if no connection has ever been opened', async () => {
		const { forceReconnect } = await import('./liveSocket')
		expect(() => forceReconnect()).not.toThrow()
		expect(FakeWebSocket.instances).toHaveLength(0)
	})
})

describe('automatic reconnect on an unexpected drop (unchanged by this checkpoint)', () => {
	it('reconnects after the normal delay, not immediately', async () => {
		const { acquireLiveSocket } = await import('./liveSocket')
		const sub = acquireLiveSocket('ws://test')
		FakeWebSocket.instances[0]!.triggerOpen()

		FakeWebSocket.instances[0]!.triggerClose(1006) // abnormal closure
		expect(FakeWebSocket.instances).toHaveLength(1)

		vi.advanceTimersByTime(5_000)
		expect(FakeWebSocket.instances).toHaveLength(2)

		sub.release()
	})
})
