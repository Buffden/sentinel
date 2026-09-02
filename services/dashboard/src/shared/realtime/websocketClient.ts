// Thin WebSocket factory. No React, no reconnect — callers decide strategy.
// Returns a send function and a close function.

export interface WsHandle {
	send: (data: string) => void
	close: () => void
}

export function openWebSocket(
	url: string,
	handlers: {
		onOpen?: () => void
		onMessage: (data: string) => void
		onClose: (code: number) => void
		onError?: (err: Event) => void
	},
): WsHandle {
	const ws = new WebSocket(url)

	ws.addEventListener('open', () => handlers.onOpen?.())
	ws.addEventListener('message', (e) => handlers.onMessage(e.data as string))
	ws.addEventListener('close', (e) => handlers.onClose(e.code))
	ws.addEventListener('error', (e) => handlers.onError?.(e))

	return {
		send: (data) => {
			if (ws.readyState === WebSocket.OPEN) ws.send(data)
		},
		close: () => ws.close(),
	}
}
