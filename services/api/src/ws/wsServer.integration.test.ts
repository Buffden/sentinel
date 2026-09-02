// Integration tests for wsServer.ts: JWT-gated WebSocket upgrade, position
// bbox filtering, unfiltered alert fan-out, and demo-session lifecycle.
// Runs a real http.Server + real `ws` client + real Redis (docker-compose),
// not mocks — the guarantee under test is "an unauthenticated socket never
// completes the handshake" and "a client only sees what its subscription
// says it should," which live in real HTTP-Upgrade and Redis pub/sub
// behavior, not application code a mock could fake.
//
// Requires: `make up` (Redis), or the CI service container.
import { createServer, type Server } from 'node:http';
import type { AddressInfo } from 'node:net';
import { randomUUID } from 'node:crypto';
import jwt from 'jsonwebtoken';
import WebSocket from 'ws';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';
import { config } from '../config.js';
import { redis } from '../redis.js';
import { getDemoCount } from '../shared/demoSessions.js';
import { attachWebSocketServer } from './wsServer.js';

let httpServer: Server;
let wsUrl: string;
const openClients: WebSocket[] = [];

function signToken(
	payload: { user_id: string; email: string; role: 'operator' | 'demo' },
	options?: jwt.SignOptions,
): string {
	return jwt.sign(payload, config.JWT_SECRET, { expiresIn: '1h', ...options });
}

// Resolves on a completed WS handshake, rejects otherwise. A rejected upgrade
// can surface as either an HTTP 'unexpected-response' (server wrote a full
// response before closing) or a raw socket 'error' (destroyed mid-write) —
// both mean the same thing here: the handshake never completed.
function connect(token?: string): Promise<WebSocket> {
	return new Promise((resolve, reject) => {
		const headers = token ? { Cookie: `sentinel_jwt=${token}` } : {};
		const ws = new WebSocket(wsUrl, { headers });
		ws.once('open', () => {
			openClients.push(ws);
			resolve(ws);
		});
		ws.once('unexpected-response', (_req, res) => {
			reject(new Error(`handshake rejected: HTTP ${res.statusCode}`));
		});
		ws.once('error', (err) => reject(err));
	});
}

function waitForMessage(ws: WebSocket, timeoutMs = 3_000): Promise<Record<string, unknown>> {
	return new Promise((resolve, reject) => {
		const timer = setTimeout(() => reject(new Error('timed out waiting for message')), timeoutMs);
		ws.once('message', (data) => {
			clearTimeout(timer);
			resolve(JSON.parse(data.toString()) as Record<string, unknown>);
		});
	});
}

function assertNoMessage(ws: WebSocket, waitMs = 800): Promise<void> {
	return new Promise((resolve, reject) => {
		const handler = (): void => {
			clearTimeout(timer);
			reject(new Error('unexpected message received'));
		};
		ws.once('message', handler);
		const timer = setTimeout(() => {
			ws.off('message', handler);
			resolve();
		}, waitMs);
	});
}

function subscribe(ws: WebSocket, bbox: [number, number, number, number]): void {
	ws.send(JSON.stringify({ type: 'subscribe', bbox }));
}

describe('wsServer.ts (integration)', () => {
	beforeAll(async () => {
		httpServer = createServer();
		attachWebSocketServer(httpServer);
		await new Promise<void>((resolve) => httpServer.listen(0, resolve));
		const { port } = httpServer.address() as AddressInfo;
		wsUrl = `ws://localhost:${port}`;

		// The server's Redis subscriber calls redis.subscribe() asynchronously;
		// give it a moment to complete before any test publishes.
		await new Promise((r) => setTimeout(r, 300));
	}, 15_000);

	afterAll(async () => {
		await new Promise<void>((resolve) => httpServer.close(() => resolve()));
		await redis.quit();
	});

	afterEach(() => {
		while (openClients.length > 0) {
			const ws = openClients.pop();
			if (ws && ws.readyState === WebSocket.OPEN) ws.close();
		}
	});

	describe('upgrade authentication', () => {
		it('rejects a connection with no sentinel_jwt cookie', async () => {
			await expect(connect(undefined)).rejects.toBeDefined();
		});

		it('rejects a connection with a token signed by the wrong secret', async () => {
			const tampered = jwt.sign(
				{ user_id: 'u1', email: 'u1@example.com', role: 'operator' },
				'wrong-secret',
			);
			await expect(connect(tampered)).rejects.toBeDefined();
		});

		it('rejects a connection with an expired token', async () => {
			const expired = signToken(
				{ user_id: 'u1', email: 'u1@example.com', role: 'operator' },
				{ expiresIn: -1 },
			);
			await expect(connect(expired)).rejects.toBeDefined();
		});

		it('accepts a connection with a validly signed, unexpired token', async () => {
			const token = signToken({ user_id: 'u1', email: 'u1@example.com', role: 'operator' });
			const ws = await connect(token);
			expect(ws.readyState).toBe(WebSocket.OPEN);
		});
	});

	describe('position-updates bbox filtering', () => {
		it('delivers to a client whose subscribed bbox contains the point', async () => {
			const token = signToken({ user_id: 'u1', email: 'u1@example.com', role: 'operator' });
			const ws = await connect(token);
			subscribe(ws, [50, -1, 52, 1]); // contains (51.5, -0.1)
			await new Promise((r) => setTimeout(r, 100)); // let the subscribe message land

			const entityId = `test-ws-${randomUUID()}`;
			const pending = waitForMessage(ws);
			await redis.publish(
				config.POSITION_UPDATES_CHANNEL,
				JSON.stringify({ entity_id: entityId, lat: 51.5, lon: -0.1 }),
			);

			const received = await pending;
			expect(received['channel']).toBe(config.POSITION_UPDATES_CHANNEL);
			expect((received['data'] as { entity_id: string }).entity_id).toBe(entityId);
		});

		it('does not deliver to a client whose subscribed bbox excludes the point', async () => {
			const token = signToken({ user_id: 'u1', email: 'u1@example.com', role: 'operator' });
			const ws = await connect(token);
			subscribe(ws, [10, 10, 12, 12]); // does not contain (51.5, -0.1)
			await new Promise((r) => setTimeout(r, 100));

			const pending = assertNoMessage(ws);
			await redis.publish(
				config.POSITION_UPDATES_CHANNEL,
				JSON.stringify({ entity_id: `test-ws-${randomUUID()}`, lat: 51.5, lon: -0.1 }),
			);

			await expect(pending).resolves.toBeUndefined();
		});

		it('delivers to a client that has not sent a subscribe message yet (unfiltered by default)', async () => {
			const token = signToken({ user_id: 'u1', email: 'u1@example.com', role: 'operator' });
			const ws = await connect(token);
			// No subscribe() call — bbox stays null in connectionBBox.

			const entityId = `test-ws-${randomUUID()}`;
			const pending = waitForMessage(ws);
			await redis.publish(
				config.POSITION_UPDATES_CHANNEL,
				JSON.stringify({ entity_id: entityId, lat: 0, lon: 0 }),
			);

			const received = await pending;
			expect((received['data'] as { entity_id: string }).entity_id).toBe(entityId);
		});
	});

	describe('alert-events fan-out', () => {
		it('delivers to every connected client regardless of its position bbox', async () => {
			const tokenA = signToken({ user_id: 'a', email: 'a@example.com', role: 'operator' });
			const tokenB = signToken({ user_id: 'b', email: 'b@example.com', role: 'operator' });
			const wsA = await connect(tokenA);
			const wsB = await connect(tokenB);
			// Neither client's bbox would contain a plausible position — proves
			// alert-events bypasses the position filter entirely.
			subscribe(wsA, [10, 10, 12, 12]);
			subscribe(wsB, [20, 20, 22, 22]);
			await new Promise((r) => setTimeout(r, 100));

			const alertId = `test-alert-${randomUUID()}`;
			const pendingA = waitForMessage(wsA);
			const pendingB = waitForMessage(wsB);
			await redis.publish(config.ALERT_EVENTS_CHANNEL, JSON.stringify({ alert_id: alertId }));

			const [receivedA, receivedB] = await Promise.all([pendingA, pendingB]);
			expect(receivedA['channel']).toBe(config.ALERT_EVENTS_CHANNEL);
			expect((receivedA['data'] as { alert_id: string }).alert_id).toBe(alertId);
			expect((receivedB['data'] as { alert_id: string }).alert_id).toBe(alertId);
		});
	});

	describe('demo session lifecycle', () => {
		it('increments the demo count on connect and decrements it on close', async () => {
			const before = getDemoCount();
			const token = signToken(
				{ user_id: 'demo', email: 'demo', role: 'demo' },
				{ expiresIn: '1m' },
			);
			const ws = await connect(token);
			await new Promise((r) => setTimeout(r, 100)); // let the connection handler run

			expect(getDemoCount()).toBe(before + 1);

			ws.close();
			await new Promise((r) => setTimeout(r, 100));
			expect(getDemoCount()).toBe(before);
		});

		it('force-closes a demo connection once its JWT expires', async () => {
			const token = signToken(
				{ user_id: 'demo', email: 'demo', role: 'demo' },
				{ expiresIn: '1s' },
			);
			const ws = await connect(token);

			const closed = new Promise<number>((resolve) => {
				ws.once('close', (code) => resolve(code));
			});

			const code = await Promise.race([
				closed,
				new Promise<number>((_, reject) =>
					setTimeout(() => reject(new Error('did not close before timeout')), 3_000),
				),
			]);
			expect(code).toBe(4401);
		}, 5_000);
	});
});
