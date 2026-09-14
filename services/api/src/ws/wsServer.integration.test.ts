// Integration tests for wsServer.ts: JWT-gated WebSocket upgrade, position
// bbox filtering, scope-filtered alert fan-out, and demo-session lifecycle.
// Runs a real http.Server + real `ws` client + real Redis + real Postgres
// (docker-compose), not mocks — the guarantee under test is "an
// unauthenticated socket never completes the handshake" and "a client only
// sees what its scope says it should," which live in real HTTP-Upgrade,
// Redis pub/sub, and Postgres-backed workspace lookups, not application
// code a mock could fake.
//
// Requires: `make up && make migrate` (locally) or the CI service containers.
import { createServer, type Server } from 'node:http';
import type { AddressInfo } from 'node:net';
import { randomUUID } from 'node:crypto';
import jwt from 'jsonwebtoken';
import WebSocket from 'ws';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';
import { config } from '../config.js';
import { redis } from '../redis.js';
import { pool } from '../db.js';
import { getDemoCount } from '../shared/demoSessions.js';
import { attachWebSocketServer } from './wsServer.js';

let httpServer: Server;
let wsUrl: string;
const openClients: WebSocket[] = [];
const seededUserIds: string[] = [];

async function insertUserWithWorkspace(
	userId: string,
	scope: {
		geo_region: { name: string | null; bounds: Record<string, number> };
		entity_types: string[];
		alert_types: string[];
	},
): Promise<void> {
	await pool.query(
		`INSERT INTO users (user_id, google_sub, email, last_login_at, created_at)
		 VALUES ($1, $2, $3, now(), now())`,
		[userId, `google-${userId}`, `${userId}@example.com`],
	);
	await pool.query(
		`INSERT INTO user_workspaces (user_id, scope, updated_at) VALUES ($1, $2, now())`,
		[userId, JSON.stringify(scope)],
	);
}

async function insertUserWithoutWorkspace(userId: string): Promise<void> {
	await pool.query(
		`INSERT INTO users (user_id, google_sub, email, last_login_at, created_at)
		 VALUES ($1, $2, $3, now(), now())`,
		[userId, `google-${userId}`, `${userId}@example.com`],
	);
}

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
		await pool.end();
	});

	afterEach(async () => {
		while (openClients.length > 0) {
			const ws = openClients.pop();
			if (ws && ws.readyState === WebSocket.OPEN) ws.close();
		}
		// ws.close() only starts the close handshake; the server's own 'close'
		// handler (which decrements demoCount and drops connection state) runs
		// asynchronously afterward. Without this wait, a test that opens a demo
		// connection can leave a not-yet-processed close pending when the next
		// test captures its "before" baseline off the shared demoCount counter.
		await new Promise((r) => setTimeout(r, 100));
		if (seededUserIds.length > 0) {
			await pool.query('DELETE FROM user_workspaces WHERE user_id = ANY($1)', [seededUserIds]);
			await pool.query('DELETE FROM users WHERE user_id = ANY($1)', [seededUserIds]);
			seededUserIds.length = 0;
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

	describe('alert-events fan-out (scope-filtered)', () => {
		function proximityAlert(overrides: {
			alert_id: string;
			lat: number;
			lon: number;
			entity_type?: string;
		}): Record<string, unknown> {
			return {
				alert_id: overrides.alert_id,
				entity_id: 'test-entity',
				entity_type: overrides.entity_type ?? 'aircraft',
				alert_type: 'UNSCHEDULED_PROXIMITY',
				payload: { lat: overrides.lat, lon: overrides.lon },
			};
		}

		it('delivers nothing to an operator with no saved workspace', async () => {
			const userId = randomUUID();
			await insertUserWithoutWorkspace(userId);
			seededUserIds.push(userId);

			const token = signToken({
				user_id: userId,
				email: `${userId}@example.com`,
				role: 'operator',
			});
			const ws = await connect(token);
			await new Promise((r) => setTimeout(r, 150)); // let the async scope load resolve (to "none")

			const pending = assertNoMessage(ws);
			await redis.publish(
				config.ALERT_EVENTS_CHANNEL,
				JSON.stringify(proximityAlert({ alert_id: `test-alert-${randomUUID()}`, lat: 45, lon: 2 })),
			);
			await expect(pending).resolves.toBeUndefined();
		});

		it("delivers an alert inside an operator's saved scope, and withholds one outside it", async () => {
			const userId = randomUUID();
			await insertUserWithWorkspace(userId, {
				geo_region: {
					name: 'France',
					bounds: { min_lat: 41.3, max_lat: 51.1, min_lon: -5.2, max_lon: 9.6 },
				},
				entity_types: ['aircraft'],
				alert_types: ['UNSCHEDULED_PROXIMITY'],
			});
			seededUserIds.push(userId);

			const token = signToken({
				user_id: userId,
				email: `${userId}@example.com`,
				role: 'operator',
			});
			const ws = await connect(token);
			await new Promise((r) => setTimeout(r, 150)); // let the async scope load resolve

			const inFranceId = `test-alert-${randomUUID()}`;
			const pending = waitForMessage(ws);
			await redis.publish(
				config.ALERT_EVENTS_CHANNEL,
				JSON.stringify(proximityAlert({ alert_id: inFranceId, lat: 45, lon: 2 })),
			);
			const received = await pending;
			expect((received['data'] as { alert_id: string }).alert_id).toBe(inFranceId);

			const outsideFrancePending = assertNoMessage(ws);
			await redis.publish(
				config.ALERT_EVENTS_CHANNEL,
				JSON.stringify(
					proximityAlert({ alert_id: `test-alert-${randomUUID()}`, lat: 40.7, lon: -74.0 }),
				),
			);
			await expect(outsideFrancePending).resolves.toBeUndefined();
		});

		it('filters a demo connection to its subscribed bbox, geography only', async () => {
			const token = signToken({ user_id: 'demo', email: 'demo', role: 'demo' });
			const ws = await connect(token);
			subscribe(ws, [41.3, -5.2, 51.1, 9.6]); // France
			await new Promise((r) => setTimeout(r, 100));

			const inBboxId = `test-alert-${randomUUID()}`;
			const pending = waitForMessage(ws);
			await redis.publish(
				config.ALERT_EVENTS_CHANNEL,
				JSON.stringify(proximityAlert({ alert_id: inBboxId, lat: 45, lon: 2 })),
			);
			const received = await pending;
			expect((received['data'] as { alert_id: string }).alert_id).toBe(inBboxId);

			const outsideBboxPending = assertNoMessage(ws);
			await redis.publish(
				config.ALERT_EVENTS_CHANNEL,
				JSON.stringify(
					proximityAlert({ alert_id: `test-alert-${randomUUID()}`, lat: 40.7, lon: -74.0 }),
				),
			);
			await expect(outsideBboxPending).resolves.toBeUndefined();
		});

		it('delivers unfiltered to a demo connection with no subscribed bbox yet', async () => {
			const token = signToken({ user_id: 'demo', email: 'demo', role: 'demo' });
			const ws = await connect(token);
			// No subscribe() call.

			const alertId = `test-alert-${randomUUID()}`;
			const pending = waitForMessage(ws);
			await redis.publish(
				config.ALERT_EVENTS_CHANNEL,
				JSON.stringify(proximityAlert({ alert_id: alertId, lat: 45, lon: 2 })),
			);
			const received = await pending;
			expect((received['data'] as { alert_id: string }).alert_id).toBe(alertId);
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
