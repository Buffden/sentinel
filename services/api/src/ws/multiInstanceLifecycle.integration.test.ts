// Integration tests for Phase 08 CP4: cross-instance convergence and the
// crash-after-persist-before-publish boundary. Two independent
// http.Server + attachWebSocketServer pairs stand in for two real API
// instances -- each gets its own in-memory connection map and its own
// Redis subscriber connection (attachWebSocketServer creates a fresh one
// per call), the same isolation two real processes would have. Both share
// the same Postgres and Redis the real deployment would share; nothing
// links the two instances directly.
//
// Requires: `make up && make migrate` (locally) or the CI service containers.
import { createServer, type Server } from 'node:http';
import type { AddressInfo } from 'node:net';
import { randomUUID } from 'node:crypto';
import express, { type ErrorRequestHandler } from 'express';
import cookieParser from 'cookie-parser';
import jwt from 'jsonwebtoken';
import WebSocket from 'ws';
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from 'vitest';
import { config } from '../config.js';
import { redis } from '../redis.js';
import { pool } from '../db.js';
import { requireAuth } from '../middleware/auth.js';
import { alertsRouter } from '../routes/alerts.js';
import { attachWebSocketServer } from './wsServer.js';

// Mirrors index.ts's own error-handling middleware. Without it, an
// unhandled rejection in an async route handler -- exactly what the crash
// test below deliberately triggers -- would leave the request hanging
// instead of returning a clean response, the same gap CP2 found and fixed
// in the real server.
const handleUnhandledRouteError: ErrorRequestHandler = (err, _req, res, next) => {
	if (res.headersSent) {
		next(err);
		return;
	}
	res.status(500).json({ error: 'internal error' });
};

interface Instance {
	server: Server;
	baseUrl: string;
}

async function buildInstance(): Promise<Instance> {
	const app = express();
	app.use(express.json());
	app.use(cookieParser());
	app.use(requireAuth);
	app.use('/alerts', alertsRouter);
	app.use(handleUnhandledRouteError);
	const server = createServer(app);
	attachWebSocketServer(server);
	await new Promise<void>((resolve) => server.listen(0, resolve));
	const { port } = server.address() as AddressInfo;
	return { server, baseUrl: `http://localhost:${port}` };
}

function signOperatorCookie(userId: string): string {
	const token = jwt.sign(
		{ user_id: userId, email: `${userId}@example.com`, role: 'operator' },
		config.JWT_SECRET,
		{ expiresIn: '1h' },
	);
	return `sentinel_jwt=${token}`;
}

function connectDemo(baseUrl: string, bbox: [number, number, number, number]): Promise<WebSocket> {
	return new Promise((resolve, reject) => {
		const token = jwt.sign({ user_id: 'demo', email: 'demo', role: 'demo' }, config.JWT_SECRET, {
			expiresIn: '1h',
		});
		const ws = new WebSocket(baseUrl.replace('http://', 'ws://'), {
			headers: { Cookie: `sentinel_jwt=${token}` },
		});
		ws.once('open', () => {
			ws.send(JSON.stringify({ type: 'subscribe', bbox }));
			resolve(ws);
		});
		ws.once('error', reject);
	});
}

function waitForAlertMessage(
	ws: WebSocket,
	alertId: string,
	timeoutMs = 5_000,
): Promise<Record<string, unknown>> {
	return new Promise((resolve, reject) => {
		const timer = setTimeout(
			() => reject(new Error('timed out waiting for alert message')),
			timeoutMs,
		);
		const handler = (data: WebSocket.RawData): void => {
			const frame = JSON.parse(data.toString()) as {
				channel?: string;
				data?: Record<string, unknown>;
			};
			if (frame.channel === config.ALERT_EVENTS_CHANNEL && frame.data?.['alert_id'] === alertId) {
				clearTimeout(timer);
				ws.off('message', handler);
				resolve(frame.data);
			}
		};
		ws.on('message', handler);
	});
}

async function seedUser(userId: string): Promise<void> {
	await pool.query(
		`INSERT INTO users (user_id, google_sub, email, last_login_at, created_at)
		 VALUES ($1, $2, $3, now(), now())`,
		[userId, `google-${userId}`, `${userId}@example.com`],
	);
}

// last_known_lat/lon are required for extractAlertPosition to place this
// SIGNAL_LOSS alert inside a bbox filter -- see alertScopeFilter.ts.
async function seedAlert(alertId: string): Promise<void> {
	await pool.query(
		`INSERT INTO alerts
		   (alert_id, entity_id, counterparty_entity_id, entity_type, alert_type, priority, status, payload, detected_at, updated_at)
		 VALUES ($1, 'test-multi-entity', NULL, 'aircraft', 'SIGNAL_LOSS', 'STANDARD', 'NEW', $2, now(), now())`,
		[
			alertId,
			JSON.stringify({ dark_since_ms: 1_700_000_000_000, last_known_lat: 45, last_known_lon: 2 }),
		],
	);
}

const BBOX_COVERING_ALERT: [number, number, number, number] = [40, -5, 51, 10]; // contains (45, 2)

describe('cross-instance alert lifecycle convergence (integration)', () => {
	let instanceA: Instance;
	let instanceB: Instance;
	const openClients: WebSocket[] = [];
	const seededAlertIds: string[] = [];
	const seededUserIds: string[] = [];

	beforeAll(async () => {
		instanceA = await buildInstance();
		instanceB = await buildInstance();
		// Each instance's Redis subscriber calls redis.subscribe() asynchronously;
		// give both a moment to complete before any test publishes.
		await new Promise((r) => setTimeout(r, 300));
	}, 15_000);

	afterAll(async () => {
		await new Promise<void>((resolve) => instanceA.server.close(() => resolve()));
		await new Promise<void>((resolve) => instanceB.server.close(() => resolve()));
		await pool.end();
	});

	afterEach(async () => {
		while (openClients.length > 0) {
			const ws = openClients.pop();
			if (ws && ws.readyState === WebSocket.OPEN) ws.close();
		}
		await new Promise((r) => setTimeout(r, 100));
		if (seededAlertIds.length > 0) {
			await pool.query('DELETE FROM alerts WHERE alert_id = ANY($1)', [seededAlertIds]);
			seededAlertIds.length = 0;
		}
		if (seededUserIds.length > 0) {
			await pool.query('DELETE FROM users WHERE user_id = ANY($1)', [seededUserIds]);
			seededUserIds.length = 0;
		}
	});

	it('an ACK through instance A converges on a client connected only to instance B', async () => {
		const userId = randomUUID();
		const alertId = `test-multi-${randomUUID()}`;
		await seedUser(userId);
		await seedAlert(alertId);
		seededUserIds.push(userId);
		seededAlertIds.push(alertId);

		const clientOnA = await connectDemo(instanceA.baseUrl, BBOX_COVERING_ALERT);
		const clientOnB = await connectDemo(instanceB.baseUrl, BBOX_COVERING_ALERT);
		openClients.push(clientOnA, clientOnB);
		await new Promise((r) => setTimeout(r, 150)); // let both subscribe messages land

		const pendingA = waitForAlertMessage(clientOnA, alertId);
		const pendingB = waitForAlertMessage(clientOnB, alertId);

		// The DB write and the publish both happen on instance A only.
		const res = await fetch(`${instanceA.baseUrl}/alerts/${alertId}`, {
			method: 'PATCH',
			headers: { 'Content-Type': 'application/json', Cookie: signOperatorCookie(userId) },
			body: JSON.stringify({ status: 'ACKNOWLEDGED' }),
		});
		expect(res.status).toBe(200);

		// Both clients converge -- including clientOnB, which is attached to an
		// instance that never touched Postgres for this request. Only Redis
		// pub/sub carried this from A to B.
		const [receivedByA, receivedByB] = await Promise.all([pendingA, pendingB]);
		expect(receivedByA['status']).toBe('ACKNOWLEDGED');
		expect(receivedByB['status']).toBe('ACKNOWLEDGED');
	}, 15_000);

	it('a publish failure after the DB commit leaves the transition durable; a client retry republishes it with no duplicate write', async () => {
		const userId = randomUUID();
		const alertId = `test-multi-crash-${randomUUID()}`;
		await seedUser(userId);
		await seedAlert(alertId);
		seededUserIds.push(userId);
		seededAlertIds.push(alertId);

		const client = await connectDemo(instanceB.baseUrl, BBOX_COVERING_ALERT);
		openClients.push(client);
		await new Promise((r) => setTimeout(r, 150));

		// Simulate a crash between the transaction commit and the publish: the
		// very next redis.publish call throws, exactly once.
		let threw = false;
		const publishSpy = vi
			.spyOn(redis, 'publish')
			.mockImplementationOnce(async (_channel: string | Buffer, _message: string | Buffer) => {
				threw = true;
				throw new Error('simulated crash before publish');
			});

		try {
			const failingRes = await fetch(`${instanceA.baseUrl}/alerts/${alertId}`, {
				method: 'PATCH',
				headers: { 'Content-Type': 'application/json', Cookie: signOperatorCookie(userId) },
				body: JSON.stringify({ status: 'ACKNOWLEDGED' }),
			});
			expect(threw).toBe(true);
			expect(failingRes.status).toBe(500); // clean failure, not a hang

			// The DB transition is already durable even though the publish (and
			// therefore the HTTP response) failed.
			const { rows: afterFailedPublish } = await pool.query(
				'SELECT status, acknowledged_at, acknowledged_by FROM alerts WHERE alert_id = $1',
				[alertId],
			);
			expect(afterFailedPublish[0].status).toBe('ACKNOWLEDGED');
			expect(afterFailedPublish[0].acknowledged_by).toBe(userId);
			const firstAcknowledgedAt = (afterFailedPublish[0].acknowledged_at as Date).getTime();

			publishSpy.mockRestore();

			// The client never got a push for the failed attempt. Simulate the
			// client's own retry -- the same idempotent-replay path CP2 proved.
			const pending = waitForAlertMessage(client, alertId);
			const retryRes = await fetch(`${instanceA.baseUrl}/alerts/${alertId}`, {
				method: 'PATCH',
				headers: { 'Content-Type': 'application/json', Cookie: signOperatorCookie(userId) },
				body: JSON.stringify({ status: 'ACKNOWLEDGED' }),
			});
			expect(retryRes.status).toBe(200);

			const received = await pending;
			expect(received['status']).toBe('ACKNOWLEDGED');

			// No duplicate transition: acknowledged_at is unchanged by the retry.
			const { rows: afterRetry } = await pool.query(
				'SELECT status, acknowledged_at, acknowledged_by FROM alerts WHERE alert_id = $1',
				[alertId],
			);
			expect((afterRetry[0].acknowledged_at as Date).getTime()).toBe(firstAcknowledgedAt);
			expect(afterRetry[0].acknowledged_by).toBe(userId);
		} finally {
			publishSpy.mockRestore();
		}
	}, 15_000);
});
