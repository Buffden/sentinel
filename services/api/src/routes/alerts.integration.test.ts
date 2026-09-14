// Integration test for GET /alerts: proves the status filter and ordering
// against a real Postgres query, not a mocked pool result, plus the CP2
// scope-filtering behavior (operator saved-scope filtering, demo ad-hoc
// bbox filtering) against real Postgres/Redis-backed data.
// Requires: `make up && make migrate` (locally) or the CI service containers.
import { randomUUID } from 'node:crypto';
import express from 'express';
import cookieParser from 'cookie-parser';
import type { Server } from 'node:http';
import type { AddressInfo } from 'node:net';
import jwt from 'jsonwebtoken';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';
import { pool } from '../db.js';
import { config } from '../config.js';
import { requireAuth } from '../middleware/auth.js';
import { alertsRouter } from './alerts.js';

let server: Server;
let baseUrl: string;

async function insertAlert(
	overrides: Partial<{
		status: string;
		detectedAt: Date;
		counterpartyEntityId: string;
		alertType: string;
		entityType: string;
		payload: Record<string, unknown>;
	}> = {},
): Promise<string> {
	const alertId = `test-alert-${randomUUID()}`;
	const detectedAt = overrides.detectedAt ?? new Date();
	await pool.query(
		`INSERT INTO alerts
			 (alert_id, entity_id, counterparty_entity_id, entity_type, alert_type, priority, status, payload, detected_at, updated_at)
		 VALUES ($1, 'test-entity', $2, $3, $4, 'STANDARD', $5, $6, $7, $7)`,
		[
			alertId,
			overrides.counterpartyEntityId ?? null,
			overrides.entityType ?? 'aircraft',
			overrides.alertType ?? 'SIGNAL_LOSS',
			overrides.status ?? 'NEW',
			JSON.stringify(overrides.payload ?? {}),
			detectedAt,
		],
	);
	return alertId;
}

describe('GET /alerts (integration)', () => {
	const seededIds: string[] = [];

	beforeAll(async () => {
		await pool.query('SELECT 1'); // fail fast with a clear error if Postgres is unreachable

		const app = express();
		app.use('/alerts', alertsRouter);
		server = app.listen(0);
		await new Promise<void>((resolve) => server.once('listening', resolve));
		const { port } = server.address() as AddressInfo;
		baseUrl = `http://localhost:${port}`;
	});

	afterAll(async () => {
		await new Promise<void>((resolve) => server.close(() => resolve()));
		// pool is a shared module-level singleton also used by the scope-filtering
		// describe block below in this same file -- ended there instead, once
		// both blocks are done with it.
	});

	afterEach(async () => {
		if (seededIds.length > 0) {
			await pool.query('DELETE FROM alerts WHERE alert_id = ANY($1)', [seededIds]);
			seededIds.length = 0;
		}
	});

	it('returns NEW and ACKNOWLEDGED alerts but excludes RESOLVED ones', async () => {
		const newId = await insertAlert({ status: 'NEW' });
		const ackId = await insertAlert({ status: 'ACKNOWLEDGED' });
		const resolvedId = await insertAlert({ status: 'RESOLVED' });
		seededIds.push(newId, ackId, resolvedId);

		const res = await fetch(`${baseUrl}/alerts`);
		expect(res.status).toBe(200);
		const body = (await res.json()) as Array<{ alert_id: string; status: string }>;
		const returnedIds = body.map((a) => a.alert_id);

		expect(returnedIds).toContain(newId);
		expect(returnedIds).toContain(ackId);
		expect(returnedIds).not.toContain(resolvedId);
	});

	it('includes counterparty_entity_id for a proximity-style alert', async () => {
		const id = await insertAlert({ counterpartyEntityId: 'test-counterparty' });
		seededIds.push(id);

		const res = await fetch(`${baseUrl}/alerts`);
		const body = (await res.json()) as Array<{
			alert_id: string;
			counterparty_entity_id: string | null;
		}>;
		const found = body.find((a) => a.alert_id === id);

		expect(found?.counterparty_entity_id).toBe('test-counterparty');
	});

	it('orders results by detected_at descending', async () => {
		const older = await insertAlert({ detectedAt: new Date(Date.now() - 60_000) });
		const newer = await insertAlert({ detectedAt: new Date() });
		seededIds.push(older, newer);

		const res = await fetch(`${baseUrl}/alerts`);
		const body = (await res.json()) as Array<{ alert_id: string }>;
		const ids = body.map((a) => a.alert_id);

		expect(ids.indexOf(newer)).toBeLessThan(ids.indexOf(older));
	});
});

// Separate authenticated app instance: requireAuth needs a real sentinel_jwt
// cookie, which the unauthenticated tests above deliberately don't send
// (proving the demo/no-role default stays unfiltered, unchanged from before
// CP2). These tests exercise the role-based branches CP2 actually added.
describe('GET /alerts scope filtering (integration)', () => {
	let authedServer: Server;
	let authedBaseUrl: string;
	const seededAlertIds: string[] = [];
	const seededUserIds: string[] = [];

	function signCookie(userId: string, role: 'operator' | 'demo'): string {
		const token = jwt.sign(
			{ user_id: userId, email: `${userId}@example.com`, role },
			config.JWT_SECRET,
			{ expiresIn: '1h' },
		);
		return `sentinel_jwt=${token}`;
	}

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

	beforeAll(async () => {
		const app = express();
		app.use(cookieParser());
		app.use(requireAuth);
		app.use('/alerts', alertsRouter);
		authedServer = app.listen(0);
		await new Promise<void>((resolve) => authedServer.once('listening', resolve));
		const { port } = authedServer.address() as AddressInfo;
		authedBaseUrl = `http://localhost:${port}`;
	});

	afterAll(async () => {
		await new Promise<void>((resolve) => authedServer.close(() => resolve()));
		await pool.end();
	});

	afterEach(async () => {
		if (seededAlertIds.length > 0) {
			await pool.query('DELETE FROM alerts WHERE alert_id = ANY($1)', [seededAlertIds]);
			seededAlertIds.length = 0;
		}
		if (seededUserIds.length > 0) {
			await pool.query('DELETE FROM user_workspaces WHERE user_id = ANY($1)', [seededUserIds]);
			await pool.query('DELETE FROM users WHERE user_id = ANY($1)', [seededUserIds]);
			seededUserIds.length = 0;
		}
	});

	it('returns an empty list for an operator with no saved workspace', async () => {
		const userId = randomUUID();
		await insertUserWithoutWorkspace(userId);
		seededUserIds.push(userId);
		const inFrance = await insertAlert({ payload: { last_known_lat: 45, last_known_lon: 2 } });
		seededAlertIds.push(inFrance);

		const res = await fetch(`${authedBaseUrl}/alerts`, {
			headers: { Cookie: signCookie(userId, 'operator') },
		});
		expect(res.status).toBe(200);
		expect(await res.json()).toEqual([]);
	});

	it("returns only alerts inside an operator's saved bounds and alert_types", async () => {
		const userId = randomUUID();
		await insertUserWithWorkspace(userId, {
			geo_region: {
				name: 'France',
				bounds: { min_lat: 41.3, max_lat: 51.1, min_lon: -5.2, max_lon: 9.6 },
			},
			entity_types: ['aircraft'],
			alert_types: ['SIGNAL_LOSS'],
		});
		seededUserIds.push(userId);

		const inFrance = await insertAlert({ payload: { last_known_lat: 45, last_known_lon: 2 } });
		const inNyc = await insertAlert({ payload: { last_known_lat: 40.7, last_known_lon: -74.0 } });
		const wrongType = await insertAlert({
			alertType: 'UNSCHEDULED_PROXIMITY',
			payload: { lat: 45, lon: 2 },
		});
		seededAlertIds.push(inFrance, inNyc, wrongType);

		const res = await fetch(`${authedBaseUrl}/alerts`, {
			headers: { Cookie: signCookie(userId, 'operator') },
		});
		expect(res.status).toBe(200);
		const body = (await res.json()) as Array<{ alert_id: string }>;
		const ids = body.map((a) => a.alert_id);
		expect(ids).toContain(inFrance);
		expect(ids).not.toContain(inNyc);
		expect(ids).not.toContain(wrongType);
	});

	it('filters a demo session to an ad-hoc bbox, geography only', async () => {
		const inFrance = await insertAlert({ payload: { last_known_lat: 45, last_known_lon: 2 } });
		const inNyc = await insertAlert({ payload: { last_known_lat: 40.7, last_known_lon: -74.0 } });
		seededAlertIds.push(inFrance, inNyc);

		const res = await fetch(`${authedBaseUrl}/alerts?bbox=41.3,-5.2,51.1,9.6`, {
			headers: { Cookie: signCookie('demo', 'demo') },
		});
		expect(res.status).toBe(200);
		const body = (await res.json()) as Array<{ alert_id: string }>;
		const ids = body.map((a) => a.alert_id);
		expect(ids).toContain(inFrance);
		expect(ids).not.toContain(inNyc);
	});

	it('returns every alert unfiltered for a demo session with no bbox', async () => {
		const inFrance = await insertAlert({ payload: { last_known_lat: 45, last_known_lon: 2 } });
		const inNyc = await insertAlert({ payload: { last_known_lat: 40.7, last_known_lon: -74.0 } });
		seededAlertIds.push(inFrance, inNyc);

		const res = await fetch(`${authedBaseUrl}/alerts`, {
			headers: { Cookie: signCookie('demo', 'demo') },
		});
		expect(res.status).toBe(200);
		const body = (await res.json()) as Array<{ alert_id: string }>;
		const ids = body.map((a) => a.alert_id);
		expect(ids).toContain(inFrance);
		expect(ids).toContain(inNyc);
	});

	it('rejects a malformed bbox for a demo session', async () => {
		const res = await fetch(`${authedBaseUrl}/alerts?bbox=not,valid`, {
			headers: { Cookie: signCookie('demo', 'demo') },
		});
		expect(res.status).toBe(400);
	});
});
