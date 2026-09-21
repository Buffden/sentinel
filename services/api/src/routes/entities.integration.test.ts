// Integration test for GET /entities (Phase 09 CP1): proves workspace-scope
// filtering (operator saved-scope, fail-closed with no saved workspace, demo
// ad-hoc bbox / unfiltered fallback) against real Postgres + Redis state, not
// a mocked scan or scope lookup.
// Requires: `make up && make migrate` (locally) or the CI service containers.
import { randomUUID } from 'node:crypto';
import express from 'express';
import cookieParser from 'cookie-parser';
import type { Server } from 'node:http';
import type { AddressInfo } from 'node:net';
import jwt from 'jsonwebtoken';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';
import { pool } from '../db.js';
import { redis } from '../redis.js';
import { config } from '../config.js';
import { requireAuth } from '../middleware/auth.js';
import { entitiesRouter } from './entities.js';
import { entitiesLiveRouter } from './entitiesLive.js';

let server: Server;
let baseUrl: string;

interface LiveEntity {
	entity_id: string;
	lat: number;
	lon: number;
}

function signCookie(userId: string, role: 'operator' | 'demo'): string {
	const token = jwt.sign(
		{ user_id: userId, email: `${userId}@example.com`, role },
		config.JWT_SECRET,
		{ expiresIn: '1h' },
	);
	return `sentinel_jwt=${token}`;
}

function seedEntity(entityId: string, lat: number, lon: number): Promise<number> {
	return redis.hset(
		`entity:live:${entityId}`,
		'lat',
		String(lat),
		'lon',
		String(lon),
		'last_seen_ms',
		String(Date.now()),
		'entity_type',
		'aircraft',
	);
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

async function insertAlert(
	entityId: string,
	overrides: Partial<{
		counterpartyEntityId: string;
		alertType: string;
		entityType: string;
		payload: Record<string, unknown>;
	}> = {},
): Promise<string> {
	const alertId = `test-alert-${randomUUID()}`;
	await pool.query(
		`INSERT INTO alerts
			 (alert_id, entity_id, counterparty_entity_id, entity_type, alert_type, priority, status, payload, detected_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, 'STANDARD', 'NEW', $6, now(), now())`,
		[
			alertId,
			entityId,
			overrides.counterpartyEntityId ?? null,
			overrides.entityType ?? 'aircraft',
			overrides.alertType ?? 'SIGNAL_LOSS',
			JSON.stringify(overrides.payload ?? {}),
		],
	);
	return alertId;
}

describe('GET /entities (integration)', () => {
	const seededEntityIds: string[] = [];
	const seededUserIds: string[] = [];

	beforeAll(async () => {
		await pool.query('SELECT 1'); // fail fast with a clear error if Postgres is unreachable
		await redis.ping(); // fail fast with a clear error if Redis is unreachable

		const app = express();
		app.use(cookieParser());
		app.use(requireAuth);
		app.use('/entities', entitiesRouter);
		server = app.listen(0);
		await new Promise<void>((resolve) => server.once('listening', resolve));
		const { port } = server.address() as AddressInfo;
		baseUrl = `http://localhost:${port}`;
	});

	afterAll(async () => {
		await new Promise<void>((resolve) => server.close(() => resolve()));
		// pool/redis are shared module-level singletons also used by the two
		// describe blocks below in this same file -- ended in the last one
		// instead, once every block is done with them (same convention
		// alerts.integration.test.ts already uses).
	});

	afterEach(async () => {
		if (seededEntityIds.length > 0) {
			await redis.del(...seededEntityIds.map((id) => `entity:live:${id}`));
			seededEntityIds.length = 0;
		}
		if (seededUserIds.length > 0) {
			await pool.query('DELETE FROM user_workspaces WHERE user_id = ANY($1)', [seededUserIds]);
			await pool.query('DELETE FROM users WHERE user_id = ANY($1)', [seededUserIds]);
			seededUserIds.length = 0;
		}
	});

	it('returns 401 with no auth cookie', async () => {
		const res = await fetch(`${baseUrl}/entities`);
		expect(res.status).toBe(401);
	});

	it('returns an empty list for an operator with no saved workspace', async () => {
		const userId = randomUUID();
		await insertUserWithoutWorkspace(userId);
		seededUserIds.push(userId);
		const inFrance = `test-entity-${randomUUID()}`;
		seededEntityIds.push(inFrance);
		await seedEntity(inFrance, 45, 2);

		const res = await fetch(`${baseUrl}/entities`, {
			headers: { Cookie: signCookie(userId, 'operator') },
		});
		expect(res.status).toBe(200);
		expect(await res.json()).toEqual([]);
	});

	it("returns only entities inside an operator's saved bounds and entity_types", async () => {
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

		const inside = `test-entity-${randomUUID()}`;
		const outside = `test-entity-${randomUUID()}`;
		seededEntityIds.push(inside, outside);
		await seedEntity(inside, 45, 2); // Paris-ish, inside France bounds
		await seedEntity(outside, 40.7, -74); // New York, outside France bounds

		const res = await fetch(`${baseUrl}/entities`, {
			headers: { Cookie: signCookie(userId, 'operator') },
		});
		expect(res.status).toBe(200);
		const body = (await res.json()) as LiveEntity[];
		const ids = body.map((e) => e.entity_id);
		expect(ids).toContain(inside);
		expect(ids).not.toContain(outside);
	});

	it('excludes an entity whose type is not in the saved entity_types', async () => {
		const userId = randomUUID();
		await insertUserWithWorkspace(userId, {
			geo_region: {
				name: 'Global',
				bounds: { min_lat: -90, max_lat: 90, min_lon: -180, max_lon: 180 },
			},
			entity_types: ['vessel'],
			alert_types: ['SIGNAL_LOSS'],
		});
		seededUserIds.push(userId);

		const aircraftId = `test-entity-${randomUUID()}`;
		seededEntityIds.push(aircraftId);
		await seedEntity(aircraftId, 0, 0); // seedEntity always writes entity_type=aircraft

		const res = await fetch(`${baseUrl}/entities`, {
			headers: { Cookie: signCookie(userId, 'operator') },
		});
		const body = (await res.json()) as LiveEntity[];
		expect(body.map((e) => e.entity_id)).not.toContain(aircraftId);
	});

	it('filters a demo session by an ad-hoc bbox, unrestricted by entity type', async () => {
		const inside = `test-entity-${randomUUID()}`;
		const outside = `test-entity-${randomUUID()}`;
		seededEntityIds.push(inside, outside);
		await seedEntity(inside, 51.5, -0.1);
		await seedEntity(outside, 10, 10);

		const res = await fetch(`${baseUrl}/entities?bbox=50,-1,52,1`, {
			headers: { Cookie: signCookie('demo', 'demo') },
		});
		expect(res.status).toBe(200);
		const body = (await res.json()) as LiveEntity[];
		const ids = body.map((e) => e.entity_id);
		expect(ids).toContain(inside);
		expect(ids).not.toContain(outside);
	});

	it('returns the fully unfiltered list for a demo session with no bbox', async () => {
		const anywhereId = `test-entity-${randomUUID()}`;
		seededEntityIds.push(anywhereId);
		await seedEntity(anywhereId, -33.9, 151.2); // Sydney -- nowhere near a "typical" scope

		const res = await fetch(`${baseUrl}/entities`, {
			headers: { Cookie: signCookie('demo', 'demo') },
		});
		expect(res.status).toBe(200);
		const body = (await res.json()) as LiveEntity[];
		expect(body.map((e) => e.entity_id)).toContain(anywhereId);
	});

	it('rejects a malformed demo bbox', async () => {
		const res = await fetch(`${baseUrl}/entities?bbox=1,2,3`, {
			headers: { Cookie: signCookie('demo', 'demo') },
		});
		expect(res.status).toBe(400);
	});
});

// Phase 09 CP2: GET /entities/:entity_id -- live state + recent alerts join.
describe('GET /entities/:entity_id (integration)', () => {
	let authedServer: Server;
	let authedBaseUrl: string;
	const seededEntityIds: string[] = [];
	const seededAlertIds: string[] = [];
	const seededUserIds: string[] = [];

	beforeAll(async () => {
		const app = express();
		app.use(cookieParser());
		app.use(requireAuth);
		app.use('/entities', entitiesRouter);
		authedServer = app.listen(0);
		await new Promise<void>((resolve) => authedServer.once('listening', resolve));
		const { port } = authedServer.address() as AddressInfo;
		authedBaseUrl = `http://localhost:${port}`;
	});

	afterAll(async () => {
		await new Promise<void>((resolve) => authedServer.close(() => resolve()));
	});

	afterEach(async () => {
		if (seededEntityIds.length > 0) {
			await redis.del(...seededEntityIds.map((id) => `entity:live:${id}`));
			seededEntityIds.length = 0;
		}
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

	it('returns 404 for an id with no live state and no alerts', async () => {
		const res = await fetch(`${authedBaseUrl}/entities/no-such-entity`, {
			headers: { Cookie: signCookie('demo', 'demo') },
		});
		expect(res.status).toBe(404);
	});

	it('returns live state plus alerts where the entity is primary or counterparty', async () => {
		const entityId = `test-entity-${randomUUID()}`;
		const counterpartyId = `test-entity-${randomUUID()}`;
		seededEntityIds.push(entityId);
		await seedEntity(entityId, 45, 2);

		const ownAlert = await insertAlert(entityId);
		const asCounterparty = await insertAlert(counterpartyId, { counterpartyEntityId: entityId });
		seededAlertIds.push(ownAlert, asCounterparty);

		const res = await fetch(`${authedBaseUrl}/entities/${entityId}`, {
			headers: { Cookie: signCookie('demo', 'demo') },
		});
		expect(res.status).toBe(200);
		const body = (await res.json()) as {
			entity: LiveEntity | null;
			alerts: Array<{ alert_id: string }>;
		};
		expect(body.entity?.entity_id).toBe(entityId);
		const alertIds = body.alerts.map((a) => a.alert_id);
		expect(alertIds).toContain(ownAlert);
		expect(alertIds).toContain(asCounterparty);
	});

	it('returns entity: null with populated alerts for a dark entity with no Redis state', async () => {
		const entityId = `test-entity-${randomUUID()}`;
		const alertId = await insertAlert(entityId);
		seededAlertIds.push(alertId);

		const res = await fetch(`${authedBaseUrl}/entities/${entityId}`, {
			headers: { Cookie: signCookie('demo', 'demo') },
		});
		expect(res.status).toBe(200);
		const body = (await res.json()) as { entity: LiveEntity | null; alerts: unknown[] };
		expect(body.entity).toBeNull();
		expect(body.alerts.length).toBeGreaterThan(0);
	});

	it('returns 404 for an operator with no saved workspace, even if the entity exists', async () => {
		const userId = randomUUID();
		await insertUserWithoutWorkspace(userId);
		seededUserIds.push(userId);
		const entityId = `test-entity-${randomUUID()}`;
		seededEntityIds.push(entityId);
		await seedEntity(entityId, 45, 2);

		const res = await fetch(`${authedBaseUrl}/entities/${entityId}`, {
			headers: { Cookie: signCookie(userId, 'operator') },
		});
		expect(res.status).toBe(404);
	});

	it("returns 404 for an operator when the entity's live position is outside their saved bounds", async () => {
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
		const entityId = `test-entity-${randomUUID()}`;
		seededEntityIds.push(entityId);
		await seedEntity(entityId, 40.7, -74); // New York -- outside France bounds

		const res = await fetch(`${authedBaseUrl}/entities/${entityId}`, {
			headers: { Cookie: signCookie(userId, 'operator') },
		});
		expect(res.status).toBe(404);
	});

	it("returns the entity and its alerts for an operator when it's inside their saved scope", async () => {
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
		const entityId = `test-entity-${randomUUID()}`;
		seededEntityIds.push(entityId);
		await seedEntity(entityId, 45, 2); // Paris -- inside France bounds

		const inScopeAlert = await insertAlert(entityId, {
			payload: { last_known_lat: 45, last_known_lon: 2 },
		});
		seededAlertIds.push(inScopeAlert);

		const res = await fetch(`${authedBaseUrl}/entities/${entityId}`, {
			headers: { Cookie: signCookie(userId, 'operator') },
		});
		expect(res.status).toBe(200);
		const body = (await res.json()) as {
			entity: LiveEntity | null;
			alerts: Array<{ alert_id: string }>;
		};
		expect(body.entity?.entity_id).toBe(entityId);
		expect(body.alerts.map((a) => a.alert_id)).toContain(inScopeAlert);
	});
});

interface HistoryPoint {
	entity_id: string;
	timestamp_ms: number;
	lat: number;
	lon: number;
}

async function insertPosition(
	entityId: string,
	timestampMs: number,
	lat: number,
	lon: number,
): Promise<void> {
	await pool.query(
		`INSERT INTO position_history (entity_id, entity_type, observed_at, timestamp_ms, lat, lon, source)
		 VALUES ($1, 'aircraft', to_timestamp($2 / 1000.0), $2, $3, $4, 'synthetic')
		 ON CONFLICT (entity_id, observed_at) DO NOTHING`,
		[entityId, timestampMs, lat, lon],
	);
}

// Phase 09 CP3: GET /entities/:entity_id/history -- TimescaleDB position timeline.
describe('GET /entities/:entity_id/history (integration)', () => {
	let authedServer: Server;
	let authedBaseUrl: string;
	const seededEntityIds: string[] = [];
	const seededUserIds: string[] = [];
	const seededAlertIdsForHistory: string[] = [];

	beforeAll(async () => {
		const app = express();
		app.use(cookieParser());
		app.use(requireAuth);
		app.use('/entities', entitiesRouter);
		authedServer = app.listen(0);
		await new Promise<void>((resolve) => authedServer.once('listening', resolve));
		const { port } = authedServer.address() as AddressInfo;
		authedBaseUrl = `http://localhost:${port}`;
	});

	afterAll(async () => {
		await new Promise<void>((resolve) => authedServer.close(() => resolve()));
	});

	afterEach(async () => {
		if (seededEntityIds.length > 0) {
			await pool.query('DELETE FROM position_history WHERE entity_id = ANY($1)', [seededEntityIds]);
			await redis.del(...seededEntityIds.map((id) => `entity:live:${id}`));
			seededEntityIds.length = 0;
		}
		if (seededAlertIdsForHistory.length > 0) {
			await pool.query('DELETE FROM alerts WHERE alert_id = ANY($1)', [seededAlertIdsForHistory]);
			seededAlertIdsForHistory.length = 0;
		}
		if (seededUserIds.length > 0) {
			await pool.query('DELETE FROM user_workspaces WHERE user_id = ANY($1)', [seededUserIds]);
			await pool.query('DELETE FROM users WHERE user_id = ANY($1)', [seededUserIds]);
			seededUserIds.length = 0;
		}
	});

	it('rejects a request with missing from_ms/to_ms', async () => {
		const res = await fetch(`${authedBaseUrl}/entities/some-id/history`, {
			headers: { Cookie: signCookie('demo', 'demo') },
		});
		expect(res.status).toBe(400);
	});

	it('rejects from_ms greater than to_ms', async () => {
		const res = await fetch(`${authedBaseUrl}/entities/some-id/history?from_ms=200&to_ms=100`, {
			headers: { Cookie: signCookie('demo', 'demo') },
		});
		expect(res.status).toBe(400);
	});

	it('returns points inside the window, ascending, and excludes points outside it', async () => {
		const entityId = `test-entity-${randomUUID()}`;
		seededEntityIds.push(entityId);
		const baseMs = Date.parse('2026-01-01T00:00:00.000Z');

		await insertPosition(entityId, baseMs, 45, 2); // before window
		await insertPosition(entityId, baseMs + 60_000, 45.1, 2.1); // inside window
		await insertPosition(entityId, baseMs + 120_000, 45.2, 2.2); // inside window
		await insertPosition(entityId, baseMs + 600_000, 46, 3); // after window

		const res = await fetch(
			`${authedBaseUrl}/entities/${entityId}/history?from_ms=${baseMs + 1}&to_ms=${baseMs + 300_000}`,
			{ headers: { Cookie: signCookie('demo', 'demo') } },
		);
		expect(res.status).toBe(200);
		const body = (await res.json()) as HistoryPoint[];
		expect(body.map((p) => p.timestamp_ms)).toEqual([baseMs + 60_000, baseMs + 120_000]);
	});

	it('caps the response at ENTITY_HISTORY_MAX_POINTS', async () => {
		const entityId = `test-entity-${randomUUID()}`;
		seededEntityIds.push(entityId);
		const baseMs = Date.parse('2026-01-02T00:00:00.000Z');
		const count = config.ENTITY_HISTORY_MAX_POINTS + 20;

		for (let i = 0; i < count; i++) {
			await insertPosition(entityId, baseMs + i * 1000, 45, 2);
		}

		const res = await fetch(
			`${authedBaseUrl}/entities/${entityId}/history?from_ms=${baseMs}&to_ms=${baseMs + count * 1000}`,
			{ headers: { Cookie: signCookie('demo', 'demo') } },
		);
		const body = (await res.json()) as HistoryPoint[];
		expect(body.length).toBe(config.ENTITY_HISTORY_MAX_POINTS);
	});

	it('returns 404 for an operator with no saved workspace', async () => {
		const userId = randomUUID();
		await insertUserWithoutWorkspace(userId);
		seededUserIds.push(userId);
		const entityId = `test-entity-${randomUUID()}`;
		seededEntityIds.push(entityId);
		await seedEntity(entityId, 45, 2);
		await insertPosition(entityId, Date.now(), 45, 2);

		const res = await fetch(
			`${authedBaseUrl}/entities/${entityId}/history?from_ms=0&to_ms=${Date.now() + 1_000_000}`,
			{ headers: { Cookie: signCookie(userId, 'operator') } },
		);
		expect(res.status).toBe(404);
	});

	it("returns 404 for an operator when the entity's live position is outside their saved bounds", async () => {
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
		const entityId = `test-entity-${randomUUID()}`;
		seededEntityIds.push(entityId);
		await seedEntity(entityId, 40.7, -74); // New York -- outside France bounds
		await insertPosition(entityId, Date.now(), 40.7, -74);

		const res = await fetch(
			`${authedBaseUrl}/entities/${entityId}/history?from_ms=0&to_ms=${Date.now() + 1_000_000}`,
			{ headers: { Cookie: signCookie(userId, 'operator') } },
		);
		expect(res.status).toBe(404);
	});

	it('returns history for a dark entity when it is the primary on an in-scope alert', async () => {
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
		const entityId = `test-entity-${randomUUID()}`;
		seededEntityIds.push(entityId);
		// No live Redis state -- entity has gone dark.
		const baseMs = Date.parse('2026-01-03T00:00:00.000Z');
		await insertPosition(entityId, baseMs, 45, 2);

		const alertId = await insertAlert(entityId, {
			payload: { last_known_lat: 45, last_known_lon: 2 },
		});
		seededAlertIdsForHistory.push(alertId);

		const res = await fetch(
			`${authedBaseUrl}/entities/${entityId}/history?from_ms=${baseMs - 1000}&to_ms=${baseMs + 1000}`,
			{ headers: { Cookie: signCookie(userId, 'operator') } },
		);
		expect(res.status).toBe(200);
		const body = (await res.json()) as HistoryPoint[];
		expect(body.map((p) => p.timestamp_ms)).toContain(baseMs);
	});
});

// Regression coverage for the route-mount-order bug this checkpoint's own
// GET /:entity_id could otherwise reintroduce: mounted in the wrong order,
// "/entities/live" would match GET /:entity_id with entity_id="live" and
// entitiesLiveRouter would never run. Mounts both routers exactly as
// index.ts does (live before the param route) to prove that doesn't happen.
describe('GET /entities and GET /entities/live mounted together (integration)', () => {
	let mountedServer: Server;
	let mountedBaseUrl: string;

	beforeAll(async () => {
		const app = express();
		app.use('/entities/live', entitiesLiveRouter);
		app.use('/entities', entitiesRouter);
		mountedServer = app.listen(0);
		await new Promise<void>((resolve) => mountedServer.once('listening', resolve));
		const { port } = mountedServer.address() as AddressInfo;
		mountedBaseUrl = `http://localhost:${port}`;
	});

	afterAll(async () => {
		await new Promise<void>((resolve) => mountedServer.close(() => resolve()));
		await redis.quit();
		await pool.end();
	});

	it('routes /entities/live to entitiesLiveRouter, not to GET /:entity_id', async () => {
		// entitiesLiveRouter requires bbox and returns 400 without it --
		// entitiesRouter's GET /:entity_id has no such validation and would
		// return 200 (404 at worst) if it wrongly received this request.
		const res = await fetch(`${mountedBaseUrl}/entities/live`);
		expect(res.status).toBe(400);
		const body = (await res.json()) as { error: string };
		expect(body.error).toBe('bbox query parameter is required');
	});

	it('still routes a real entity_id to GET /:entity_id', async () => {
		const res = await fetch(`${mountedBaseUrl}/entities/some-entity-id`);
		expect(res.status).toBe(404);
		const body = (await res.json()) as { error: string };
		expect(body.error).toBe('entity not found');
	});
});
