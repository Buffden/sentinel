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
		await redis.quit();
		await pool.end();
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
