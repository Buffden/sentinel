// Integration test for POST/PUT /users/me/workspace: proves persistence,
// the demo-role gate, validation, and the no-workspace-yet 404 against real
// Postgres, not a mocked pool result.
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
import { workspaceRouter } from './workspace.js';

let server: Server;
let baseUrl: string;

function signCookie(userId: string, role: 'operator' | 'demo'): string {
	const token = jwt.sign(
		{ user_id: userId, email: `${userId}@example.com`, role },
		config.JWT_SECRET,
		{
			expiresIn: '1h',
		},
	);
	return `sentinel_jwt=${token}`;
}

async function insertUser(userId: string): Promise<void> {
	await pool.query(
		`INSERT INTO users (user_id, google_sub, email, last_login_at, created_at)
		 VALUES ($1, $2, $3, now(), now())`,
		[userId, `google-${userId}`, `${userId}@example.com`],
	);
}

interface WorkspaceScopeResponse {
	geo_region: { name: string | null; bounds: Record<string, number> };
	entity_types: string[];
	alert_types: string[];
}

describe('POST/PUT /users/me/workspace (integration)', () => {
	const seededUserIds: string[] = [];

	beforeAll(async () => {
		await pool.query('SELECT 1'); // fail fast with a clear error if Postgres is unreachable

		const app = express();
		app.use(express.json());
		app.use(cookieParser());
		app.use(requireAuth);
		app.use('/users/me/workspace', workspaceRouter);
		server = app.listen(0);
		await new Promise<void>((resolve) => server.once('listening', resolve));
		const { port } = server.address() as AddressInfo;
		baseUrl = `http://localhost:${port}`;
	});

	afterAll(async () => {
		await new Promise<void>((resolve) => server.close(() => resolve()));
		await pool.end();
	});

	afterEach(async () => {
		if (seededUserIds.length > 0) {
			await pool.query('DELETE FROM user_workspaces WHERE user_id = ANY($1)', [seededUserIds]);
			await pool.query('DELETE FROM users WHERE user_id = ANY($1)', [seededUserIds]);
			seededUserIds.length = 0;
		}
	});

	it('returns 404 no_workspace for an operator with no saved workspace', async () => {
		const userId = randomUUID();
		await insertUser(userId);
		seededUserIds.push(userId);

		const res = await fetch(`${baseUrl}/users/me/workspace`, {
			method: 'POST',
			headers: { Cookie: signCookie(userId, 'operator') },
		});

		expect(res.status).toBe(404);
		expect(await res.json()).toEqual({ error: 'no_workspace' });
	});

	it('creates a workspace on PUT and returns the identical scope on the next read', async () => {
		const userId = randomUUID();
		await insertUser(userId);
		seededUserIds.push(userId);
		const cookie = signCookie(userId, 'operator');

		const putRes = await fetch(`${baseUrl}/users/me/workspace`, {
			method: 'PUT',
			headers: { Cookie: cookie, 'Content-Type': 'application/json' },
			body: JSON.stringify({
				geo_region: { name: 'France' },
				entity_types: ['aircraft'],
				alert_types: ['SIGNAL_LOSS'],
			}),
		});
		expect(putRes.status).toBe(200);
		const putBody = (await putRes.json()) as WorkspaceScopeResponse;
		expect(putBody.geo_region).toEqual({
			name: 'France',
			bounds: { min_lat: 41.3, max_lat: 51.1, min_lon: -5.2, max_lon: 9.6 },
		});

		const getRes = await fetch(`${baseUrl}/users/me/workspace`, {
			method: 'POST',
			headers: { Cookie: cookie },
		});
		expect(getRes.status).toBe(200);
		expect(await getRes.json()).toEqual(putBody);
	});

	it('replaces an existing workspace on a second PUT (upsert, not a duplicate row)', async () => {
		const userId = randomUUID();
		await insertUser(userId);
		seededUserIds.push(userId);
		const cookie = signCookie(userId, 'operator');

		await fetch(`${baseUrl}/users/me/workspace`, {
			method: 'PUT',
			headers: { Cookie: cookie, 'Content-Type': 'application/json' },
			body: JSON.stringify({
				geo_region: { name: 'France' },
				entity_types: ['aircraft'],
				alert_types: ['SIGNAL_LOSS'],
			}),
		});

		const secondPut = await fetch(`${baseUrl}/users/me/workspace`, {
			method: 'PUT',
			headers: { Cookie: cookie, 'Content-Type': 'application/json' },
			body: JSON.stringify({
				geo_region: { name: 'United Kingdom' },
				entity_types: ['aircraft'],
				alert_types: ['SIGNAL_LOSS', 'COMPOSITE'],
			}),
		});
		expect(secondPut.status).toBe(200);

		const rows = await pool.query<{ scope: WorkspaceScopeResponse }>(
			'SELECT scope FROM user_workspaces WHERE user_id = $1',
			[userId],
		);
		expect(rows.rows).toHaveLength(1);
		expect(rows.rows[0]!.scope.geo_region.name).toBe('United Kingdom');
		expect(rows.rows[0]!.scope.alert_types).toEqual(['SIGNAL_LOSS', 'COMPOSITE']);
	});

	it('rejects an invalid PUT body (inverted bounds) and writes nothing', async () => {
		const userId = randomUUID();
		await insertUser(userId);
		seededUserIds.push(userId);

		const res = await fetch(`${baseUrl}/users/me/workspace`, {
			method: 'PUT',
			headers: { Cookie: signCookie(userId, 'operator'), 'Content-Type': 'application/json' },
			body: JSON.stringify({
				geo_region: { name: null, bounds: { min_lat: 50, max_lat: 10, min_lon: 0, max_lon: 1 } },
				entity_types: ['aircraft'],
				alert_types: ['SIGNAL_LOSS'],
			}),
		});

		expect(res.status).toBe(400);
		const rows = await pool.query('SELECT 1 FROM user_workspaces WHERE user_id = $1', [userId]);
		expect(rows.rows).toHaveLength(0);
	});

	it('rejects entity_types outside the allowed v1 set', async () => {
		const userId = randomUUID();
		await insertUser(userId);
		seededUserIds.push(userId);

		const res = await fetch(`${baseUrl}/users/me/workspace`, {
			method: 'PUT',
			headers: { Cookie: signCookie(userId, 'operator'), 'Content-Type': 'application/json' },
			body: JSON.stringify({
				geo_region: { name: 'Global' },
				entity_types: ['vessel'],
				alert_types: ['SIGNAL_LOSS'],
			}),
		});

		expect(res.status).toBe(400);
	});

	it('returns 403 for a demo session on both read and write, without touching the database', async () => {
		const demoCookie = signCookie('demo', 'demo');

		const readRes = await fetch(`${baseUrl}/users/me/workspace`, {
			method: 'POST',
			headers: { Cookie: demoCookie },
		});
		expect(readRes.status).toBe(403);

		const writeRes = await fetch(`${baseUrl}/users/me/workspace`, {
			method: 'PUT',
			headers: { Cookie: demoCookie, 'Content-Type': 'application/json' },
			body: JSON.stringify({
				geo_region: { name: 'Global' },
				entity_types: ['aircraft'],
				alert_types: ['SIGNAL_LOSS'],
			}),
		});
		expect(writeRes.status).toBe(403);
	});

	it('serves the predefined region catalog', async () => {
		const userId = randomUUID();
		await insertUser(userId);
		seededUserIds.push(userId);

		const res = await fetch(`${baseUrl}/users/me/workspace/regions`, {
			headers: { Cookie: signCookie(userId, 'operator') },
		});
		expect(res.status).toBe(200);
		const body = (await res.json()) as Array<{ name: string }>;
		expect(body.map((r) => r.name)).toContain('France');
	});
});
