// Integration test for GET /alerts: proves the status filter and ordering
// against a real Postgres query, not a mocked pool result.
// Requires: `make up && make migrate` (locally) or the CI service containers.
import { randomUUID } from 'node:crypto';
import express from 'express';
import type { Server } from 'node:http';
import type { AddressInfo } from 'node:net';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';
import { pool } from '../db.js';
import { alertsRouter } from './alerts.js';

let server: Server;
let baseUrl: string;

async function insertAlert(
	overrides: Partial<{ status: string; detectedAt: Date }> = {},
): Promise<string> {
	const alertId = `test-alert-${randomUUID()}`;
	const detectedAt = overrides.detectedAt ?? new Date();
	await pool.query(
		`INSERT INTO alerts
			 (alert_id, entity_id, entity_type, alert_type, priority, status, payload, detected_at, updated_at)
		 VALUES ($1, 'test-entity', 'aircraft', 'SIGNAL_LOSS', 'STANDARD', $2, '{}', $3, $3)`,
		[alertId, overrides.status ?? 'NEW', detectedAt],
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
		await pool.end();
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
