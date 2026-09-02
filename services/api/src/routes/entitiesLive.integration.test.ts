// Integration test for GET /entities/live: proves bbox validation, bbox
// filtering, staleness filtering, and the response cap against real Redis
// entity:live:* hashes, not a mocked scan result.
// Requires: `make up` (Redis) or the CI service container.
import { randomUUID } from 'node:crypto';
import express from 'express';
import type { Server } from 'node:http';
import type { AddressInfo } from 'node:net';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';
import { redis } from '../redis.js';
import { config } from '../config.js';
import { entitiesLiveRouter } from './entitiesLive.js';

let server: Server;
let baseUrl: string;

interface LiveEntity {
	entity_id: string;
	lat: number;
	lon: number;
	last_seen_ms: number;
	on_ground: boolean | null;
}

function seedEntity(
	entityId: string,
	fields: Partial<{ lat: number; lon: number; last_seen_ms: number; on_ground: string }>,
): Promise<number> {
	const hashFields: string[] = [];
	if (fields.lat !== undefined) hashFields.push('lat', String(fields.lat));
	if (fields.lon !== undefined) hashFields.push('lon', String(fields.lon));
	if (fields.last_seen_ms !== undefined)
		hashFields.push('last_seen_ms', String(fields.last_seen_ms));
	if (fields.on_ground !== undefined) hashFields.push('on_ground', fields.on_ground);
	return redis.hset(`entity:live:${entityId}`, ...hashFields);
}

describe('GET /entities/live (integration)', () => {
	const seededIds: string[] = [];

	beforeAll(async () => {
		await redis.ping(); // fail fast with a clear error if Redis is unreachable

		const app = express();
		app.use('/entities/live', entitiesLiveRouter);
		server = app.listen(0);
		await new Promise<void>((resolve) => server.once('listening', resolve));
		const { port } = server.address() as AddressInfo;
		baseUrl = `http://localhost:${port}`;
	});

	afterAll(async () => {
		await new Promise<void>((resolve) => server.close(() => resolve()));
		await redis.quit();
	});

	afterEach(async () => {
		if (seededIds.length > 0) {
			await redis.del(...seededIds.map((id) => `entity:live:${id}`));
			seededIds.length = 0;
		}
	});

	it('rejects a request with no bbox query parameter', async () => {
		const res = await fetch(`${baseUrl}/entities/live`);
		expect(res.status).toBe(400);
	});

	it('rejects a malformed bbox (wrong part count)', async () => {
		const res = await fetch(`${baseUrl}/entities/live?bbox=1,2,3`);
		expect(res.status).toBe(400);
	});

	it('rejects a malformed bbox (non-numeric part)', async () => {
		const res = await fetch(`${baseUrl}/entities/live?bbox=1,2,3,abc`);
		expect(res.status).toBe(400);
	});

	it('includes a fresh entity inside the bbox and excludes one outside it', async () => {
		const inside = `test-live-${randomUUID()}`;
		const outside = `test-live-${randomUUID()}`;
		seededIds.push(inside, outside);
		const nowMs = Date.now();

		await seedEntity(inside, { lat: 51.5, lon: -0.1, last_seen_ms: nowMs });
		await seedEntity(outside, { lat: 10, lon: 10, last_seen_ms: nowMs });

		const res = await fetch(`${baseUrl}/entities/live?bbox=50,-1,52,1`);
		const body = (await res.json()) as LiveEntity[];
		const ids = body.map((e) => e.entity_id);

		expect(ids).toContain(inside);
		expect(ids).not.toContain(outside);
	});

	it('excludes an entity whose last_seen_ms is older than the staleness cutoff', async () => {
		const staleId = `test-live-${randomUUID()}`;
		seededIds.push(staleId);
		const staleMs = Date.now() - config.LIVE_ENTITY_STALE_AFTER_MS - 60_000;

		await seedEntity(staleId, { lat: 51.5, lon: -0.1, last_seen_ms: staleMs });

		const res = await fetch(`${baseUrl}/entities/live?bbox=50,-1,52,1`);
		const body = (await res.json()) as LiveEntity[];
		expect(body.map((e) => e.entity_id)).not.toContain(staleId);
	});

	it('excludes an entity with no position (missing lat/lon)', async () => {
		const noPosId = `test-live-${randomUUID()}`;
		seededIds.push(noPosId);
		await seedEntity(noPosId, { last_seen_ms: Date.now() });

		const res = await fetch(`${baseUrl}/entities/live?bbox=-90,-180,90,180`);
		const body = (await res.json()) as LiveEntity[];
		expect(body.map((e) => e.entity_id)).not.toContain(noPosId);
	});

	it('maps on_ground to a real boolean, and to null when absent', async () => {
		const groundedId = `test-live-${randomUUID()}`;
		const airborneId = `test-live-${randomUUID()}`;
		const unknownId = `test-live-${randomUUID()}`;
		seededIds.push(groundedId, airborneId, unknownId);
		const nowMs = Date.now();

		await seedEntity(groundedId, { lat: 51.5, lon: -0.1, last_seen_ms: nowMs, on_ground: 'true' });
		await seedEntity(airborneId, { lat: 51.5, lon: -0.1, last_seen_ms: nowMs, on_ground: 'false' });
		await seedEntity(unknownId, { lat: 51.5, lon: -0.1, last_seen_ms: nowMs });

		const res = await fetch(`${baseUrl}/entities/live?bbox=50,-1,52,1`);
		const body = (await res.json()) as LiveEntity[];
		const byId = new Map(body.map((e) => [e.entity_id, e]));

		expect(byId.get(groundedId)?.on_ground).toBe(true);
		expect(byId.get(airborneId)?.on_ground).toBe(false);
		expect(byId.get(unknownId)?.on_ground).toBeNull();
	});

	it('caps the response at LIVE_ENTITIES_MAX even when more fresh entities exist', async () => {
		const ids = Array.from(
			{ length: config.LIVE_ENTITIES_MAX + 20 },
			() => `test-live-${randomUUID()}`,
		);
		seededIds.push(...ids);
		const nowMs = Date.now();

		const pipeline = redis.pipeline();
		for (const id of ids) {
			pipeline.hset(
				`entity:live:${id}`,
				'lat',
				'51.5',
				'lon',
				'-0.1',
				'last_seen_ms',
				String(nowMs),
			);
		}
		await pipeline.exec();

		const res = await fetch(`${baseUrl}/entities/live?bbox=50,-1,52,1`);
		const body = (await res.json()) as LiveEntity[];
		expect(body.length).toBe(config.LIVE_ENTITIES_MAX);
	});
});
