// Runs against a REAL Redis, not a mock: the guarantee under test is that
// entity:live:* lat/lon reads combined with a real great-circle distance
// calculation correctly include/exclude real candidates -- a fake HMGET
// wouldn't prove a real threshold boundary is handled correctly.
//
// Requires: `make up`.
import { randomUUID } from 'node:crypto';
import { Redis } from 'ioredis';
import { afterAll, afterEach, describe, expect, it } from 'vitest';
import { config } from './config.js';
import { filterByDistance } from './distance.js';

const ORIGIN_LAT = 37.0;
const ORIGIN_LON = -121.0;

// ~111,320m per degree of latitude at the equator, close enough at this
// latitude for these test distances to land clearly inside/outside 1000m.
const NEARER_LAT = 37.0003; // ~33m from origin
const NEAR_LAT = 37.0009; // ~100m from origin
const FAR_LAT = 37.05; // ~5,566m from origin -- well outside the threshold

const redis = new Redis(config.REDIS_URL);
const testId = () => `test-distance-${randomUUID()}`;

const seededKeys = new Set<string>();

async function seedPosition(entityId: string, lat: number, lon: number): Promise<void> {
	seededKeys.add(`entity:live:${entityId}`);
	await redis.hset(
		`entity:live:${entityId}`,
		'lat',
		String(lat),
		'lon',
		String(lon),
		'entity_type',
		'aircraft',
	);
}

async function seedNoPosition(entityId: string): Promise<void> {
	seededKeys.add(`entity:live:${entityId}`);
	await redis.hset(`entity:live:${entityId}`, 'entity_type', 'aircraft');
}

async function seedPositionWithoutType(entityId: string, lat: number, lon: number): Promise<void> {
	seededKeys.add(`entity:live:${entityId}`);
	await redis.hset(`entity:live:${entityId}`, 'lat', String(lat), 'lon', String(lon));
}

afterEach(async () => {
	if (seededKeys.size > 0) {
		await redis.del(...seededKeys);
		seededKeys.clear();
	}
});

afterAll(async () => {
	await redis.quit();
});

describe('filterByDistance', () => {
	it('keeps a candidate within the threshold and reports its distance', async () => {
		const near = testId();
		await seedPosition(near, NEAR_LAT, ORIGIN_LON);

		const result = await filterByDistance(
			redis,
			ORIGIN_LAT,
			ORIGIN_LON,
			[near],
			config.PROXIMITY_THRESHOLD_METRES,
		);

		expect(result).toHaveLength(1);
		expect(result[0]!.entityId).toBe(near);
		expect(result[0]!.entityType).toBe('aircraft');
		expect(result[0]!.lat).toBe(NEAR_LAT);
		expect(result[0]!.lon).toBe(ORIGIN_LON);
		expect(result[0]!.distanceMetres).toBeGreaterThan(90);
		expect(result[0]!.distanceMetres).toBeLessThan(110);
	});

	it('excludes a candidate beyond the threshold', async () => {
		const far = testId();
		await seedPosition(far, FAR_LAT, ORIGIN_LON);

		const result = await filterByDistance(
			redis,
			ORIGIN_LAT,
			ORIGIN_LON,
			[far],
			config.PROXIMITY_THRESHOLD_METRES,
		);

		expect(result).toEqual([]);
	});

	it('skips a candidate with no live lat/lon rather than throwing', async () => {
		const ghost = testId();
		await seedNoPosition(ghost);

		const result = await filterByDistance(
			redis,
			ORIGIN_LAT,
			ORIGIN_LON,
			[ghost],
			config.PROXIMITY_THRESHOLD_METRES,
		);

		expect(result).toEqual([]);
	});

	it('skips a candidate with a position but no entity_type', async () => {
		const untyped = testId();
		await seedPositionWithoutType(untyped, NEAR_LAT, ORIGIN_LON);

		const result = await filterByDistance(
			redis,
			ORIGIN_LAT,
			ORIGIN_LON,
			[untyped],
			config.PROXIMITY_THRESHOLD_METRES,
		);

		expect(result).toEqual([]);
	});

	it('skips a candidate with no entity:live hash at all', async () => {
		const missing = testId(); // deliberately never seeded

		const result = await filterByDistance(
			redis,
			ORIGIN_LAT,
			ORIGIN_LON,
			[missing],
			config.PROXIMITY_THRESHOLD_METRES,
		);

		expect(result).toEqual([]);
	});

	it('sorts surviving candidates by ascending distance', async () => {
		const near = testId();
		const nearer = testId();
		await seedPosition(near, NEAR_LAT, ORIGIN_LON);
		await seedPosition(nearer, NEARER_LAT, ORIGIN_LON);

		const result = await filterByDistance(
			redis,
			ORIGIN_LAT,
			ORIGIN_LON,
			[near, nearer],
			config.PROXIMITY_THRESHOLD_METRES,
		);

		expect(result.map((m) => m.entityId)).toEqual([nearer, near]);
	});
});
