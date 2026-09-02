// Integration tests for the idempotency and monotonicity guarantees owned by
// consumer.ts. These run against a REAL TimescaleDB and Redis (docker-compose),
// not mocks — the guarantees under test live in Postgres's unique-index engine
// and Redis's Lua-script atomicity, not in application code, so a fake client
// would only prove the fake behaves as coded, not that the real constraint holds.
//
// Requires: `make up && make migrate` (locally) or the CI service containers.
import { randomUUID } from 'node:crypto';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';
import type { NormalizedPosition } from './normalize.js';
import {
	pool,
	redis,
	writeRawEvent,
	writePositionHistory,
	updateLiveState,
	updateGeoCell,
} from './consumer.js';

function buildPosition(overrides: Partial<NormalizedPosition> = {}): NormalizedPosition {
	return {
		entity_id: 'test-entity',
		entity_type: 'aircraft',
		timestamp_ms: 1_700_000_000_000,
		lat: 51.5,
		lon: -0.1,
		speed_mps: null,
		course_deg: null,
		heading_deg: null,
		source: 'adsb',
		provider: 'opensky',
		altitude_m: null,
		baro_altitude_m: null,
		geo_altitude_m: null,
		vertical_rate_mps: null,
		on_ground: null,
		last_contact_ms: null,
		squawk: null,
		spi: null,
		position_source: null,
		navigation_status: null,
		rate_of_turn: null,
		position_accuracy: null,
		destination: null,
		eta: null,
		draught_m: null,
		callsign: null,
		entity_subtype: null,
		provider_category: null,
		...overrides,
	};
}

describe('consumer.ts idempotency and monotonicity (integration)', () => {
	beforeAll(async () => {
		await pool.query('SELECT 1'); // fail fast with a clear error if Postgres is unreachable
		await redis.ping(); // fail fast if Redis is unreachable
	});

	afterAll(async () => {
		await pool.end();
		await redis.quit();
	});

	describe('writeRawEvent — idempotent by (source_topic, source_partition, source_offset)', () => {
		const topic = `test.raw.${randomUUID()}`;
		const partition = 0;
		const offset = '42';

		afterEach(async () => {
			await pool.query('DELETE FROM raw_events WHERE source_topic = $1', [topic]);
		});

		it('replaying the same Kafka message never produces a second row', async () => {
			await writeRawEvent(
				JSON.stringify({ icao24: 'first' }),
				'entity-first',
				'adsb',
				'opensky',
				topic,
				partition,
				offset,
				new Date(1_700_000_000_000),
			);

			// Simulated redelivery of the SAME Kafka record: identical topic/partition/offset.
			// A real redelivery carries the identical payload too, but using a different
			// payload here makes it unambiguous whether the second call was a no-op or a write.
			await writeRawEvent(
				JSON.stringify({ icao24: 'second' }),
				'entity-second',
				'adsb',
				'opensky',
				topic,
				partition,
				offset,
				new Date(1_700_000_099_000),
			);

			const { rows } = await pool.query(
				'SELECT entity_id, payload FROM raw_events WHERE source_topic = $1 AND source_partition = $2 AND source_offset = $3',
				[topic, partition, offset],
			);

			expect(rows).toHaveLength(1);
			expect(rows[0].entity_id).toBe('entity-first');
			expect(rows[0].payload).toEqual({ icao24: 'first' });
		});

		it('a different offset on the same partition is a distinct row', async () => {
			await writeRawEvent(
				JSON.stringify({ icao24: 'a' }),
				'entity-a',
				'adsb',
				'opensky',
				topic,
				partition,
				'100',
				new Date(),
			);
			await writeRawEvent(
				JSON.stringify({ icao24: 'b' }),
				'entity-b',
				'adsb',
				'opensky',
				topic,
				partition,
				'101',
				new Date(),
			);

			const { rows } = await pool.query(
				'SELECT source_offset FROM raw_events WHERE source_topic = $1 ORDER BY source_offset',
				[topic],
			);
			expect(rows.map((r: { source_offset: string }) => r.source_offset)).toEqual(['100', '101']);
		});
	});

	describe('writePositionHistory — idempotent by (entity_id, observed_at)', () => {
		const entityId = `test-entity-${randomUUID()}`;

		afterEach(async () => {
			await pool.query('DELETE FROM position_history WHERE entity_id = $1', [entityId]);
		});

		it('replaying the same source-time position never produces a second row', async () => {
			const first = buildPosition({
				entity_id: entityId,
				timestamp_ms: 1_700_000_000_000,
				lat: 51.5,
				lon: -0.1,
			});
			await writePositionHistory(first, 'history-cell-1');

			// Redelivery of the same message: same entity_id + timestamp_ms (→ same observed_at),
			// deliberately different lat/lon so it's unambiguous whether this landed a second row.
			const replay = buildPosition({
				entity_id: entityId,
				timestamp_ms: 1_700_000_000_000,
				lat: 60.0,
				lon: 10.0,
			});
			await writePositionHistory(replay, 'history-cell-2');

			const { rows } = await pool.query(
				'SELECT lat, lon, geo_cell FROM position_history WHERE entity_id = $1',
				[entityId],
			);
			expect(rows).toHaveLength(1);
			expect(rows[0].lat).toBe(51.5);
			expect(rows[0].lon).toBe(-0.1);
			expect(rows[0].geo_cell).toBe('history-cell-1');
		});

		it('a different source timestamp for the same entity is a distinct row', async () => {
			await writePositionHistory(
				buildPosition({ entity_id: entityId, timestamp_ms: 1_700_000_000_000 }),
				'cell-1',
			);
			await writePositionHistory(
				buildPosition({ entity_id: entityId, timestamp_ms: 1_700_000_060_000 }),
				'cell-1',
			);

			const { rows } = await pool.query(
				'SELECT count(*)::int AS n FROM position_history WHERE entity_id = $1',
				[entityId],
			);
			expect(rows[0].n).toBe(2);
		});
	});

	describe('updateLiveState — monotonic by last_seen_ms (Lua guard)', () => {
		const entityId = `test-entity-${randomUUID()}`;
		const liveKey = `entity:live:${entityId}`;

		afterEach(async () => {
			await redis.del(liveKey);
		});

		it('accepts the first write and sets a TTL', async () => {
			const accepted = await updateLiveState(
				buildPosition({ entity_id: entityId, timestamp_ms: 1_700_000_000_000, callsign: 'BAW1' }),
				'live-cell-1',
			);
			expect(accepted).toBe(true);

			const hash = await redis.hgetall(liveKey);
			expect(hash['last_seen_ms']).toBe('1700000000000');
			expect(hash['callsign']).toBe('BAW1');

			const ttl = await redis.ttl(liveKey);
			expect(ttl).toBeGreaterThan(0);
		});

		it('rejects an older timestamp and leaves the hash untouched', async () => {
			await updateLiveState(
				buildPosition({ entity_id: entityId, timestamp_ms: 1_700_000_000_000, callsign: 'BAW1' }),
				'live-cell-1',
			);

			const accepted = await updateLiveState(
				buildPosition({ entity_id: entityId, timestamp_ms: 1_699_999_999_000, callsign: 'STALE' }),
				'live-cell-stale',
			);
			expect(accepted).toBe(false);

			const hash = await redis.hgetall(liveKey);
			expect(hash['last_seen_ms']).toBe('1700000000000');
			expect(hash['callsign']).toBe('BAW1');
			expect(hash['live_geo_cell']).toBe('live-cell-1');
		});

		it('rejects an equal timestamp — strictly-newer is required, not newer-or-equal', async () => {
			await updateLiveState(
				buildPosition({ entity_id: entityId, timestamp_ms: 1_700_000_000_000 }),
				'live-cell-1',
			);
			const accepted = await updateLiveState(
				buildPosition({
					entity_id: entityId,
					timestamp_ms: 1_700_000_000_000,
					callsign: 'DUPLICATE',
				}),
				'live-cell-1',
			);
			expect(accepted).toBe(false);

			const hash = await redis.hgetall(liveKey);
			expect(hash['callsign']).toBe('');
		});

		it('accepts a newer timestamp and overwrites every field, including live_geo_cell', async () => {
			await updateLiveState(
				buildPosition({ entity_id: entityId, timestamp_ms: 1_700_000_000_000, callsign: 'BAW1' }),
				'live-cell-1',
			);
			const accepted = await updateLiveState(
				buildPosition({ entity_id: entityId, timestamp_ms: 1_700_000_060_000, callsign: 'BAW2' }),
				'live-cell-2',
			);
			expect(accepted).toBe(true);

			const hash = await redis.hgetall(liveKey);
			expect(hash['last_seen_ms']).toBe('1700000060000');
			expect(hash['callsign']).toBe('BAW2');
			expect(hash['live_geo_cell']).toBe('live-cell-2');
		});
	});

	describe('updateGeoCell — sorted-set membership follows the monotonic-accepted cell', () => {
		const entityId = `test-entity-${randomUUID()}`;
		const cellA = `test-cell-a-${randomUUID()}`;
		const cellB = `test-cell-b-${randomUUID()}`;

		afterEach(async () => {
			await redis.del(`geo-cell:${cellA}`, `geo-cell:${cellB}`);
		});

		it('adds the entity to the new cell with the source timestamp as score', async () => {
			await updateGeoCell(entityId, cellA, 1_700_000_000_000, null);
			const score = await redis.zscore(`geo-cell:${cellA}`, entityId);
			expect(score).toBe('1700000000000');
		});

		it('moves the entity: removes it from the old cell, adds it to the new one', async () => {
			await updateGeoCell(entityId, cellA, 1_700_000_000_000, null);
			await updateGeoCell(entityId, cellB, 1_700_000_060_000, cellA);

			const oldScore = await redis.zscore(`geo-cell:${cellA}`, entityId);
			const newScore = await redis.zscore(`geo-cell:${cellB}`, entityId);
			expect(oldScore).toBeNull();
			expect(newScore).toBe('1700000060000');
		});

		it('re-pinging the same cell upserts the score instead of duplicating the member', async () => {
			await updateGeoCell(entityId, cellA, 1_700_000_000_000, null);
			await updateGeoCell(entityId, cellA, 1_700_000_060_000, cellA);

			const members = await redis.zrange(`geo-cell:${cellA}`, '0', '-1');
			expect(members).toEqual([entityId]);
			const score = await redis.zscore(`geo-cell:${cellA}`, entityId);
			expect(score).toBe('1700000060000');
		});
	});
});
