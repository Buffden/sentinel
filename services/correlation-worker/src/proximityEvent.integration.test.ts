// Runs against a REAL Neo4j (docker-compose), not a mock: the guarantee under
// test is the actual database uniqueness constraint plus MERGE's directional
// pattern matching -- behavior confirmed by hand in cypher-shell before this
// suite was written (see concepts/neo4j-proximity-event). A fake driver would
// only prove the fake behaves as coded, not that the real constraint holds.
//
// Requires: `make up` and `make neo4j-schema` (applies the Entity.id and
// PROXIMITY_EVENT.idempotency_key uniqueness constraints this suite depends on).
import { randomUUID } from 'node:crypto';
import neo4j, { type Driver, type Session } from 'neo4j-driver';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';
import { config } from './config.js';
import { mergeProximityEvent, type ProximityEntity } from './proximityEvent.js';

let driver: Driver;
let session: Session;

const testEntity = (): ProximityEntity => ({
	id: `test-proximity-${randomUUID()}`,
	type: 'aircraft',
});

const seededIds = new Set<string>();

beforeAll(() => {
	driver = neo4j.driver(
		config.NEO4J_URI,
		neo4j.auth.basic(config.NEO4J_USER, config.NEO4J_PASSWORD),
	);
	session = driver.session();
});

afterEach(async () => {
	if (seededIds.size > 0) {
		await session.executeWrite((tx) =>
			tx.run('MATCH (e:Entity) WHERE e.id IN $ids DETACH DELETE e', {
				ids: [...seededIds],
			}),
		);
		seededIds.clear();
	}
});

afterAll(async () => {
	await session.close();
	await driver.close();
});

describe('mergeProximityEvent', () => {
	it('creates one edge with the initial detection properties', async () => {
		const a = testEntity();
		const b = testEntity();
		seededIds.add(a.id).add(b.id);

		await mergeProximityEvent(session, a, b, {
			episodeStartMs: 1_700_000_000_000,
			lastSeenMs: 1_700_000_000_000,
			distanceMetres: 500,
			lat: 37.0,
			lon: -121.0,
		});

		const result = await session.executeRead((tx) =>
			tx.run('MATCH (:Entity {id: $a})-[r:PROXIMITY_EVENT]-(:Entity {id: $b}) RETURN r', {
				a: a.id,
				b: b.id,
			}),
		);

		expect(result.records).toHaveLength(1);
		const r = result.records[0]!.get('r').properties;
		expect(r.episode_start_ms).toBe(1_700_000_000_000);
		expect(r.last_seen_ms).toBe(1_700_000_000_000);
		expect(r.min_distance_metres).toBe(500);
		expect(r.distance_at_detection).toBe(500);
	});

	it('refreshes last_seen_ms on a repeat call without duplicating the edge', async () => {
		const a = testEntity();
		const b = testEntity();
		seededIds.add(a.id).add(b.id);

		await mergeProximityEvent(session, a, b, {
			episodeStartMs: 1_700_000_000_000,
			lastSeenMs: 1_700_000_000_000,
			distanceMetres: 500,
			lat: 37.0,
			lon: -121.0,
		});
		await mergeProximityEvent(session, a, b, {
			episodeStartMs: 1_700_000_000_000,
			lastSeenMs: 1_700_000_009_000,
			distanceMetres: 500,
			lat: 37.0,
			lon: -121.0,
		});

		const result = await session.executeRead((tx) =>
			tx.run('MATCH (:Entity {id: $a})-[r:PROXIMITY_EVENT]-(:Entity {id: $b}) RETURN r', {
				a: a.id,
				b: b.id,
			}),
		);

		expect(result.records).toHaveLength(1);
		expect(result.records[0]!.get('r').properties.last_seen_ms).toBe(1_700_000_009_000);
	});

	it('produces the same edge regardless of which entity is passed first', async () => {
		const a = testEntity();
		const b = testEntity();
		seededIds.add(a.id).add(b.id);

		await mergeProximityEvent(session, a, b, {
			episodeStartMs: 1_700_000_000_000,
			lastSeenMs: 1_700_000_000_000,
			distanceMetres: 500,
			lat: 37.0,
			lon: -121.0,
		});
		// Same episode, arguments swapped -- would throw a constraint violation
		// if mergeProximityEvent did not canonicalize node order itself.
		await mergeProximityEvent(session, b, a, {
			episodeStartMs: 1_700_000_000_000,
			lastSeenMs: 1_700_000_009_000,
			distanceMetres: 500,
			lat: 37.0,
			lon: -121.0,
		});

		const result = await session.executeRead((tx) =>
			tx.run(
				'MATCH (:Entity {id: $a})-[r:PROXIMITY_EVENT]-(:Entity {id: $b}) RETURN count(r) AS c',
				{ a: a.id, b: b.id },
			),
		);

		expect(result.records[0]!.get('c').toNumber()).toBe(1);
	});

	it('tightens min_distance_metres but never lets it increase', async () => {
		const a = testEntity();
		const b = testEntity();
		seededIds.add(a.id).add(b.id);

		await mergeProximityEvent(session, a, b, {
			episodeStartMs: 1_700_000_000_000,
			lastSeenMs: 1_700_000_000_000,
			distanceMetres: 500,
			lat: 37.0,
			lon: -121.0,
		});
		// Farther than the first observation -- must not overwrite the minimum.
		await mergeProximityEvent(session, a, b, {
			episodeStartMs: 1_700_000_000_000,
			lastSeenMs: 1_700_000_005_000,
			distanceMetres: 800,
			lat: 37.0,
			lon: -121.0,
		});
		// Closer than both previous observations -- must become the new minimum.
		await mergeProximityEvent(session, a, b, {
			episodeStartMs: 1_700_000_000_000,
			lastSeenMs: 1_700_000_010_000,
			distanceMetres: 200,
			lat: 37.0,
			lon: -121.0,
		});

		const result = await session.executeRead((tx) =>
			tx.run('MATCH (:Entity {id: $a})-[r:PROXIMITY_EVENT]-(:Entity {id: $b}) RETURN r', {
				a: a.id,
				b: b.id,
			}),
		);

		const r = result.records[0]!.get('r').properties;
		expect(r.min_distance_metres).toBe(200);
		expect(r.distance_at_detection).toBe(500); // set once, at episode creation only
	});

	it('creates a separate edge for a different episode_start_ms on the same pair', async () => {
		const a = testEntity();
		const b = testEntity();
		seededIds.add(a.id).add(b.id);

		await mergeProximityEvent(session, a, b, {
			episodeStartMs: 1_700_000_000_000,
			lastSeenMs: 1_700_000_000_000,
			distanceMetres: 500,
			lat: 37.0,
			lon: -121.0,
		});
		await mergeProximityEvent(session, a, b, {
			episodeStartMs: 1_700_001_000_000, // a later, distinct encounter
			lastSeenMs: 1_700_001_000_000,
			distanceMetres: 300,
			lat: 37.0,
			lon: -121.0,
		});

		const result = await session.executeRead((tx) =>
			tx.run(
				'MATCH (:Entity {id: $a})-[r:PROXIMITY_EVENT]-(:Entity {id: $b}) RETURN count(r) AS c',
				{ a: a.id, b: b.id },
			),
		);

		expect(result.records[0]!.get('c').toNumber()).toBe(2);
	});

	it('sets Entity.type only on first creation', async () => {
		const a: ProximityEntity = { id: `test-proximity-${randomUUID()}`, type: 'aircraft' };
		const b = testEntity();
		seededIds.add(a.id).add(b.id);

		await mergeProximityEvent(session, a, b, {
			episodeStartMs: 1_700_000_000_000,
			lastSeenMs: 1_700_000_000_000,
			distanceMetres: 500,
			lat: 37.0,
			lon: -121.0,
		});

		const result = await session.executeRead((tx) =>
			tx.run('MATCH (e:Entity {id: $id}) RETURN e.type AS type', { id: a.id }),
		);

		expect(result.records).toHaveLength(1);
		expect(result.records[0]!.get('type')).toBe('aircraft');
	});
});
