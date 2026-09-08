// Runs against REAL Redis and REAL Neo4j, not mocks: the guarantee under
// test is the actual decision sequence -- episode timing, graph evidence,
// and the known-associate/publish gate -- working together against real
// state, not each piece individually (those are covered by their own
// integration suites).
//
// Requires: `make up` and `make neo4j-schema`.
import { randomUUID } from 'node:crypto';
import { Redis } from 'ioredis';
import neo4j, { type Driver, type Session } from 'neo4j-driver';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';
import { config } from './config.js';
import { markCandidatePublished } from './episode.js';
import { evaluateProximityEncounter, type ProximityObservation } from './proximityDecision.js';
import type { ProximityEntity } from './proximityEvent.js';

const redis = new Redis(config.REDIS_URL);
let driver: Driver;
let session: Session;

const testEntity = (): ProximityEntity => ({
	id: `test-decision-${randomUUID()}`,
	type: 'aircraft',
});

const observation = (overrides: Partial<ProximityObservation> = {}): ProximityObservation => ({
	observedAtMs: 1_700_000_000_000,
	distanceMetres: 500,
	lat: 37.0,
	lon: -121.0,
	...overrides,
});

const seededEntityIds = new Set<string>();
const seededRedisKeys = new Set<string>();

beforeAll(() => {
	driver = neo4j.driver(
		config.NEO4J_URI,
		neo4j.auth.basic(config.NEO4J_USER, config.NEO4J_PASSWORD),
	);
	session = driver.session();
});

afterEach(async () => {
	if (seededRedisKeys.size > 0) {
		await redis.del(...seededRedisKeys);
		seededRedisKeys.clear();
	}
	if (seededEntityIds.size > 0) {
		await session.executeWrite((tx) =>
			tx.run('MATCH (e:Entity) WHERE e.id IN $ids DETACH DELETE e', {
				ids: [...seededEntityIds],
			}),
		);
		seededEntityIds.clear();
	}
});

afterAll(async () => {
	await session.close();
	await driver.close();
	await redis.quit();
});

describe('evaluateProximityEncounter', () => {
	it('publishes a candidate for a brand-new unscheduled episode', async () => {
		const a = testEntity();
		const b = testEntity();
		seededEntityIds.add(a.id).add(b.id);

		const decision = await evaluateProximityEncounter(redis, session, a, b, observation(), 60_000);
		seededRedisKeys.add(`proximity-episode:${decision.pairKey}`);

		expect(decision.isNewEpisode).toBe(true);
		expect(decision.isKnownAssociate).toBe(false);
		expect(decision.shouldPublishCandidate).toBe(true);

		const publishState = await redis.hget(
			`proximity-episode:${decision.pairKey}`,
			'candidate_published',
		);
		expect(publishState).toBe('0');
	});

	it('records graph evidence but never publishes for a known-associate pair', async () => {
		const a = testEntity();
		const b = testEntity();
		seededEntityIds.add(a.id).add(b.id);

		await session.executeWrite((tx) =>
			tx.run(
				'MERGE (a:Entity {id: $a}) MERGE (b:Entity {id: $b}) MERGE (a)-[:KNOWN_ASSOCIATE]->(b)',
				{
					a: a.id,
					b: b.id,
				},
			),
		);

		const decision = await evaluateProximityEncounter(redis, session, a, b, observation(), 60_000);
		seededRedisKeys.add(`proximity-episode:${decision.pairKey}`);

		expect(decision.isKnownAssociate).toBe(true);
		expect(decision.shouldPublishCandidate).toBe(false);

		// Evidence is retained regardless of known-associate status.
		const edge = await session.executeRead((tx) =>
			tx.run(
				'MATCH (:Entity {id: $a})-[r:PROXIMITY_EVENT]-(:Entity {id: $b}) RETURN count(r) AS c',
				{ a: a.id, b: b.id },
			),
		);
		expect(edge.records[0]!.get('c').toNumber()).toBe(1);

		// candidate_published is never written for a known associate.
		const publishState = await redis.hget(
			`proximity-episode:${decision.pairKey}`,
			'candidate_published',
		);
		expect(publishState).toBeNull();
	});

	it('does not re-publish a confirmed candidate on a later ping in the same episode', async () => {
		const a = testEntity();
		const b = testEntity();
		seededEntityIds.add(a.id).add(b.id);

		const first = await evaluateProximityEncounter(redis, session, a, b, observation(), 60_000);
		seededRedisKeys.add(`proximity-episode:${first.pairKey}`);
		await markCandidatePublished(redis, first.pairKey);

		const second = await evaluateProximityEncounter(
			redis,
			session,
			a,
			b,
			observation({ observedAtMs: 1_700_000_010_000 }),
			60_000,
		);

		expect(second.isNewEpisode).toBe(false);
		expect(second.isKnownAssociate).toBe(false);
		expect(second.shouldPublishCandidate).toBe(false);
	});

	it('retries publishing on a later ping if the previous attempt never confirmed', async () => {
		const a = testEntity();
		const b = testEntity();
		seededEntityIds.add(a.id).add(b.id);

		// First call leaves candidate_published at '0' -- simulating a crash or
		// failed Kafka publish between markCandidatePending and confirmation.
		const first = await evaluateProximityEncounter(redis, session, a, b, observation(), 60_000);
		seededRedisKeys.add(`proximity-episode:${first.pairKey}`);
		expect(first.shouldPublishCandidate).toBe(true);

		const second = await evaluateProximityEncounter(
			redis,
			session,
			a,
			b,
			observation({ observedAtMs: 1_700_000_010_000 }),
			60_000,
		);

		expect(second.isNewEpisode).toBe(false);
		expect(second.shouldPublishCandidate).toBe(true);
	});

	it('recognizes an existing known-associate episode without re-querying a fresh publish decision', async () => {
		const a = testEntity();
		const b = testEntity();
		seededEntityIds.add(a.id).add(b.id);

		await session.executeWrite((tx) =>
			tx.run(
				'MERGE (a:Entity {id: $a}) MERGE (b:Entity {id: $b}) MERGE (a)-[:KNOWN_ASSOCIATE]->(b)',
				{
					a: a.id,
					b: b.id,
				},
			),
		);

		const first = await evaluateProximityEncounter(redis, session, a, b, observation(), 60_000);
		seededRedisKeys.add(`proximity-episode:${first.pairKey}`);

		const second = await evaluateProximityEncounter(
			redis,
			session,
			a,
			b,
			observation({ observedAtMs: 1_700_000_010_000 }),
			60_000,
		);

		expect(second.isNewEpisode).toBe(false);
		expect(second.isKnownAssociate).toBe(true);
		expect(second.shouldPublishCandidate).toBe(false);
	});
});
