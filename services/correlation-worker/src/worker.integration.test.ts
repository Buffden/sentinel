// Integration test for the full correlation pipeline wired together in
// handlePosition: real Redis (candidate lookup, distance, episode state),
// real Neo4j (evidence, known-associate check), and a REAL Kafka producer
// publishing to the actual proximity.candidates topic, consumed back by a
// temporary test consumer -- proving serialization and delivery, not just
// that a function was called. Nothing in this codebase consumes
// proximity.candidates yet, so unlike alert-evaluator's equivalent test
// there is no live consumer to avoid polluting.
//
// Requires: `make up`, `make topics`, `make neo4j-schema`.
import { randomUUID } from 'node:crypto';
import { Kafka } from 'kafkajs';
import { Redis } from 'ioredis';
import neo4j, { type Driver, type Session } from 'neo4j-driver';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';
import { latLngToCell } from 'h3-js';
import { config } from './config.js';
import { handlePosition } from './worker.js';

interface CandidateMessage {
	pair_key: string;
	entity_a_id: string;
	entity_b_id: string;
	episode_start_ms: number;
	lat: number;
	lon: number;
	distance_at_detection: number;
}

const redis = new Redis(config.REDIS_URL);
let driver: Driver;
let session: Session;

const kafka = new Kafka({
	clientId: 'correlation-worker-test',
	brokers: config.KAFKA_BROKERS,
	logLevel: 0,
});
const producer = kafka.producer();
const testConsumer = kafka.consumer({ groupId: `test-correlation-worker-${randomUUID()}` });

const receivedCandidates: CandidateMessage[] = [];

async function waitForCandidate(
	pairKey: string,
	timeoutMs = 8_000,
): Promise<CandidateMessage | undefined> {
	const deadline = Date.now() + timeoutMs;
	while (Date.now() < deadline) {
		const found = receivedCandidates.find((c) => c.pair_key === pairKey);
		if (found) return found;
		await new Promise((r) => setTimeout(r, 100));
	}
	return undefined;
}

async function assertNoCandidate(pairKey: string, waitMs = 1_500): Promise<void> {
	await new Promise((r) => setTimeout(r, waitMs));
	expect(receivedCandidates.some((c) => c.pair_key === pairKey)).toBe(false);
}

const BASE_LAT = 37.0;
const BASE_LON = -121.0;
const RES = config.LIVE_H3_RESOLUTION;

const seededEntityIds = new Set<string>();
const seededRedisKeys = new Set<string>();

beforeAll(async () => {
	driver = neo4j.driver(
		config.NEO4J_URI,
		neo4j.auth.basic(config.NEO4J_USER, config.NEO4J_PASSWORD),
	);
	session = driver.session();

	await producer.connect();
	await testConsumer.connect();
	await testConsumer.subscribe({ topic: config.CANDIDATES_TOPIC, fromBeginning: false });

	const joined = new Promise<void>((resolve) => {
		testConsumer.on(testConsumer.events.GROUP_JOIN, () => resolve());
	});
	void testConsumer.run({
		eachMessage: async ({ message }) => {
			if (!message.value) return;
			receivedCandidates.push(JSON.parse(message.value.toString()) as CandidateMessage);
		},
	});
	await joined;
}, 30_000);

afterEach(async () => {
	receivedCandidates.length = 0;
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
	await testConsumer.disconnect();
	await producer.disconnect();
	await session.close();
	await driver.close();
	await redis.quit();
});

async function seedCandidateEntity(
	entityId: string,
	lat: number,
	lon: number,
	lastSeenMs: number,
): Promise<void> {
	const cell = latLngToCell(lat, lon, RES);
	seededRedisKeys.add(`entity:live:${entityId}`).add(`geo-cell:${cell}`);
	await redis.hset(
		`entity:live:${entityId}`,
		'lat',
		String(lat),
		'lon',
		String(lon),
		'entity_type',
		'aircraft',
	);
	await redis.zadd(`geo-cell:${cell}`, lastSeenMs, entityId);
}

describe('handlePosition', () => {
	it('publishes a real proximity.candidates message for a genuinely close, unscheduled pair', async () => {
		const incomingId = `test-worker-${randomUUID()}`;
		const candidateId = `test-worker-${randomUUID()}`;
		seededEntityIds.add(incomingId).add(candidateId);

		const observedAtMs = Date.now();
		// ~33m away -- well inside the default 1000m threshold.
		await seedCandidateEntity(candidateId, BASE_LAT + 0.0003, BASE_LON, observedAtMs);

		await handlePosition(redis, session, producer, {
			entity_id: incomingId,
			entity_type: 'aircraft',
			timestamp_ms: observedAtMs,
			lat: BASE_LAT,
			lon: BASE_LON,
		});

		const pairKey =
			incomingId <= candidateId ? `${incomingId}:${candidateId}` : `${candidateId}:${incomingId}`;
		seededRedisKeys.add(`proximity-episode:${pairKey}`);

		const candidate = await waitForCandidate(pairKey);
		expect(candidate).toBeDefined();
		expect(candidate?.episode_start_ms).toBe(observedAtMs);
		expect(candidate?.distance_at_detection).toBeGreaterThan(20);
		expect(candidate?.distance_at_detection).toBeLessThan(50);
		expect([candidate?.entity_a_id, candidate?.entity_b_id].sort()).toEqual(
			[incomingId, candidateId].sort(),
		);

		const publishState = await redis.hget(`proximity-episode:${pairKey}`, 'candidate_published');
		expect(publishState).toBe('1');
	});

	it('does not publish for a known-associate pair, but still records graph evidence', async () => {
		const incomingId = `test-worker-${randomUUID()}`;
		const candidateId = `test-worker-${randomUUID()}`;
		seededEntityIds.add(incomingId).add(candidateId);

		await session.executeWrite((tx) =>
			tx.run(
				'MERGE (a:Entity {id: $a}) MERGE (b:Entity {id: $b}) MERGE (a)-[:KNOWN_ASSOCIATE]->(b)',
				{ a: incomingId, b: candidateId },
			),
		);

		const observedAtMs = Date.now();
		await seedCandidateEntity(candidateId, BASE_LAT + 0.0003, BASE_LON, observedAtMs);

		await handlePosition(redis, session, producer, {
			entity_id: incomingId,
			entity_type: 'aircraft',
			timestamp_ms: observedAtMs,
			lat: BASE_LAT,
			lon: BASE_LON,
		});

		const pairKey =
			incomingId <= candidateId ? `${incomingId}:${candidateId}` : `${candidateId}:${incomingId}`;
		seededRedisKeys.add(`proximity-episode:${pairKey}`);

		await assertNoCandidate(pairKey);

		const edge = await session.executeRead((tx) =>
			tx.run(
				'MATCH (:Entity {id: $a})-[r:PROXIMITY_EVENT]-(:Entity {id: $b}) RETURN count(r) AS c',
				{ a: incomingId, b: candidateId },
			),
		);
		expect(edge.records[0]!.get('c').toNumber()).toBe(1);
	});

	it('does not publish a second time for a later ping in the same episode', async () => {
		const incomingId = `test-worker-${randomUUID()}`;
		const candidateId = `test-worker-${randomUUID()}`;
		seededEntityIds.add(incomingId).add(candidateId);

		const firstMs = Date.now();
		await seedCandidateEntity(candidateId, BASE_LAT + 0.0003, BASE_LON, firstMs);

		await handlePosition(redis, session, producer, {
			entity_id: incomingId,
			entity_type: 'aircraft',
			timestamp_ms: firstMs,
			lat: BASE_LAT,
			lon: BASE_LON,
		});

		const pairKey =
			incomingId <= candidateId ? `${incomingId}:${candidateId}` : `${candidateId}:${incomingId}`;
		seededRedisKeys.add(`proximity-episode:${pairKey}`);
		await waitForCandidate(pairKey);
		receivedCandidates.length = 0;

		const secondMs = firstMs + 5_000;
		await redis.zadd(
			`geo-cell:${latLngToCell(BASE_LAT + 0.0003, BASE_LON, RES)}`,
			secondMs,
			candidateId,
		);
		await handlePosition(redis, session, producer, {
			entity_id: incomingId,
			entity_type: 'aircraft',
			timestamp_ms: secondMs,
			lat: BASE_LAT,
			lon: BASE_LON,
		});

		await assertNoCandidate(pairKey);
	});

	it('redelivering the exact same position message does not create a second episode or a second publish', async () => {
		// Simulates Kafka at-least-once redelivery: a crash between handling a
		// message and committing its offset causes the identical message
		// (same timestamp_ms, not a later one) to be processed again.
		const incomingId = `test-worker-${randomUUID()}`;
		const candidateId = `test-worker-${randomUUID()}`;
		seededEntityIds.add(incomingId).add(candidateId);

		const observedAtMs = Date.now();
		await seedCandidateEntity(candidateId, BASE_LAT + 0.0003, BASE_LON, observedAtMs);

		const position = {
			entity_id: incomingId,
			entity_type: 'aircraft',
			timestamp_ms: observedAtMs,
			lat: BASE_LAT,
			lon: BASE_LON,
		};

		await handlePosition(redis, session, producer, position);

		const pairKey =
			incomingId <= candidateId ? `${incomingId}:${candidateId}` : `${candidateId}:${incomingId}`;
		seededRedisKeys.add(`proximity-episode:${pairKey}`);
		const first = await waitForCandidate(pairKey);
		expect(first).toBeDefined();
		receivedCandidates.length = 0;

		// Redeliver: identical message, identical timestamp_ms.
		await handlePosition(redis, session, producer, position);
		await assertNoCandidate(pairKey);

		const edgeCount = await session.executeRead((tx) =>
			tx.run(
				'MATCH (:Entity {id: $a})-[r:PROXIMITY_EVENT]-(:Entity {id: $b}) RETURN count(r) AS c',
				{ a: incomingId, b: candidateId },
			),
		);
		expect(edgeCount.records[0]!.get('c').toNumber()).toBe(1);
	});

	it('does nothing when no candidates are within range', async () => {
		const incomingId = `test-worker-${randomUUID()}`;
		seededEntityIds.add(incomingId);

		await handlePosition(redis, session, producer, {
			entity_id: incomingId,
			entity_type: 'aircraft',
			timestamp_ms: Date.now(),
			lat: BASE_LAT,
			lon: BASE_LON,
		});

		const keys = await redis.keys(`proximity-episode:*${incomingId}*`);
		expect(keys).toEqual([]);
	});
});
