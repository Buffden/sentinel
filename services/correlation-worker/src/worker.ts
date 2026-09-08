// Correlation Worker: consume position.normalized, find real proximity
// candidates, record graph evidence, and publish proximity.candidates for
// new unscheduled encounters.
//
// Consumer group: correlation-worker (canonical; do not change without ADR).
//
// Per message: find H3 candidates -> filter to real distance matches ->
// for each match, touch episode state + Neo4j evidence + known-associate
// gate -> publish once per new/retrying unscheduled episode -> commit offset.
//
// Offset commit strategy: manual, after all candidates for the message are
// processed. A crash mid-message causes Kafka to redeliver; every write in
// the pipeline (episode state, Neo4j MERGE, candidate_published gating) is
// idempotent or replay-safe by design, so redelivery is always safe.
//
// One Neo4j session for the process lifetime, not one per message: this
// consumer processes messages sequentially (no concurrent partitions
// configured), so there is no concurrency for separate sessions to isolate,
// and session creation has real overhead worth avoiding per message.
import { fileURLToPath } from 'node:url';
import { Kafka, Partitioners, type Producer } from 'kafkajs';
import { Redis } from 'ioredis';
import neo4j, { type Session } from 'neo4j-driver';
import { latLngToCell } from 'h3-js';
import { config } from './config.js';
import { findProximityCandidates } from './candidates.js';
import { filterByDistance } from './distance.js';
import { computeMidpoint } from './midpoint.js';
import { markCandidatePublished } from './episode.js';
import { evaluateProximityEncounter } from './proximityDecision.js';
import type { ProximityEntity } from './proximityEvent.js';

interface IncomingPosition {
	entity_id: string;
	entity_type: string;
	timestamp_ms: number;
	lat: number;
	lon: number;
}

function parsePosition(rawValue: string): IncomingPosition | null {
	let parsed: unknown;
	try {
		parsed = JSON.parse(rawValue);
	} catch {
		return null;
	}
	if (typeof parsed !== 'object' || parsed === null) return null;
	const p = parsed as Record<string, unknown>;
	if (
		typeof p['entity_id'] !== 'string' ||
		typeof p['entity_type'] !== 'string' ||
		typeof p['timestamp_ms'] !== 'number' ||
		typeof p['lat'] !== 'number' ||
		typeof p['lon'] !== 'number'
	) {
		return null;
	}
	return {
		entity_id: p['entity_id'],
		entity_type: p['entity_type'],
		timestamp_ms: p['timestamp_ms'],
		lat: p['lat'],
		lon: p['lon'],
	};
}

// Exported for direct testing against real Redis/Neo4j/a fake producer,
// separate from the Kafka consumer loop itself (see position-consumer and
// alert-evaluator for the same split: the wiring loop is not independently
// tested, the message-handling logic is).
export async function handlePosition(
	redis: Redis,
	session: Session,
	producer: Pick<Producer, 'send'>,
	position: IncomingPosition,
): Promise<void> {
	const liveGeoCell = latLngToCell(position.lat, position.lon, config.LIVE_H3_RESOLUTION);
	const minLastSeenMs = position.timestamp_ms - config.CANDIDATE_FRESHNESS_MS;

	const candidateIds = await findProximityCandidates(
		redis,
		position.entity_id,
		liveGeoCell,
		config.CANDIDATE_SEARCH_K,
		minLastSeenMs,
	);
	if (candidateIds.length === 0) return;

	const matches = await filterByDistance(
		redis,
		position.lat,
		position.lon,
		candidateIds,
		config.PROXIMITY_THRESHOLD_METRES,
	);

	const entityA: ProximityEntity = { id: position.entity_id, type: position.entity_type };

	for (const match of matches) {
		const entityB: ProximityEntity = { id: match.entityId, type: match.entityType };
		const { lat, lon } = computeMidpoint(position.lat, position.lon, match.lat, match.lon);

		const decision = await evaluateProximityEncounter(
			redis,
			session,
			entityA,
			entityB,
			{
				observedAtMs: position.timestamp_ms,
				distanceMetres: match.distanceMetres,
				lat,
				lon,
			},
			config.PROXIMITY_EPISODE_GAP_MS,
		);

		if (!decision.shouldPublishCandidate) continue;

		const [entityAId, entityBId] =
			entityA.id <= entityB.id ? [entityA.id, entityB.id] : [entityB.id, entityA.id];

		await producer.send({
			topic: config.CANDIDATES_TOPIC,
			messages: [
				{
					key: decision.pairKey,
					value: JSON.stringify({
						pair_key: decision.pairKey,
						entity_a_id: entityAId,
						entity_b_id: entityBId,
						episode_start_ms: decision.episodeStartMs,
						lat,
						lon,
						distance_at_detection: match.distanceMetres,
					}),
				},
			],
		});

		// Only after the publish above actually completes -- a crash or
		// throw before this line leaves candidate_published at '0' so the
		// next qualifying ping for this episode retries.
		await markCandidatePublished(redis, decision.pairKey);
	}
}

// ---- Kafka / Redis / Neo4j setup -------------------------------------------

const kafka = new Kafka({
	clientId: 'correlation-worker',
	brokers: config.KAFKA_BROKERS,
	logLevel: 0,
});

const producer = kafka.producer({ createPartitioner: Partitioners.LegacyPartitioner });
const consumer = kafka.consumer({ groupId: config.GROUP_ID });

const redis = new Redis(config.REDIS_URL);
const driver = neo4j.driver(
	config.NEO4J_URI,
	neo4j.auth.basic(config.NEO4J_USER, config.NEO4J_PASSWORD),
);
const session = driver.session();

// ---- Consumer loop ----------------------------------------------------------

async function run(): Promise<void> {
	console.info(
		{
			brokers: config.KAFKA_BROKERS,
			group: config.GROUP_ID,
			source_topic: config.SOURCE_TOPIC,
			candidates_topic: config.CANDIDATES_TOPIC,
			from_beginning: config.FROM_BEGINNING,
		},
		'correlation worker starting',
	);

	await producer.connect();
	await consumer.connect();
	await consumer.subscribe({ topic: config.SOURCE_TOPIC, fromBeginning: config.FROM_BEGINNING });

	await consumer.run({
		autoCommit: false,
		eachMessage: async ({ topic, partition, message }) => {
			const rawValue = message.value?.toString() ?? '';
			const offset = message.offset;

			const position = parsePosition(rawValue);
			if (position === null) {
				console.warn(
					{ topic, partition, offset },
					'skipping unparseable position.normalized message',
				);
			} else {
				await handlePosition(redis, session, producer, position);
			}

			// Commit AFTER processing. Crash before commit -> redeliver on
			// restart -> idempotent replay throughout handlePosition.
			await consumer.commitOffsets([
				{ topic, partition, offset: (BigInt(offset) + 1n).toString() },
			]);
		},
	});
}

async function shutdown(signal: string): Promise<void> {
	console.info({ signal }, 'shutdown initiated');
	await consumer.disconnect();
	await producer.disconnect();
	await session.close();
	await driver.close();
	await redis.quit();
	process.exit(0);
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
	process.on('SIGINT', () => {
		shutdown('SIGINT').catch(() => process.exit(1));
	});
	process.on('SIGTERM', () => {
		shutdown('SIGTERM').catch(() => process.exit(1));
	});

	run().catch((err: unknown) => {
		console.error({ err }, 'correlation worker failed');
		process.exit(1);
	});
}
