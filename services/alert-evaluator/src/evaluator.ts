import { randomUUID } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { Kafka, Partitioners } from 'kafkajs';
import { Redis } from 'ioredis';
import { LeaderElection } from './leader.js';
import { config } from './config.js';

// ---- Kafka setup -----------------------------------------------------------

const kafka = new Kafka({
	clientId: 'alert-evaluator',
	brokers: config.KAFKA_BROKERS,
	logLevel: 0,
});

export const producer = kafka.producer({
	createPartitioner: Partitioners.LegacyPartitioner,
});

// Kafka's own consumer-group partition assignment already guarantees each
// proximity.candidates message is processed by exactly one instance at a
// time -- unlike the signal-loss scan (driven by a timer, not a partition),
// this consumer needs no additional Redis-lease leader election on top.
export const proximityConsumer = kafka.consumer({ groupId: config.GROUP_ID });

// ---- Redis setup -----------------------------------------------------------

const instanceId = randomUUID();
export const redis = new Redis(config.REDIS_URL);
const leader = new LeaderElection(
	redis,
	instanceId,
	config.LEADER_KEY,
	config.LEADER_LEASE_TTL_MS,
	config.LEADER_RENEWAL_INTERVAL_MS,
);

// ---- Signal-loss scan ------------------------------------------------------

// Scan all entity:live:* keys and emit a SIGNAL_LOSS alert for any entity
// that has been silent beyond the threshold and has no existing episode gate.
//
// Uses a consistent nowMs across the whole scan so that entities that cross
// the threshold mid-scan are handled uniformly on the next tick.
//
// Redis SCAN is cursor-based: results are returned in batches. The same key
// may appear more than once across cursor iterations (safe — the gate check
// absorbs duplicate evaluations). Keys added or removed during iteration may
// or may not appear in this scan; the next tick catches them.
//
// Write order per detected entity:
//   1. HSET alert-state:{entity_id}   — episode gate; prevents re-emission
//   2. producer.send to alerts topic  — delivers alert downstream
//
// Gate is written first. A crash between 1 and 2 means the entity misses an
// alert for this episode. The alternative (Kafka first) would re-emit on
// every scan tick until restart. Gate-first is the accepted trade-off.
export async function runScan(): Promise<void> {
	const nowMs = Date.now();
	let cursor = '0';
	let scanned = 0;
	let alerted = 0;

	do {
		const [nextCursor, keys] = await redis.scan(
			cursor,
			'MATCH',
			'entity:live:*',
			'COUNT',
			config.REDIS_SCAN_COUNT,
		);
		cursor = nextCursor;

		for (const key of keys) {
			// key shape: "entity:live:{entity_id}"
			const entityId = key.slice('entity:live:'.length);
			scanned++;

			const entity = await redis.hgetall(key);
			if (!entity || Object.keys(entity).length === 0) continue;

			// Aircraft on the ground legitimately power down transponders.
			// Unknown ground state (empty string) is treated conservatively: included.
			if (entity['on_ground'] === 'true') continue;

			// last_seen_ms is set from source event time by the Position Consumer.
			// Missing or empty means the entity hash exists but has no accepted position.
			const lastSeenMsStr = entity['last_seen_ms'];
			if (!lastSeenMsStr || lastSeenMsStr === '') continue;

			const lastSeenMs = Number(lastSeenMsStr);
			if (nowMs - lastSeenMs < config.SIGNAL_LOSS_THRESHOLD_MS) continue;

			// Episode gate: if alert-state exists, this dark period is already alerted.
			const gateExists = await redis.exists(`alert-state:${entityId}`);
			if (gateExists) continue;

			// dark_since_ms anchors the episode. It is the last_seen_ms at detection
			// time — source event time, not processing time. Two distinct dark periods
			// will have different last_seen_ms values and therefore different alert_ids.
			const darkSinceMs = lastSeenMs;
			const alertId = `${entityId}:SIGNAL_LOSS:${darkSinceMs}`;

			// Write the episode gate before producing to Kafka.
			// composite_issued = '0' is consumed by Phase 06 (composite correlation).
			await redis.hset(
				`alert-state:${entityId}`,
				'dark_since_ms',
				String(darkSinceMs),
				'signal_loss_alert_id',
				alertId,
				'composite_issued',
				'0',
			);

			// All last_known_* fields are read from the live hash at scan time.
			// Empty string values (Redis stores null as '') become null in the payload.
			const parseField = (v: string | undefined): number | null =>
				v && v !== '' ? Number(v) : null;

			const callsign = entity['callsign'] && entity['callsign'] !== '' ? entity['callsign'] : null;

			const alert = {
				alert_id: alertId,
				entity_id: entityId,
				entity_type: entity['entity_type'] ?? '',
				alert_type: 'SIGNAL_LOSS',
				priority: 'STANDARD',
				status: 'NEW',
				// detected_at_ms is processing time — the moment the scan noticed the silence.
				// dark_since_ms in payload is source event time — the last known position timestamp.
				detected_at_ms: nowMs,
				payload: {
					dark_since_ms: darkSinceMs,
					callsign,
					last_known_lat: parseField(entity['lat']),
					last_known_lon: parseField(entity['lon']),
					last_known_altitude_m: parseField(entity['altitude_m']),
					last_known_speed_mps: parseField(entity['speed_mps']),
					last_known_course_deg: parseField(entity['course_deg']),
				},
			};

			// Keyed by entity_id so all alerts for the same entity land on the same
			// partition, preserving order for downstream consumers.
			await producer.send({
				topic: config.ALERTS_TOPIC,
				messages: [{ key: entityId, value: JSON.stringify(alert) }],
			});

			alerted++;
			console.info({ instanceId, entityId, alertId, darkSinceMs }, 'signal loss detected');
		}
	} while (cursor !== '0');

	console.info({ instanceId, scanned, alerted }, 'scan complete');
}

// ---- Proximity candidate handling -------------------------------------------

export interface ProximityCandidateMessage {
	pair_key: string;
	entity_a_id: string;
	entity_b_id: string;
	episode_start_ms: number;
	lat: number;
	lon: number;
	distance_at_detection: number;
}

function parseProximityCandidate(rawValue: string): ProximityCandidateMessage | null {
	let parsed: unknown;
	try {
		parsed = JSON.parse(rawValue);
	} catch {
		return null;
	}
	if (typeof parsed !== 'object' || parsed === null) return null;
	const p = parsed as Record<string, unknown>;
	if (
		typeof p['pair_key'] !== 'string' ||
		typeof p['entity_a_id'] !== 'string' ||
		typeof p['entity_b_id'] !== 'string' ||
		typeof p['episode_start_ms'] !== 'number' ||
		typeof p['lat'] !== 'number' ||
		typeof p['lon'] !== 'number' ||
		typeof p['distance_at_detection'] !== 'number'
	) {
		return null;
	}
	return p as unknown as ProximityCandidateMessage;
}

// proximity.candidates already means "exact proximity confirmed, new
// episode, no KNOWN_ASSOCIATE relationship" -- the Correlation Worker did
// that work before publishing, so this does not repeat a Neo4j check.
// Composite correlation (checking alert-state/recent-loss for a qualifying
// signal-loss episode on either entity) is a later phase; every candidate
// becomes UNSCHEDULED_PROXIMITY here.
//
// entity_a_id is always the alert's primary entity_id and entity_b_id the
// counterparty -- both are already canonically ordered by the Correlation
// Worker, so this assignment is deterministic across redelivery.
export async function handleProximityCandidate(
	candidate: ProximityCandidateMessage,
): Promise<void> {
	const alertId = `${candidate.pair_key}:UNSCHEDULED_PROXIMITY:${candidate.episode_start_ms}`;

	// entity_type isn't on the candidate message -- entity:live:* is already
	// read here for signal-loss, for the same reason: it's the one place
	// last-known entity facts live. Default to '' (never null) to match the
	// alerts table's NOT NULL entity_type column, same as the signal-loss path.
	const entityType =
		(await redis.hget(`entity:live:${candidate.entity_a_id}`, 'entity_type')) ?? '';

	const alert = {
		alert_id: alertId,
		entity_id: candidate.entity_a_id,
		counterparty_entity_id: candidate.entity_b_id,
		entity_type: entityType,
		alert_type: 'UNSCHEDULED_PROXIMITY',
		priority: 'STANDARD',
		status: 'NEW',
		detected_at_ms: Date.now(),
		payload: {
			pair_key: candidate.pair_key,
			counterparty_entity_id: candidate.entity_b_id,
			lat: candidate.lat,
			lon: candidate.lon,
			distance_metres: candidate.distance_at_detection,
			episode_start_ms: candidate.episode_start_ms,
		},
	};

	await producer.send({
		topic: config.ALERTS_TOPIC,
		messages: [{ key: candidate.pair_key, value: JSON.stringify(alert) }],
	});

	console.info(
		{ instanceId, alertId, pairKey: candidate.pair_key },
		'unscheduled proximity alert emitted',
	);
}

// ---- Leader session --------------------------------------------------------

// Each time this instance becomes leader it gets a fresh AbortController.
// When the lease is lost (or revoked), the controller is aborted, which
// unblocks the sleeping loop immediately and stops it before the next tick.
// This prevents the previous-session loop from waking up and running
// alongside a newly started session.
async function runLeaderSession(): Promise<void> {
	const ac = new AbortController();

	leader.startRenewal(() => {
		console.warn({ instanceId }, 'lease lost — aborting leader session');
		ac.abort();
	});

	console.info({ instanceId }, 'acquired leader lease — starting scan loop');

	while (!ac.signal.aborted) {
		await runScan();
		await sleep(config.SCAN_INTERVAL_MS, ac.signal);
	}

	// Stop the renewal timer now that the loop has exited cleanly.
	leader.stopRenewal();
}

// ---- Main ------------------------------------------------------------------

async function main(): Promise<void> {
	console.info({ instanceId }, 'alert evaluator starting');

	await producer.connect();
	console.info({ instanceId }, 'kafka producer connected');

	await proximityConsumer.connect();
	await proximityConsumer.subscribe({
		topic: config.PROXIMITY_CANDIDATES_TOPIC,
		fromBeginning: config.FROM_BEGINNING,
	});
	// consumer.run() resolves once the fetch loop has started, not when
	// message processing finishes -- messages continue arriving via
	// eachMessage in the background, so this does not block the leader loop
	// started below.
	await proximityConsumer.run({
		autoCommit: false,
		eachMessage: async ({ topic, partition, message }) => {
			const rawValue = message.value?.toString() ?? '';
			const offset = message.offset;

			const candidate = parseProximityCandidate(rawValue);
			if (candidate === null) {
				console.warn(
					{ instanceId, topic, partition, offset },
					'skipping unparseable proximity.candidates message',
				);
			} else {
				await handleProximityCandidate(candidate);
			}

			await proximityConsumer.commitOffsets([
				{ topic, partition, offset: (BigInt(offset) + 1n).toString() },
			]);
		},
	});
	console.info({ instanceId }, 'proximity candidates consumer running');

	// Single loop: try to acquire, run as leader, then fall back to polling.
	while (true) {
		const acquired = await leader.tryAcquire();
		if (acquired) {
			await runLeaderSession();
		} else {
			console.info({ instanceId }, 'running as follower — waiting for leader lease');
		}
		await sleep(config.FOLLOWER_RETRY_INTERVAL_MS);
	}
}

async function shutdown(): Promise<void> {
	console.info({ instanceId }, 'shutting down');
	leader.stopRenewal();
	await leader.release();
	await proximityConsumer.disconnect();
	await producer.disconnect();
	await redis.quit();
}

// Only run the service when this file is executed directly (`npm run evaluator`),
// not when imported — e.g. by an integration test importing runScan against a
// real Redis and Kafka broker.
if (process.argv[1] === fileURLToPath(import.meta.url)) {
	process.on('SIGINT', () => {
		shutdown().then(() => process.exit(0));
	});
	process.on('SIGTERM', () => {
		shutdown().then(() => process.exit(0));
	});

	main().catch((err) => {
		console.error({ err }, 'fatal error');
		process.exit(1);
	});
}

// Resolves after `ms` milliseconds, or immediately if the signal is already
// aborted or fires before the timer expires.
function sleep(ms: number, signal?: AbortSignal): Promise<void> {
	return new Promise<void>((resolve) => {
		if (signal?.aborted) {
			resolve();
			return;
		}
		const timer = setTimeout(resolve, ms);
		signal?.addEventListener(
			'abort',
			() => {
				clearTimeout(timer);
				resolve();
			},
			{ once: true },
		);
	});
}
