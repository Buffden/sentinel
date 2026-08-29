import { randomUUID } from 'node:crypto';
import { Kafka, Partitioners } from 'kafkajs';
import { Redis } from 'ioredis';
import { LeaderElection } from './leader.js';

// ---- Configuration ---------------------------------------------------------

const BROKERS = (process.env['KAFKA_BROKERS'] ?? 'localhost:9092').split(',');
const REDIS_URL = process.env['REDIS_URL'] ?? 'redis://localhost:6379';
const ALERTS_TOPIC = 'alerts';

// How often the leader scans all entity:live:* keys.
const SCAN_INTERVAL_MS = 30_000;

// Silence duration before an entity is declared lost.
// Applied uniformly to all entity types in v1.
const SIGNAL_LOSS_THRESHOLD_MS = 300_000; // 5 minutes

// How long before a follower retries acquiring the leader lease.
const FOLLOWER_RETRY_INTERVAL_MS = 5_000;

// ---- Kafka setup -----------------------------------------------------------

const kafka = new Kafka({
	clientId: 'alert-evaluator',
	brokers: BROKERS,
	logLevel: 0,
});

const producer = kafka.producer({
	createPartitioner: Partitioners.LegacyPartitioner,
});

// ---- Redis setup -----------------------------------------------------------

const instanceId = randomUUID();
const redis = new Redis(REDIS_URL);
const leader = new LeaderElection(redis, instanceId);

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
async function runScan(): Promise<void> {
	const nowMs = Date.now();
	let cursor = '0';
	let scanned = 0;
	let alerted = 0;

	do {
		const [nextCursor, keys] = await redis.scan(cursor, 'MATCH', 'entity:live:*', 'COUNT', 100);
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
			if (nowMs - lastSeenMs < SIGNAL_LOSS_THRESHOLD_MS) continue;

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
				'dark_since_ms', String(darkSinceMs),
				'signal_loss_alert_id', alertId,
				'composite_issued', '0',
			);

			// All last_known_* fields are read from the live hash at scan time.
			// Empty string values (Redis stores null as '') become null in the payload.
			const parseField = (v: string | undefined): number | null =>
				v && v !== '' ? Number(v) : null;

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
				topic: ALERTS_TOPIC,
				messages: [{ key: entityId, value: JSON.stringify(alert) }],
			});

			alerted++;
			console.info({ instanceId, entityId, alertId, darkSinceMs }, 'signal loss detected');
		}
	} while (cursor !== '0');

	console.info({ instanceId, scanned, alerted }, 'scan complete');
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
		await sleep(SCAN_INTERVAL_MS, ac.signal);
	}

	// Stop the renewal timer now that the loop has exited cleanly.
	leader.stopRenewal();
}

// ---- Main ------------------------------------------------------------------

async function main(): Promise<void> {
	console.info({ instanceId }, 'alert evaluator starting');

	await producer.connect();
	console.info({ instanceId }, 'kafka producer connected');

	// Single loop: try to acquire, run as leader, then fall back to polling.
	while (true) {
		const acquired = await leader.tryAcquire();
		if (acquired) {
			await runLeaderSession();
		} else {
			console.info({ instanceId }, 'running as follower — waiting for leader lease');
		}
		await sleep(FOLLOWER_RETRY_INTERVAL_MS);
	}
}

async function shutdown(): Promise<void> {
	console.info({ instanceId }, 'shutting down');
	leader.stopRenewal();
	await leader.release();
	await producer.disconnect();
	await redis.quit();
}

process.on('SIGINT', () => { shutdown().then(() => process.exit(0)); });
process.on('SIGTERM', () => { shutdown().then(() => process.exit(0)); });

main().catch((err) => {
	console.error({ err }, 'fatal error');
	process.exit(1);
});

// Resolves after `ms` milliseconds, or immediately if the signal is already
// aborted or fires before the timer expires.
function sleep(ms: number, signal?: AbortSignal): Promise<void> {
	return new Promise<void>((resolve) => {
		if (signal?.aborted) { resolve(); return; }
		const timer = setTimeout(resolve, ms);
		signal?.addEventListener('abort', () => { clearTimeout(timer); resolve(); }, { once: true });
	});
}
