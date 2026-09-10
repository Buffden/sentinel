import { randomUUID } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { Kafka, Partitioners, type Consumer } from 'kafkajs';
import { Redis } from 'ioredis';
import { LeaderElection } from './leader.js';
import { config } from './config.js';
import {
	CandidateDecisionConflictError,
	CompositeFinalizeInvariantError,
	buildCompositeAlert,
	claimCompositeEpisode,
	finalizeCompositeEpisode,
	readCandidateDecision,
	releaseCompositeClaim,
	resolveCompositeEligibility,
	writeCandidateDecisionIfAbsent,
} from './composite.js';
import type { CandidateDecision, CompositeCandidateDecision } from './composite.js';

// ---- Kafka setup -----------------------------------------------------------

const kafka = new Kafka({
	clientId: 'alert-evaluator',
	brokers: config.KAFKA_BROKERS,
	logLevel: 0,
});

export const producer = kafka.producer({
	createPartitioner: Partitioners.LegacyPartitioner,
});

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
//
// entity_a_id is always canonicalized (lexicographically smaller) by the
// Correlation Worker, so UNSCHEDULED_PROXIMITY's primary/counterparty
// assignment below is deterministic across redelivery. COMPOSITE's primary
// entity is whichever pair member's loss episode qualified (composite.ts's
// buildCompositeAlert), not always entity_a_id.
async function publishUnscheduledProximityAlert(
	candidate: ProximityCandidateMessage,
): Promise<void> {
	const alertId = `${candidate.pair_key}:UNSCHEDULED_PROXIMITY:${candidate.episode_start_ms}`;

	// entity_type isn't on the candidate message -- entity:live:* is the one
	// place last-known entity facts live. Default to '' (never null) to
	// match the alerts table's NOT NULL entity_type column.
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

// DATA_MODEL.md's composite claim and decision protocol: builds and
// publishes via CP4's buildCompositeAlert (entityType/detectedAtMs/
// correlationWindowMs supplied here, not read inside the pure builder),
// then FINALIZEs the decision's own episode. Called for a fresh COMPOSITE
// decision, an existing-decision replay, and a post-conflict adopted
// decision alike -- FINALIZE must run in all three cases, idempotent if an
// earlier attempt already reached it (CP3B).
async function publishCompositeAlert(
	decision: CompositeCandidateDecision,
	candidate: ProximityCandidateMessage,
): Promise<void> {
	const entityType =
		(await redis.hget(`entity:live:${decision.selected_entity_id}`, 'entity_type')) ?? '';

	const alert = buildCompositeAlert(
		decision,
		candidate,
		entityType,
		Date.now(),
		config.COMPOSITE_CORRELATION_WINDOW_MS,
	);

	await producer.send({
		topic: config.ALERTS_TOPIC,
		messages: [{ key: candidate.pair_key, value: JSON.stringify(alert) }],
	});

	// Pre-CP5A(c): NO_EPISODE and NOT_CLAIMED are not equivalent. NO_EPISODE
	// means the key is gone entirely -- nothing else can reuse or corrupt
	// it, safe to warn and let the input offset commit. NOT_CLAIMED means
	// the episode still exists but ownership no longer matches this
	// candidate_id -- an invariant violation; the caller must not commit.
	const result = await finalizeCompositeEpisode(
		redis,
		decision.selected_entity_id,
		decision.dark_since_ms,
		decision.candidate_id,
	);

	if (result === 'NOT_CLAIMED') {
		throw new CompositeFinalizeInvariantError(
			decision.selected_entity_id,
			decision.dark_since_ms,
			decision.candidate_id,
		);
	}
	if (result === 'NO_EPISODE') {
		console.warn(
			{ instanceId, candidateId: decision.candidate_id, entityId: decision.selected_entity_id },
			'FINALIZE found no retained episode -- already expired; the published alert stands as the only evidence',
		);
	}

	console.info(
		{ instanceId, alertId: alert.alert_id, pairKey: candidate.pair_key },
		'composite alert emitted',
	);
}

async function publishDecision(
	decision: CandidateDecision,
	candidate: ProximityCandidateMessage,
): Promise<void> {
	if (decision.decision === 'UNSCHEDULED_PROXIMITY') {
		await publishUnscheduledProximityAlert(candidate);
	} else {
		await publishCompositeAlert(decision, candidate);
	}
}

export async function handleProximityCandidate(
	candidate: ProximityCandidateMessage,
): Promise<void> {
	const candidateId = `${candidate.pair_key}:${candidate.episode_start_ms}`;

	// Pre-CP5A: checked first, before CP2 ever runs. A candidate that
	// already has a decision replays it rather than re-resolving eligibility
	// against Redis state that may have changed since.
	const existing = await readCandidateDecision(redis, candidateId);
	if (existing) {
		await publishDecision(existing, candidate);
		return;
	}

	const winner = await resolveCompositeEligibility(
		redis,
		candidate.entity_a_id,
		candidate.entity_b_id,
		candidate.episode_start_ms,
		config.COMPOSITE_CORRELATION_WINDOW_MS,
	);

	// Pre-CP5A(a): any CLAIM failure, for any reason (CLAIMED_BY_OTHER,
	// ALREADY_ISSUED, or NO_EPISODE -- claimCompositeEpisode does not
	// distinguish them, since all three collapse to this identical
	// outcome), decides UNSCHEDULED_PROXIMITY. No fallback to the other pair
	// member: the deterministic tie-break already picked a single winner
	// over a Redis snapshot.
	let decision: CandidateDecision;
	if (winner) {
		const claimed = await claimCompositeEpisode(
			redis,
			winner.entity_id,
			winner.dark_since_ms,
			candidateId,
		);
		decision = claimed
			? {
					decision: 'COMPOSITE',
					candidate_id: candidateId,
					selected_entity_id: winner.entity_id,
					loss_source: winner.source,
					dark_since_ms: winner.dark_since_ms,
					signal_loss_alert_id: winner.signal_loss_alert_id,
					resumed_at_ms: winner.resumed_at_ms,
				}
			: { decision: 'UNSCHEDULED_PROXIMITY', candidate_id: candidateId };
	} else {
		decision = { decision: 'UNSCHEDULED_PROXIMITY', candidate_id: candidateId };
	}

	let stored: CandidateDecision;
	try {
		stored = await writeCandidateDecisionIfAbsent(redis, decision);
	} catch (err) {
		if (!(err instanceof CandidateDecisionConflictError)) throw err;

		// Pre-CP5A(b): a decision-write conflict after this process's own
		// CLAIM already mutated Redis. Release only the locally-acquired
		// stray claim -- never anything this process did not itself claim --
		// then adopt the canonical decision and process it exactly like a
		// normal existing-decision replay. Release is best-effort: its
		// result does not gate adopting the canonical decision.
		if (decision.decision === 'COMPOSITE') {
			const released = await releaseCompositeClaim(
				redis,
				decision.selected_entity_id,
				decision.dark_since_ms,
				candidateId,
			);
			if (!released) {
				console.warn(
					{ instanceId, candidateId, entityId: decision.selected_entity_id },
					'could not release stray composite claim after losing a decision-write conflict',
				);
			}
		}

		stored = err.existing;
	}

	await publishDecision(stored, candidate);
}

// ---- Candidate consumer session --------------------------------------------

// ADR-005: only the current lease holder joins/polls the Alert Evaluator's
// candidate consumer group -- named around "candidate consumer" rather than
// "proximity consumer" because ADR-005 scopes this lifecycle to every
// candidate topic the evaluator consumes (deviation.candidates and
// proximity.candidates). deviation.candidates consumption does not exist yet,
// so this only subscribes to what is real today; it is not built opportunistically.
//
// groupId is a parameter rather than reading config.GROUP_ID directly so
// tests can join a disposable group against the real broker without
// touching the production alert-evaluator group.
export interface CandidateConsumerSession {
	consumer: Consumer;
	// Idempotent: safe to call from the lease-loss callback, a normal session
	// exit, and shutdown() without coordinating who calls it first -- every
	// caller converges on the same in-flight disconnect() promise rather than
	// racing a second disconnect.
	stop: () => Promise<void>;
}

export async function startCandidateConsumerSession(
	groupId: string,
): Promise<CandidateConsumerSession> {
	const consumer = kafka.consumer({ groupId });

	await consumer.connect();
	await consumer.subscribe({
		topic: config.PROXIMITY_CANDIDATES_TOPIC,
		fromBeginning: config.FROM_BEGINNING,
	});
	await consumer.run({
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

			await consumer.commitOffsets([
				{ topic, partition, offset: (BigInt(offset) + 1n).toString() },
			]);
		},
	});

	let stopPromise: Promise<void> | null = null;
	const stop = (): Promise<void> => {
		if (!stopPromise) {
			stopPromise = consumer.disconnect();
		}
		return stopPromise;
	};

	return { consumer, stop };
}

// ---- Leader session --------------------------------------------------------

// Set only while this instance holds the lease; null for a follower or
// between terms. shutdown() reads these to tear down an in-progress
// leadership term cleanly instead of leaving the process mid-session.
let activeSession: CandidateConsumerSession | null = null;
let activeSessionAbort: AbortController | null = null;

// Each time this instance becomes leader it gets a fresh AbortController and
// a fresh candidate consumer session -- both scoped to this leadership term,
// never reused across acquisitions.
//
// activeSession/activeSessionAbort are module-level so shutdown() (a
// separate top-level function, invoked from a signal handler that runs
// concurrently with this loop) can reach the current term's session without
// this function passing anything out. session.stop() is idempotent, so the
// lease-loss callback below, this function's own teardown, and shutdown()
// can all call it without coordinating who goes first.
//
// The lease-loss callback here does two things immediately, not on the scan
// loop's next tick: it aborts the controller (unblocking the sleeping scan
// loop) AND initiates session.stop() (leaving the Kafka group). ADR-005:
// followers do not participate, and a former leader begins leaving the
// candidate consumer group as soon as lease loss is detected -- not once
// this loop happens to notice. Kafka's own group rebalance, not this
// callback, is what fences any message already in flight when the lease
// was lost; deterministic alert_id plus idempotent persistence downstream
// remains the correctness backstop during that brief overlap.
async function runLeaderSession(): Promise<void> {
	const ac = new AbortController();
	activeSessionAbort = ac;

	const session = await startCandidateConsumerSession(config.GROUP_ID);
	activeSession = session;
	console.info({ instanceId }, 'joined candidate consumer group');

	leader.startRenewal(() => {
		console.warn({ instanceId }, 'lease lost — leaving candidate consumer group');
		ac.abort();
		void session.stop();
	});

	console.info({ instanceId }, 'acquired leader lease — starting scan loop');

	try {
		while (!ac.signal.aborted) {
			await runScan();
			await sleep(config.SCAN_INTERVAL_MS, ac.signal);
		}
	} finally {
		leader.stopRenewal();
		await session.stop();
		console.info({ instanceId }, 'left candidate consumer group');
		activeSession = null;
		activeSessionAbort = null;
	}
}

// ---- Main ------------------------------------------------------------------

async function main(): Promise<void> {
	console.info({ instanceId }, 'alert evaluator starting');

	await producer.connect();
	console.info({ instanceId }, 'kafka producer connected');

	// ADR-005: the candidate consumer group is joined only inside
	// runLeaderSession(), on lease acquisition -- not here. A follower must
	// never become a member of the alert-evaluator Kafka consumer group.
	//
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

// Mirrors lease loss: if this instance currently holds a leadership term,
// abort its scan loop and leave the candidate consumer group before
// releasing the lease -- rather than exiting mid-session and leaving Kafka
// to detect the departure via session timeout.
async function shutdown(): Promise<void> {
	console.info({ instanceId }, 'shutting down');
	activeSessionAbort?.abort();
	if (activeSession) {
		await activeSession.stop();
	}
	leader.stopRenewal();
	await leader.release();
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
