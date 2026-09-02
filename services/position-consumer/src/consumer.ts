// Position Consumer: normalize raw telemetry, persist to TimescaleDB,
// maintain Redis live state, and publish downstream.
//
// Consumer group: position-consumer (canonical; do not change without ADR).
//
// Write order for every Kafka message:
//   1. INSERT raw_events (required before offset commit on every path)
//   2. Branch by normalization result:
//        parse_error / missing_entity_id → DLQ publish → commit
//        no_position                     → warn log → commit
//        valid position                  → INSERT position_history (with H3 history cell)
//                                          → HGET old live_geo_cell
//                                          → updateLiveState (Redis, includes live_geo_cell)
//                                          → if accepted: updateGeoCell + publishPositionUpdate
//                                          → if accepted: clearSignalLossEpisode (recent-loss + del alert-state)
//                                          → publishNormalized (position.normalized, always)
//                                          → commit
//
// DLQ publish failures block offset commit.
//   A transient broker error on the DLQ path propagates out of handleMessage.
//   The offset is NOT committed. Kafka redelivers the message on the next
//   consumer start. The raw_events insert is idempotent on replay.
//
// raw_events availability dependency:
//   Successful raw_events archival is required before offset commit. If the
//   INSERT fails, the error propagates out and the offset is not committed.
//   Kafka redelivers. The INSERT is idempotent via
//   ON CONFLICT (source_topic, source_partition, source_offset) DO NOTHING.
//
// Redis live state — monotonic guard:
//   entity:live:{entity_id} is updated via a Lua script that atomically checks
//   last_seen_ms before writing. A stale or equal timestamp is rejected without
//   touching the hash. A newer timestamp updates all fields together (including
//   live_geo_cell) and resets the 24h TTL.
//
// Redis geo-cell sorted sets:
//   geo-cell:{live_geo_cell} holds member=entity_id, score=last_seen_ms.
//   The old live_geo_cell is read via HGET BEFORE the Lua hash write so we know
//   which sorted set to ZREM from. Sorted set updates happen only when the Lua
//   guard accepts (i.e. the incoming event is newer). Stale events skip both
//   ZREM and ZADD.
//   The ZREM→ZADD pair is not atomic with the hash write. A brief gap where the
//   entity is absent from the index is acceptable — the Correlation Worker's
//   freshness score lower bound makes a transiently missing candidate safe.
//
// Offset commit strategy: manual (autoCommit: false).
//   The commit is the LAST action for every message. A crash before
//   the commit causes Kafka to redeliver the message on restart.
//   All downstream writes are idempotent so replay is always safe.
//
// Testing malformed records (DLQ routing):
//   Inject fresh test records for DLQ experiments. Records already
//   committed will not be redelivered unless the group offset is reset:
//     rpk group seek position-consumer --to <offset> --topics adsb.raw
//
// publishNormalized failure handling:
//   A failed position.normalized publish propagates out of handleMessage.
//   The offset is NOT committed. Kafka redelivers the message on restart.
//   All prior writes (raw_events, position_history, Redis) are idempotent on replay.
//   This is intentional: position.normalized is a canonical output of the Position
//   Consumer. Downstream services (Deviation Detector, Correlation Worker) consume
//   nothing else. Dropping a publish silently would permanently lose the event for
//   those services.
//
// publishPositionUpdate:
//   Called ONLY when the Redis live-state write was accepted (monotonically newer
//   event). Stale events must not publish to the live-map channel — doing so would
//   move an entity backward on the UI.
//   Fire-and-forget: failure is logged but does not rethrow — offset committed
//   regardless.
//
// publishNormalized:
//   Called for ALL valid normalized positions, including stale ones. Downstream
//   detectors (Deviation Detector, Correlation Worker) evaluate every event against
//   reference geometry or episode state — they do not rely on Redis monotonic order.
//   Failure propagates — offset not committed; Kafka redelivers; all prior writes
//   replay idempotently.
//   position.normalized is at-least-once: a crash between publish and offset commit
//   causes redelivery and a duplicate publish. Downstream consumers must tolerate
//   duplicates.

import { hostname } from 'os';
import { Kafka, Partitioners } from 'kafkajs';
import pg from 'pg';
import { Redis } from 'ioredis';
import { latLngToCell } from 'h3-js';
import { normalizeAdsbRaw, type NormalizedPosition } from './normalize.js';
import { config } from './config.js';

const { Pool } = pg;

// Stable identifier for this consumer instance, used in DLQ records so
// operators can trace which consumer instance rejected a message.
const CONSUMER_ID = `${hostname()}-${process.pid}`;

// ---- Kafka setup -----------------------------------------------------------

const kafka = new Kafka({
	clientId: 'position-consumer',
	brokers: config.KAFKA_BROKERS,
	logLevel: 0,
});

const consumer = kafka.consumer({ groupId: config.GROUP_ID });

// Producer for publishing DLQ records.
// LegacyPartitioner for consistent kafkajs v2 behaviour.
const producer = kafka.producer({
	createPartitioner: Partitioners.LegacyPartitioner,
});

// ---- Database setup --------------------------------------------------------

const pool = new Pool({ connectionString: config.PG_URL, max: config.PG_POOL_MAX });

// ---- Redis setup -----------------------------------------------------------

const redis = new Redis(config.REDIS_URL, {
	// Disable ioredis auto-reconnect logging noise on clean shutdown.
	lazyConnect: false,
	enableReadyCheck: true,
});

// Lua script: monotonic guard for entity:live:{entity_id}.
//
// Atomically checks last_seen_ms before writing. Redis executes Lua scripts
// as a single unit — no other command can interleave between the check and
// the HSET. This prevents two concurrent consumer instances from racing.
//
// Returns 0 if the incoming timestamp is stale (current >= incoming).
// Returns 1 if the write was accepted and TTL was refreshed.
//
// KEYS[1]  = entity:live:{entity_id}
// ARGV[1]  = incoming last_seen_ms (milliseconds, as string)
// ARGV[2]  = TTL in seconds
// ARGV[3..] = flat field/value pairs for HSET
const LIVE_STATE_LUA = `
local current = redis.call('HGET', KEYS[1], 'last_seen_ms')
if current and tonumber(current) >= tonumber(ARGV[1]) then
	return 0
end
local fields = {}
for i = 3, #ARGV do
	fields[#fields + 1] = ARGV[i]
end
redis.call('HSET', KEYS[1], unpack(fields))
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[2]))
return 1
`;

// ---- H3 geo-cell computation -----------------------------------------------

function computeH3Cells(
	lat: number,
	lon: number,
): {
	history_geo_cell: string;
	live_geo_cell: string;
} {
	return {
		history_geo_cell: latLngToCell(lat, lon, config.HISTORY_H3_RESOLUTION),
		live_geo_cell: latLngToCell(lat, lon, config.LIVE_H3_RESOLUTION),
	};
}

// ---- Logging ---------------------------------------------------------------

function log(
	level: 'info' | 'warn' | 'error',
	message: string,
	extra?: Record<string, unknown>,
): void {
	process.stdout.write(
		JSON.stringify({
			timestamp: new Date().toISOString(),
			level,
			service: 'position-consumer',
			message,
			...extra,
		}) + '\n',
	);
}

// ---- Database writes -------------------------------------------------------

// Archive the raw Kafka record. Called on every message, before branching.
//
// payload must be a serialized JSON string suitable for the ::jsonb cast:
//   valid JSON source   → pass rawValue directly (it is a JSON object string)
//   parse_error source  → pass JSON.stringify(rawValue) (wraps the invalid
//                         string in quotes, producing a JSONB string scalar)
//
// source_offset is passed as a string and cast via ::bigint to avoid JS
// number precision loss on large offset values.
async function writeRawEvent(
	payload: string,
	entityId: string | null,
	source: string,
	provider: string | null,
	topic: string,
	partition: number,
	offset: string,
	sourceEventTime: Date | null,
): Promise<void> {
	await pool.query(
		`INSERT INTO raw_events (
			entity_id, source, provider,
			source_topic, source_partition, source_offset,
			received_at, source_event_time, payload
		) VALUES ($1, $2, $3, $4, $5, $6::bigint, NOW(), $7, $8::jsonb)
		ON CONFLICT (source_topic, source_partition, source_offset) DO NOTHING`,
		[entityId, source, provider, topic, partition, offset, sourceEventTime, payload],
	);
}

// Write the canonical position to position_history.
// Called only for valid normalized positions (not for parse_error / no_position).
//
// observed_at is derived from source event time, not processing time.
// history_geo_cell is H3 at HISTORY_H3_RESOLUTION; stored as $30 to avoid
// renumbering the existing $1-$29 parameters.
// ON CONFLICT (entity_id, observed_at) DO NOTHING: replay-safe idempotency.
async function writePositionHistory(
	position: NormalizedPosition,
	historyGeoCell: string,
): Promise<void> {
	const observedAt = new Date(position.timestamp_ms);
	await pool.query(
		`INSERT INTO position_history (
			entity_id, entity_type, observed_at, timestamp_ms, geo_cell,
			lat, lon, altitude_m, source, provider,
			baro_altitude_m, geo_altitude_m, speed_mps, course_deg, heading_deg,
			vertical_rate_mps, on_ground, last_contact_ms, navigation_status, rate_of_turn,
			callsign, entity_subtype, provider_category, squawk, spi, position_source,
			position_accuracy, destination, eta, draught_m
		) VALUES (
			$1, $2, $3, $4, $30,
			$5, $6, $7, $8, $9,
			$10, $11, $12, $13, $14,
			$15, $16, $17, $18, $19,
			$20, $21, $22, $23, $24, $25,
			$26, $27, $28, $29
		)
		ON CONFLICT (entity_id, observed_at) DO NOTHING`,
		[
			position.entity_id,
			position.entity_type,
			observedAt,
			position.timestamp_ms,
			position.lat,
			position.lon,
			position.altitude_m,
			position.source,
			position.provider,
			position.baro_altitude_m,
			position.geo_altitude_m,
			position.speed_mps,
			position.course_deg,
			position.heading_deg,
			position.vertical_rate_mps,
			position.on_ground,
			position.last_contact_ms,
			position.navigation_status,
			position.rate_of_turn,
			position.callsign,
			position.entity_subtype,
			position.provider_category,
			position.squawk,
			position.spi,
			position.position_source,
			position.position_accuracy,
			position.destination,
			position.eta,
			position.draught_m,
			historyGeoCell,
		],
	);
}

// ---- Redis live state ------------------------------------------------------

// Update entity:live:{entity_id} with the monotonic Lua guard.
//
// Returns true if the hash was written, false if the event was stale.
// A stale result is logged at warn but is not an error — out-of-order
// delivery is normal in at-least-once Kafka pipelines.
//
// All fields are written as strings — Redis stores everything as strings
// regardless of the declared type. null values are stored as '' so HGET
// always returns a string rather than nil.
async function updateLiveState(
	position: NormalizedPosition,
	liveGeoCell: string,
): Promise<boolean> {
	const key = `entity:live:${position.entity_id}`;

	// Flatten all fields into the ARGV[3..] pairs expected by the Lua script.
	const fields = [
		'last_seen_ms',
		String(position.timestamp_ms),
		'entity_type',
		position.entity_type,
		'lat',
		String(position.lat),
		'lon',
		String(position.lon),
		'altitude_m',
		position.altitude_m != null ? String(position.altitude_m) : '',
		'speed_mps',
		position.speed_mps != null ? String(position.speed_mps) : '',
		'course_deg',
		position.course_deg != null ? String(position.course_deg) : '',
		'heading_deg',
		position.heading_deg != null ? String(position.heading_deg) : '',
		'vertical_rate_mps',
		position.vertical_rate_mps != null ? String(position.vertical_rate_mps) : '',
		'on_ground',
		position.on_ground != null ? String(position.on_ground) : '',
		'navigation_status',
		position.navigation_status ?? '',
		'callsign',
		position.callsign ?? '',
		'entity_subtype',
		position.entity_subtype ?? '',
		'provider',
		position.provider ?? '',
		'live_geo_cell',
		liveGeoCell,
	];

	const result = await redis.eval(
		LIVE_STATE_LUA,
		1,
		key,
		String(position.timestamp_ms),
		String(config.LIVE_STATE_TTL_SECONDS),
		...fields,
	);

	return result === 1;
}

// ---- Redis geo-cell sorted sets --------------------------------------------

// Maintain geo-cell:{cell_id} sorted sets: member=entity_id, score=last_seen_ms.
//
// Must only be called when the Lua guard accepted the live state write (i.e.
// the incoming event is newer). Stale events must not mutate sorted sets.
//
// oldCell is read from the hash BEFORE the Lua write. If oldCell differs from
// newCell, the entity moved to a new H3 cell and must be removed from the old
// sorted set. ZREM on a non-existent member is a no-op — safe on first ping
// or if a concurrent consumer already moved the entity.
//
// The ZREM→ZADD pair is not atomic with the hash update. A brief window where
// the entity is absent from both sorted sets is acceptable: the Correlation
// Worker's freshness score lower bound treats transiently missing candidates
// as not-yet-seen rather than as an error.
async function updateGeoCell(
	entityId: string,
	newCell: string,
	lastSeenMs: number,
	oldCell: string | null,
): Promise<void> {
	if (oldCell && oldCell !== newCell) {
		await redis.zrem(`geo-cell:${oldCell}`, entityId);
	}
	// ZADD upserts the score — updates last_seen_ms for an existing member.
	await redis.zadd(`geo-cell:${newCell}`, lastSeenMs, entityId);
}

// ---- Downstream publishing -------------------------------------------------

// Publish the canonical normalized position to position.normalized.
//
// Consumed by: Deviation Detector, Correlation Worker.
// Keyed by entity_id so all positions for the same entity land on the same
// partition, preserving per-entity ordering for stateful consumers.
//
// Called for ALL valid positions regardless of whether the Redis live state
// was accepted. A stale event (older timestamp already in Redis) still
// carries a valid position that downstream detectors need to evaluate.
//
// Failure propagates to the caller — the offset is not committed and Kafka
// redelivers. Prior writes replay idempotently.
async function publishNormalized(
	position: NormalizedPosition,
	historyGeoCell: string,
	liveGeoCell: string,
): Promise<void> {
	const payload = {
		entity_id: position.entity_id,
		entity_type: position.entity_type,
		timestamp_ms: position.timestamp_ms,
		lat: position.lat,
		lon: position.lon,
		speed_mps: position.speed_mps,
		course_deg: position.course_deg,
		heading_deg: position.heading_deg,
		source: position.source,
		provider: position.provider,
		altitude_m: position.altitude_m,
		baro_altitude_m: position.baro_altitude_m,
		geo_altitude_m: position.geo_altitude_m,
		vertical_rate_mps: position.vertical_rate_mps,
		on_ground: position.on_ground,
		last_contact_ms: position.last_contact_ms,
		navigation_status: position.navigation_status,
		rate_of_turn: position.rate_of_turn,
		callsign: position.callsign,
		entity_subtype: position.entity_subtype,
		provider_category: position.provider_category,
		squawk: position.squawk,
		spi: position.spi,
		position_source: position.position_source,
		position_accuracy: position.position_accuracy,
		destination: position.destination,
		eta: position.eta,
		draught_m: position.draught_m,
		history_geo_cell: historyGeoCell,
		live_geo_cell: liveGeoCell,
	};

	await producer.send({
		topic: config.NORMALIZED_TOPIC,
		messages: [{ key: position.entity_id, value: JSON.stringify(payload) }],
	});
}

// Publish a lightweight position snapshot to the position-updates Redis pub/sub channel.
//
// Consumed by: API instances, which fan the update out to connected WebSocket clients.
// Redis pub/sub is ephemeral: if no API instance is subscribed, the message is dropped.
// That is acceptable — WebSocket delivery is at-least-once and clients tolerate gaps.
//
// Carries only the fields the API needs to push a live map update. Full canonical
// fields are in position.normalized for services that need them.
//
// Failure is logged but does not rethrow. A missed pub/sub message is a skipped
// map tick, not a data loss — the next accepted position corrects the client's view.
async function publishPositionUpdate(
	position: NormalizedPosition,
	liveGeoCell: string,
): Promise<void> {
	const payload = JSON.stringify({
		entity_id: position.entity_id,
		entity_type: position.entity_type,
		timestamp_ms: position.timestamp_ms,
		lat: position.lat,
		lon: position.lon,
		altitude_m: position.altitude_m,
		speed_mps: position.speed_mps,
		course_deg: position.course_deg,
		callsign: position.callsign,
		live_geo_cell: liveGeoCell,
	});

	try {
		await redis.publish(config.POSITION_UPDATES_CHANNEL, payload);
	} catch (err) {
		log('error', 'position-updates pub/sub publish failed', {
			entity_id: position.entity_id,
			error: err instanceof Error ? err.message : String(err),
		});
	}
}

// ---- DLQ publish -----------------------------------------------------------

interface DlqEvent {
	raw_payload: string;
	rejection_reason: string;
	source_topic: string;
	source_partition: number;
	source_offset: string;
	consumer_id: string;
	timestamp_ms: number; // processing time — this is an audit record, not a position event
}

async function publishToDlq(event: DlqEvent): Promise<void> {
	await producer.send({
		topic: config.DLQ_TOPIC,
		messages: [{ value: JSON.stringify(event) }],
	});
}

// ---- Message handler -------------------------------------------------------

async function handleMessage(
	topic: string,
	partition: number,
	offset: string,
	rawValue: string,
): Promise<void> {
	// Derive the telemetry class from the topic name.
	// 'adsb.raw' → 'adsb', 'ais.raw' → 'ais'.
	const source = topic.split('.')[0] ?? 'unknown';

	const result = normalizeAdsbRaw(rawValue);

	if (!result.ok) {
		if (result.kind === 'no_position') {
			// Valid source record; entity has no current GPS fix.
			// Archive the raw record, then skip. Not a DLQ candidate.
			await writeRawEvent(
				rawValue, // valid JSON — store directly as JSONB object
				result.entity_id,
				source,
				null, // provider not determinable without full normalization
				topic,
				partition,
				offset,
				null, // source_event_time unknown: time_position was null
			);
			log('warn', 'skipping record with no position', {
				entity_id: result.entity_id,
				offset,
			});
			return;
		}

		// parse_error or missing_entity_id — record is unprocessable.
		// Archive the raw record first, then publish to DLQ.
		//
		// parse_error: rawValue is not valid JSON. JSON.stringify wraps it in
		// quotes, producing a JSONB string scalar — satisfies the NOT NULL
		// JSONB column without inventing a wrapper object.
		// missing_entity_id: rawValue is valid JSON — store it directly.
		const payload = result.kind === 'parse_error' ? JSON.stringify(rawValue) : rawValue;
		await writeRawEvent(
			payload,
			null, // entity_id unknown for both error kinds
			source,
			null,
			topic,
			partition,
			offset,
			null,
		);

		const dlqEvent: DlqEvent = {
			raw_payload: rawValue,
			rejection_reason: `${result.kind}: ${result.detail}`,
			source_topic: topic,
			source_partition: partition,
			source_offset: offset,
			consumer_id: CONSUMER_ID,
			timestamp_ms: Date.now(),
		};

		try {
			await publishToDlq(dlqEvent);
			log('warn', 'record routed to dlq', {
				rejection_reason: dlqEvent.rejection_reason,
				source_topic: topic,
				source_partition: partition,
				source_offset: offset,
				dlq_topic: config.DLQ_TOPIC,
			});
		} catch (err) {
			// DLQ publish failed. Log and rethrow — the offset must NOT be
			// committed. Kafka will redeliver the message on restart.
			// The raw_events insert is idempotent on replay.
			log('error', 'dlq publish failed; not committing offset — Kafka will redeliver', {
				rejection_reason: dlqEvent.rejection_reason,
				source_topic: topic,
				source_partition: partition,
				source_offset: offset,
				error: err instanceof Error ? err.message : String(err),
			});
			throw err;
		}

		return;
	}

	const { position } = result;

	// Compute H3 cells once — used in both TimescaleDB and Redis writes.
	const { history_geo_cell, live_geo_cell } = computeH3Cells(position.lat, position.lon);

	// Step 1: archive the raw record.
	await writeRawEvent(
		rawValue, // valid JSON — store directly as JSONB object
		position.entity_id,
		source,
		position.provider,
		topic,
		partition,
		offset,
		new Date(position.timestamp_ms),
	);

	// Step 2: write canonical position history with H3 history cell.
	await writePositionHistory(position, history_geo_cell);

	// Step 3: read the entity's current live_geo_cell BEFORE overwriting the hash.
	// Once the Lua script runs, the old cell is gone from the hash. We need it
	// to know which sorted set to ZREM from if the entity moved.
	const oldLiveGeoCell = await redis.hget(`entity:live:${position.entity_id}`, 'live_geo_cell');

	// Step 4: update Redis live state under monotonic timestamp guard.
	// live_geo_cell is now included in the HSET fields.
	const accepted = await updateLiveState(position, live_geo_cell);

	if (accepted) {
		// Step 5: update geo-cell sorted sets.
		// Only runs when the live state was accepted (newer event). Stale events
		// must not mutate the sorted sets.
		await updateGeoCell(position.entity_id, live_geo_cell, position.timestamp_ms, oldLiveGeoCell);

		// Step 6: publish live-map update.
		// Only published for accepted events — the channel represents the monotonic
		// live view. Stale events must not push the UI to an older position.
		await publishPositionUpdate(position, live_geo_cell);

		// Step 7: clear signal-loss episode state if the entity was previously dark.
		// An accepted position means the entity has resumed transmitting. If the
		// Alert Evaluator emitted a SIGNAL_LOSS alert for this entity, the episode
		// gate (alert-state:{entity_id}) must be cleared so a future silence can
		// open a new episode with a new alert_id.
		//
		// recent-loss is written first so Phase 06 (composite correlation) can
		// read the prior episode's dark_since_ms and signal_loss_alert_id.
		// alert-state is deleted second. A crash between the two leaves both keys
		// present; the next accepted position for this entity will complete the
		// cleanup — recent-loss HSET is idempotent and DEL on a missing key is safe.
		const alertStateKey = `alert-state:${position.entity_id}`;
		const alertState = await redis.hgetall(alertStateKey);
		if (alertState && alertState['dark_since_ms']) {
			await redis.hset(
				`recent-loss:${position.entity_id}`,
				'dark_since_ms',
				alertState['dark_since_ms'],
				'resumed_at_ms',
				String(position.timestamp_ms),
				'signal_loss_alert_id',
				alertState['signal_loss_alert_id'] ?? '',
			);
			await redis.del(alertStateKey);
			log('info', 'signal loss episode cleared', {
				entity_id: position.entity_id,
				dark_since_ms: alertState['dark_since_ms'],
				resumed_at_ms: position.timestamp_ms,
				signal_loss_alert_id: alertState['signal_loss_alert_id'],
			});
		}
	} else {
		// Stale event: a newer position for this entity is already in Redis.
		// position_history still received the row (idempotent by observed_at),
		// but live state, sorted sets, and the live-map channel are not updated.
		log('warn', 'live state not updated — stale event', {
			entity_id: position.entity_id,
			timestamp_ms: position.timestamp_ms,
			offset,
		});
	}

	// Step 8: publish to position.normalized Kafka topic.
	// Published for ALL valid positions regardless of whether the Redis live-state
	// write was accepted. Downstream detectors need every event.
	// Blocks offset commit on failure — see top-of-file comment.
	await publishNormalized(position, history_geo_cell, live_geo_cell);

	log('info', 'position persisted', {
		entity_id: position.entity_id,
		entity_type: position.entity_type,
		timestamp_ms: position.timestamp_ms,
		lat: position.lat,
		lon: position.lon,
		speed_mps: position.speed_mps,
		course_deg: position.course_deg,
		altitude_m: position.altitude_m,
		callsign: position.callsign,
		history_geo_cell,
		live_geo_cell,
		live_state_accepted: accepted,
		offset,
	});
}

// ---- Consumer loop ---------------------------------------------------------

async function run(): Promise<void> {
	log('info', 'consumer starting', {
		brokers: config.KAFKA_BROKERS,
		group: config.GROUP_ID,
		source_topic: config.SOURCE_TOPIC,
		dlq_topic: config.DLQ_TOPIC,
		consumer_id: CONSUMER_ID,
		from_beginning: config.FROM_BEGINNING,
		live_state_ttl_seconds: config.LIVE_STATE_TTL_SECONDS,
		pg_pool_max: config.PG_POOL_MAX,
		features:
			'raw-events,position-history,redis-live-state,h3-geo-cell,normalized-publish,position-updates',
	});

	await producer.connect();
	log('info', 'dlq producer connected');

	await consumer.connect();
	log('info', 'consumer connected');

	await consumer.subscribe({ topic: config.SOURCE_TOPIC, fromBeginning: config.FROM_BEGINNING });
	log('info', 'consumer subscribed', { topic: SOURCE_TOPIC });

	await consumer.run({
		autoCommit: false,

		eachMessage: async ({ topic, partition, message }) => {
			const rawValue = message.value?.toString() ?? '';
			const offset = message.offset;

			await handleMessage(topic, partition, offset, rawValue);

			// Commit offset AFTER all processing.
			// Crash before commit → redeliver on restart → idempotent replay.
			// Crash after commit → no redeliver → normal at-least-once guarantee.
			await consumer.commitOffsets([
				{
					topic,
					partition,
					offset: (BigInt(offset) + 1n).toString(),
				},
			]);
		},
	});
}

// ---- Graceful shutdown -----------------------------------------------------

async function shutdown(signal: string): Promise<void> {
	log('info', 'shutdown initiated', { signal });
	await consumer.disconnect();
	await producer.disconnect();
	await pool.end();
	await redis.quit();
	log('info', 'consumer, dlq producer, db pool, and redis disconnected');
	process.exit(0);
}

process.on('SIGINT', () => {
	shutdown('SIGINT').catch(() => process.exit(1));
});
process.on('SIGTERM', () => {
	shutdown('SIGTERM').catch(() => process.exit(1));
});

run().catch((err: unknown) => {
	log('error', 'consumer failed', {
		error: {
			name: err instanceof Error ? err.name : 'UnknownError',
			message: err instanceof Error ? err.message : String(err),
		},
	});
	process.exit(1);
});
