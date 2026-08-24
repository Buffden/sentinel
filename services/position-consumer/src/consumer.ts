// Position Consumer — CP5: TimescaleDB persistence.
//
// Consumer group: position-consumer (canonical; do not change without ADR).
//
// Write order for every Kafka message:
//   1. INSERT raw_events (required before offset commit on every path)
//   2. Branch by normalization result:
//        parse_error / missing_entity_id → DLQ publish → commit
//        no_position                     → warn log → commit
//        valid position                  → INSERT position_history → commit
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
// Offset commit strategy: manual (autoCommit: false).
//   The commit is the LAST action for every message. A crash before
//   the commit causes Kafka to redeliver the message on restart.
//   All downstream writes are idempotent so replay is always safe.
//
// geo_cell is written as NULL at CP5.
//   CP7 owns H3 computation (latlon → H3 cell) and will populate it.
//
// CP4 note — testing malformed records:
//   Inject fresh test records for DLQ experiments. Records already
//   committed will not be redelivered unless the group offset is reset:
//     rpk group seek position-consumer --to <offset> --topics adsb.raw
//
// Checkpoints yet to implement (marked with TODO-CP):
//   CP6: Redis entity:live:{entity_id} monotonic update
//   CP7: H3 geo-cell computation; Redis geo-cell:{live_geo_cell} sorted-set update
//   CP8: position.normalized Kafka publish
//   CP8: Redis position-updates pub/sub publish

import { hostname } from 'os';
import { Kafka, Partitioners } from 'kafkajs';
import pg from 'pg';
import { normalizeAdsbRaw, type NormalizedPosition } from './normalize.js';

const { Pool } = pg;

// ---- Configuration ---------------------------------------------------------

const BROKERS = (process.env['KAFKA_BROKERS'] ?? 'localhost:9092').split(',');
const SOURCE_TOPIC = 'adsb.raw';
const DLQ_TOPIC = 'adsb.dlq';
const GROUP_ID = 'position-consumer';
const PG_URL = process.env['PG_URL'] ?? 'postgresql://sentinel:sentinel-dev@localhost:5433/sentinel';

// Stable identifier for this consumer instance, used in DLQ records so
// operators can trace which consumer instance rejected a message.
const CONSUMER_ID = `${hostname()}-${process.pid}`;

const FROM_BEGINNING = (process.env['FROM_BEGINNING'] ?? 'true') === 'true';

// ---- Kafka setup -----------------------------------------------------------

const kafka = new Kafka({
	clientId: 'position-consumer',
	brokers: BROKERS,
	logLevel: 0,
});

const consumer = kafka.consumer({ groupId: GROUP_ID });

// Producer for publishing DLQ records.
// LegacyPartitioner for consistent kafkajs v2 behaviour.
const producer = kafka.producer({
	createPartitioner: Partitioners.LegacyPartitioner,
});

// ---- Database setup --------------------------------------------------------

const pool = new Pool({ connectionString: PG_URL });

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
// geo_cell is NULL at CP5; CP7 populates it after computing the H3 cell.
// ON CONFLICT (entity_id, observed_at) DO NOTHING: replay-safe idempotency.
async function writePositionHistory(position: NormalizedPosition): Promise<void> {
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
      $1,  $2,  $3,  $4,  NULL,
      $5,  $6,  $7,  $8,  $9,
      $10, $11, $12, $13, $14,
      $15, $16, $17, $18, $19,
      $20, $21, $22, $23, $24, $25,
      $26, $27, $28, $29
    )
    ON CONFLICT (entity_id, observed_at) DO NOTHING`,
		[
			position.entity_id, position.entity_type, observedAt, position.timestamp_ms,
			position.lat, position.lon, position.altitude_m, position.source, position.provider,
			position.baro_altitude_m, position.geo_altitude_m, position.speed_mps, position.course_deg, position.heading_deg,
			position.vertical_rate_mps, position.on_ground, position.last_contact_ms, position.navigation_status, position.rate_of_turn,
			position.callsign, position.entity_subtype, position.provider_category, position.squawk, position.spi, position.position_source,
			position.position_accuracy, position.destination, position.eta, position.draught_m,
		],
	);
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
		topic: DLQ_TOPIC,
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
				topic, partition, offset,
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
			topic, partition, offset,
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
				dlq_topic: DLQ_TOPIC,
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

	// Step 1: archive the raw record.
	await writeRawEvent(
		rawValue, // valid JSON — store directly as JSONB object
		position.entity_id,
		source,
		position.provider,
		topic, partition, offset,
		new Date(position.timestamp_ms),
	);

	// Step 2: write canonical position history.
	await writePositionHistory(position);

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
		offset,
	});

	// TODO-CP6: await updateRedisLiveState(position);
	// TODO-CP7: const { history_geo_cell, live_geo_cell } = computeH3Cells(position.lat, position.lon);
	// TODO-CP7: await updateGeoCell(position.entity_id, live_geo_cell, position.timestamp_ms);
	// TODO-CP8: await publishNormalized({ ...position, history_geo_cell, live_geo_cell });
	// TODO-CP8: await publishPositionUpdate({ ...position, history_geo_cell, live_geo_cell });
}

// ---- Consumer loop ---------------------------------------------------------

async function run(): Promise<void> {
	log('info', 'consumer starting', {
		brokers: BROKERS,
		group: GROUP_ID,
		source_topic: SOURCE_TOPIC,
		dlq_topic: DLQ_TOPIC,
		consumer_id: CONSUMER_ID,
		from_beginning: FROM_BEGINNING,
		checkpoint: 'CP5',
	});

	await producer.connect();
	log('info', 'dlq producer connected');

	await consumer.connect();
	log('info', 'consumer connected');

	await consumer.subscribe({ topic: SOURCE_TOPIC, fromBeginning: FROM_BEGINNING });
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
	log('info', 'consumer, dlq producer, and db pool disconnected');
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
