// Position Consumer — CP4: validation + DLQ routing.
//
// Consumer group: position-consumer (canonical; do not change without ADR).
//
// CP4 adds DLQ publishing for unprocessable records:
//   parse_error / missing_entity_id → adsb.dlq with rejection envelope
//   no_position                     → skip with warn (not a DLQ candidate)
//
// DLQ publish failures block offset commit.
//   A transient broker error on the DLQ path propagates out of handleMessage.
//   The offset is NOT committed. Kafka redelivers the message on the next
//   consumer start. The raw_events insert is idempotent on replay.
//
// Offset commit strategy: manual (autoCommit: false).
//   The commit is the LAST action for every message. A crash before
//   the commit causes Kafka to redeliver the message on restart.
//   All downstream writes are idempotent so replay is always safe.
//
// CP4 note — testing malformed records:
//   Inject fresh test records for DLQ experiments. Records already
//   committed by CP3 will not be redelivered unless the group offset
//   is reset first:
//     rpk group seek position-consumer --to <offset> --topics adsb.raw
//
// Checkpoints yet to implement (marked with TODO-CP):
//   CP5: INSERT position_history (idempotent)
//   CP5: INSERT raw_events (idempotent; uses source_topic + partition + offset)
//   CP6: Redis entity:live:{entity_id} monotonic update
//   CP7: H3 geo-cell computation; Redis geo-cell:{live_geo_cell} sorted-set update
//   CP8: position.normalized Kafka publish
//   CP8: Redis position-updates pub/sub publish

import { hostname } from 'os';
import { Kafka, Partitioners } from 'kafkajs';
import { normalizeAdsbRaw } from './normalize.js';

// ---- Configuration ---------------------------------------------------------

const BROKERS = (process.env['KAFKA_BROKERS'] ?? 'localhost:9092').split(',');
const SOURCE_TOPIC = 'adsb.raw';
const DLQ_TOPIC = 'adsb.dlq';
const GROUP_ID = 'position-consumer';

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
	const result = normalizeAdsbRaw(rawValue);

	if (!result.ok) {
		if (result.kind === 'no_position') {
			// Valid source record; entity has no current GPS fix.
			// Logged at warn so operators can see the skip rate without info noise.
			log('warn', 'skipping record with no position', {
				entity_id: result.entity_id,
				offset,
			});
			return;
		}

		// parse_error or missing_entity_id — record is unprocessable.
		// Publish to DLQ so operators can inspect without blocking the pipeline.
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
			// The raw_events insert (CP5) is idempotent on replay.
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

	log('info', 'position normalized', {
		entity_id: position.entity_id,
		entity_type: position.entity_type,
		timestamp_ms: position.timestamp_ms,
		lat: position.lat,
		lon: position.lon,
		speed_mps: position.speed_mps,
		course_deg: position.course_deg,
		altitude_m: position.altitude_m,
		baro_altitude_m: position.baro_altitude_m,
		geo_altitude_m: position.geo_altitude_m,
		vertical_rate_mps: position.vertical_rate_mps,
		on_ground: position.on_ground,
		callsign: position.callsign,
		squawk: position.squawk,
		entity_subtype: position.entity_subtype,
		provider_category: position.provider_category,
		provider: position.provider,
		source: position.source,
		offset,
	});

	// TODO-CP5: await writePositionHistory(position, rawValue, topic, partition, offset);
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
		checkpoint: 'CP4',
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

			// Commit offset AFTER all processing — including DLQ publish attempt.
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
	log('info', 'consumer and dlq producer disconnected');
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
