// Position Consumer — CP3: parse + normalize + log canonical output.
//
// Consumer group: position-consumer (canonical; do not change without ADR).
//
// CP3 scope: consume adsb.raw, normalize each record using the canonical
// schema, log the canonical position object so the normalized shape can be
// inspected against real records. No persistence, no Redis, no downstream
// Kafka writes, no DLQ publishing yet — those belong to later checkpoints.
//
// Offset commit strategy: manual (autoCommit: false).
//   The commit is the LAST action for every message. A crash before the commit
//   causes Kafka to redeliver the message to the next consumer run.
//
//   At CP3 there are no durable side effects, so the commit is unconditional.
//   From CP5 onward the commit moves after all persistence writes succeed.
//
// Checkpoints yet to implement (marked with TODO-CP):
//   CP4: DLQ publish for parse_error / missing_entity_id
//   CP5: INSERT position_history (idempotent)
//   CP5: INSERT raw_events (idempotent; uses source_topic + partition + offset)
//   CP6: Redis entity:live:{entity_id} monotonic update
//   CP7: Redis geo-cell:{live_geo_cell} sorted-set update
//   CP8: position.normalized Kafka publish
//   CP8: Redis position-updates pub/sub publish

import { hostname } from 'os';
import { Kafka } from 'kafkajs';
import { normalizeAdsbRaw, HISTORY_H3_RESOLUTION, LIVE_H3_RESOLUTION } from './normalize.js';

// ---- Configuration ---------------------------------------------------------

const BROKERS = (process.env['KAFKA_BROKERS'] ?? 'localhost:9092').split(',');
const SOURCE_TOPIC = 'adsb.raw';
const GROUP_ID = 'position-consumer';

// Stable identifier for this consumer instance; used in future DLQ records.
const CONSUMER_ID = `${hostname()}-${process.pid}`;

// fromBeginning: true on first run to process all existing adsb.raw records
// and observe normalization without waiting for new polls.
const FROM_BEGINNING = (process.env['FROM_BEGINNING'] ?? 'true') === 'true';

// ---- Kafka setup -----------------------------------------------------------

const kafka = new Kafka({
	clientId: 'position-consumer',
	brokers: BROKERS,
	logLevel: 0,
});

const consumer = kafka.consumer({ groupId: GROUP_ID });

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

// ---- Message handler -------------------------------------------------------

async function handleMessage(offset: string, rawValue: string): Promise<void> {
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
		// TODO-CP4: publish to DLQ before returning.
		log('warn', 'record rejected; dlq routing added in cp4', {
			kind: result.kind,
			detail: result.detail,
			offset,
		});
		return;
	}

	const { position } = result;

	// CP3: log the full canonical position so the normalized shape can be
	// inspected against real adsb.raw records. This is the observability
	// deliverable for CP3.
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
		history_geo_cell: position.history_geo_cell,
		live_geo_cell: position.live_geo_cell,
		offset,
	});

	// TODO-CP5: await writePositionHistory(position, rawValue, SOURCE_TOPIC, partition, offset);
	// TODO-CP6: await updateRedisLiveState(position);
	// TODO-CP7: await updateGeoCell(position);
	// TODO-CP8: await publishNormalized(position);
	// TODO-CP8: await publishPositionUpdate(position);
}

// ---- Consumer loop ---------------------------------------------------------

async function run(): Promise<void> {
	log('info', 'consumer starting', {
		brokers: BROKERS,
		group: GROUP_ID,
		source_topic: SOURCE_TOPIC,
		consumer_id: CONSUMER_ID,
		from_beginning: FROM_BEGINNING,
		history_h3_resolution: HISTORY_H3_RESOLUTION,
		live_h3_resolution: LIVE_H3_RESOLUTION,
		checkpoint: 'CP3',
	});

	await consumer.connect();
	log('info', 'consumer connected');

	await consumer.subscribe({ topic: SOURCE_TOPIC, fromBeginning: FROM_BEGINNING });
	log('info', 'consumer subscribed', { topic: SOURCE_TOPIC });

	await consumer.run({
		autoCommit: false,

		eachMessage: async ({ partition, message }) => {
			const rawValue = message.value?.toString() ?? '';
			const offset = message.offset;

			await handleMessage(offset, rawValue);

			// Commit offset after processing. Crash before commit → redeliver on restart.
			// At CP3 there are no durable side effects so the commit is unconditional.
			// From CP5 onward this commit moves after all persistence writes succeed.
			await consumer.commitOffsets([
				{
					topic: SOURCE_TOPIC,
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
	log('info', 'consumer disconnected');
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
