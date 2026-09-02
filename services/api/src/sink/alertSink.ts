import { Kafka } from 'kafkajs';
import { pool } from '../db.js';
import { redis } from '../redis.js';
import { config } from '../config.js';

const kafka = new Kafka({ brokers: config.KAFKA_BROKERS });
const consumer = kafka.consumer({ groupId: config.API_GROUP_ID });

export interface AlertMessage {
	alert_id: string;
	entity_id: string;
	entity_type: string;
	alert_type: string;
	priority: string;
	status: string;
	detected_at_ms: number;
	payload: Record<string, unknown>;
}

// Idempotent persistence + WebSocket fan-out publish for one alert.
// ON CONFLICT (alert_id) DO NOTHING: the Alert Evaluator computes a
// deterministic, type-specific alert_id per episode — this is the durable
// backstop that makes a redelivered or independently re-detected alert for
// the same episode a no-op instead of a duplicate row.
//
// raw is republished verbatim (not re-serialized from `alert`) so WebSocket
// subscribers see exactly the bytes the Alert Evaluator produced.
export async function persistAlert(alert: AlertMessage, raw: string): Promise<void> {
	await pool.query(
		`INSERT INTO alerts
			 (alert_id, entity_id, entity_type, alert_type, priority, status, payload, detected_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8)
		 ON CONFLICT (alert_id) DO NOTHING`,
		[
			alert.alert_id,
			alert.entity_id,
			alert.entity_type,
			alert.alert_type,
			alert.priority,
			alert.status,
			JSON.stringify(alert.payload),
			new Date(alert.detected_at_ms),
		],
	);

	await redis.publish(config.ALERT_EVENTS_CHANNEL, raw);
}

export async function startAlertSink(): Promise<void> {
	await consumer.connect();
	await consumer.subscribe({ topic: config.ALERTS_TOPIC, fromBeginning: false });
	console.log(
		JSON.stringify({
			level: 'info',
			msg: 'alert sink consumer started',
			brokers: config.KAFKA_BROKERS,
			topic: config.ALERTS_TOPIC,
			group: config.API_GROUP_ID,
		}),
	);

	await consumer.run({
		autoCommit: false,
		eachMessage: async ({ topic, partition, message }) => {
			const raw = message.value?.toString();
			if (!raw) return;

			let alert: AlertMessage;
			try {
				alert = JSON.parse(raw) as AlertMessage;
			} catch (err) {
				console.error(
					JSON.stringify({ level: 'error', msg: 'alert parse failed', err: String(err), raw }),
				);
				// Commit and skip — malformed messages cannot be fixed by retry.
				await consumer.commitOffsets([
					{ topic, partition, offset: String(Number(message.offset) + 1) },
				]);
				return;
			}

			// Validate required fields. Missing or invalid fields cannot be fixed by retry — skip.
			if (
				!alert.alert_id ||
				!alert.entity_id ||
				!alert.alert_type ||
				!alert.priority ||
				!alert.status ||
				typeof alert.detected_at_ms !== 'number' ||
				!isFinite(alert.detected_at_ms)
			) {
				console.error(
					JSON.stringify({
						level: 'error',
						msg: 'alert validation failed — skipping',
						alert_id: alert.alert_id,
						raw,
					}),
				);
				await consumer.commitOffsets([
					{ topic, partition, offset: String(Number(message.offset) + 1) },
				]);
				return;
			}

			// Steps 1-2: persist idempotently, then publish for WebSocket fan-out (CP6).
			await persistAlert(alert, raw);

			// Step 3: commit offset — last, so a crash before here causes safe redeliver.
			await consumer.commitOffsets([
				{ topic, partition, offset: String(Number(message.offset) + 1) },
			]);

			console.log(
				JSON.stringify({
					level: 'info',
					msg: 'alert sinked',
					alert_id: alert.alert_id,
					alert_type: alert.alert_type,
				}),
			);
		},
	});
}
