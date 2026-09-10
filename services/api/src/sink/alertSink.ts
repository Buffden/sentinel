import { Kafka, type Consumer } from 'kafkajs';
import { redis } from '../redis.js';
import { config } from '../config.js';
import {
	persistCompositeAlert,
	persistIndividualAlert,
	type PublishedAlert,
} from './compositeSupersession.js';

const kafka = new Kafka({ brokers: config.KAFKA_BROKERS });

export interface AlertMessage {
	alert_id: string;
	entity_id: string;
	counterparty_entity_id?: string | null;
	entity_type: string;
	alert_type: string;
	priority: string;
	status: string;
	detected_at_ms: number;
	payload: Record<string, unknown>;
}

// Idempotent DB persistence for one alert. ON CONFLICT (alert_id) DO
// NOTHING: the Alert Evaluator computes a deterministic, type-specific
// alert_id per episode, this is the durable backstop that makes a
// redelivered or independently re-detected alert for the same episode a
// no-op instead of a duplicate row.
//
// Returns the canonical current state of every row this message concerns
// (itself, plus every alert a COMPOSITE actually supersedes), for the
// caller to publish after commit. Never returns "what changed this call",
// a COMPOSITE redelivery whose DB work is now entirely a no-op still
// returns the same rows, so the caller can always republish them, closing
// the gap where a prior delivery's later publish attempt failed after an
// earlier one succeeded (DATA_MODEL.md's Pre-CP5B(d)).
export async function persistAlert(alert: AlertMessage): Promise<PublishedAlert[]> {
	if (alert.alert_type === 'COMPOSITE') {
		return persistCompositeAlert(alert);
	}
	const published = await persistIndividualAlert(alert);
	return [published];
}

export interface AlertSinkSession {
	consumer: Consumer;
	stop: () => Promise<void>;
}

// groupId defaults to the production group; tests pass a disposable one so
// they never join the real alert-events consumer group.
export async function startAlertSink(
	groupId: string = config.API_GROUP_ID,
): Promise<AlertSinkSession> {
	const consumer = kafka.consumer({ groupId });
	await consumer.connect();
	await consumer.subscribe({ topic: config.ALERTS_TOPIC, fromBeginning: false });
	console.log(
		JSON.stringify({
			level: 'info',
			msg: 'alert sink consumer started',
			brokers: config.KAFKA_BROKERS,
			topic: config.ALERTS_TOPIC,
			group: groupId,
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

			// Persist idempotently first. The returned rows are this message's
			// canonical current state, not merely what this call happened to
			// mutate, so publishing them below is safe to redo on every
			// redelivery even when this attempt's own DB work was a no-op.
			const publishedAlerts = await persistAlert(alert);

			// Publish every row after commit, unconditionally, one at a time.
			// If any publish throws, the offset below is never reached, so
			// Kafka redelivers and every one of these publishes is retried,
			// not only whichever one failed (DATA_MODEL.md's Pre-CP5B(d)).
			for (const publishedAlert of publishedAlerts) {
				await redis.publish(config.ALERT_EVENTS_CHANNEL, JSON.stringify(publishedAlert));
			}

			// Commit offset last, so a crash or publish failure before here causes safe redelivery.
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

	let stopPromise: Promise<void> | null = null;
	const stop = (): Promise<void> => {
		if (!stopPromise) stopPromise = consumer.disconnect();
		return stopPromise;
	};
	return { consumer, stop };
}
