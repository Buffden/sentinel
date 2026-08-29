import { Kafka } from 'kafkajs';
import { pool } from '../db.js';
import { redis } from '../redis.js';

const KAFKA_BROKERS = (process.env['KAFKA_BROKERS'] ?? 'localhost:9092').split(',');
const ALERTS_TOPIC = 'alerts';
const GROUP_ID = 'api';

const kafka = new Kafka({ brokers: KAFKA_BROKERS });
const consumer = kafka.consumer({ groupId: GROUP_ID });

interface AlertMessage {
  alert_id: string;
  entity_id: string;
  entity_type: string;
  alert_type: string;
  priority: string;
  status: string;
  detected_at_ms: number;
  payload: Record<string, unknown>;
}

export async function startAlertSink(): Promise<void> {
  await consumer.connect();
  await consumer.subscribe({ topic: ALERTS_TOPIC, fromBeginning: false });
  console.log(JSON.stringify({ level: 'info', msg: 'alert sink consumer started', brokers: KAFKA_BROKERS, topic: ALERTS_TOPIC, group: GROUP_ID }));

  await consumer.run({
    autoCommit: false,
    eachMessage: async ({ topic, partition, message }) => {
      const raw = message.value?.toString();
      if (!raw) return;

      let alert: AlertMessage;
      try {
        alert = JSON.parse(raw) as AlertMessage;
      } catch (err) {
        console.error(JSON.stringify({ level: 'error', msg: 'alert parse failed', err: String(err), raw }));
        // Commit and skip — malformed messages cannot be fixed by retry.
        await consumer.commitOffsets([{ topic, partition, offset: String(Number(message.offset) + 1) }]);
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
        console.error(JSON.stringify({ level: 'error', msg: 'alert validation failed — skipping', alert_id: alert.alert_id, raw }));
        await consumer.commitOffsets([{ topic, partition, offset: String(Number(message.offset) + 1) }]);
        return;
      }

      // Step 1: persist idempotently.
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

      // Step 2: publish to alert-events for WebSocket fan-out (CP6).
      await redis.publish('alert-events', raw);

      // Step 3: commit offset — last, so a crash before here causes safe redeliver.
      await consumer.commitOffsets([{ topic, partition, offset: String(Number(message.offset) + 1) }]);

      console.log(JSON.stringify({ level: 'info', msg: 'alert sinked', alert_id: alert.alert_id, alert_type: alert.alert_type }));
    },
  });
}
