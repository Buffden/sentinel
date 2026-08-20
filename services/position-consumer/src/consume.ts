// Kafka consumer experiment — prove Node.js/TypeScript can consume a Kafka event from a consumer group.
//
// This is a learning experiment, not the final Position Consumer implementation.
// It subscribes to adsb.raw, logs each message with full Kafka context, and runs
// until interrupted.
//
// Consumer group: kafka-experiment
//   A disposable group for this experiment. The canonical application group
//   (position-consumer) is reserved for the real Position Consumer implementation.
//
// fromBeginning: true
//   On the first run the group has no committed offset. fromBeginning tells kafkajs
//   to start from offset 0 rather than the latest. Without this, a consumer started
//   after the producer would see no messages.
//
// autoCommit: true (kafkajs default)
//   After each batch is processed successfully, kafkajs commits the high-water mark
//   automatically on its commit interval (default 5 s). This is the baseline we
//   observe in the normal flow. The restart experiment will demonstrate what happens
//   when the offset is NOT committed.

import { Kafka } from 'kafkajs';

const brokers = (process.env['KAFKA_BROKERS'] ?? 'localhost:9092').split(',');

const kafka = new Kafka({
  clientId: 'position-consumer',
  brokers,
  logLevel: 0,
});

const consumer = kafka.consumer({
  groupId: 'kafka-experiment',
});

function log(level: 'info' | 'warn' | 'error', message: string, extra?: Record<string, unknown>) {
  process.stdout.write(
    JSON.stringify({
      timestamp: new Date().toISOString(),
      level,
      service: 'position-consumer',
      message,
      ...extra,
    }) + '\n'
  );
}

const TOPIC = 'adsb.raw';

async function run() {
  log('info', 'consumer connecting', { brokers, group: 'kafka-experiment' });

  await consumer.connect();
  log('info', 'consumer connected');

  // fromBeginning: true — read from offset 0 if the group has no committed position.
  await consumer.subscribe({ topic: TOPIC, fromBeginning: true });
  log('info', 'consumer subscribed', { topic: TOPIC, fromBeginning: true });

  await consumer.run({
    // autoCommit is true by default in kafkajs. Offsets are committed on a
    // background interval (default 5 s) after each message handler returns.
    // This is the behaviour we observe first. The restart experiment will show
    // what committed vs uncommitted offsets mean in practice.
    eachMessage: async ({ topic, partition, message }) => {
      const rawValue = message.value?.toString() ?? '';
      let payload: unknown;
      try {
        payload = JSON.parse(rawValue);
      } catch {
        payload = rawValue;
      }

      log('info', 'kafka message received', {
        topic,
        partition,
        // offset is a string in kafkajs — log as-is so it matches rpk output exactly.
        offset: message.offset,
        key: message.key?.toString() ?? null,
        payload,
      });
    },
  });
}

// Graceful shutdown: on SIGINT or SIGTERM, disconnect cleanly from the consumer
// group. This allows the broker to immediately reassign partitions to another
// consumer rather than waiting for the session timeout.
async function shutdown(signal: string) {
  log('info', 'shutdown initiated', { signal });
  await consumer.disconnect();
  log('info', 'consumer disconnected');
  process.exit(0);
}

process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));

run().catch((err: unknown) => {
  log('error', 'consumer failed', {
    error: {
      name: err instanceof Error ? err.name : 'UnknownError',
      message: err instanceof Error ? err.message : String(err),
    },
  });
  process.exit(1);
});
