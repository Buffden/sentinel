// Kafka producer experiment — prove Node.js/TypeScript can produce a Kafka event.
//
// This is a learning experiment, not the final OpenSky ingestion poller.
// It publishes one synthetic event to adsb.raw and exits.
//
// The message key is entity_id. In a single-partition topic the key has no
// routing effect, but using entity_id as the key is the correct convention:
// when partitions > 1, kafkajs uses a hash of the key to route messages,
// ensuring all events for the same entity land in the same partition and are
// consumed in order by one consumer.

import { Kafka, Partitioners } from 'kafkajs';

// KAFKA_BROKERS: external listener address used when running Node from the host.
// Docker Compose exposes Redpanda on localhost:9092 (the PLAINTEXT listener).
// Services inside Docker would use redpanda:29092 (the PLAINTEXT_INTERNAL listener).
const brokers = (process.env['KAFKA_BROKERS'] ?? 'localhost:9092').split(',');

const kafka = new Kafka({
  clientId: 'ingestion-poller',
  brokers,
  // Suppress kafkajs' own logger; we emit structured JSON ourselves.
  logLevel: 0,
});

const producer = kafka.producer({
  // DefaultPartitioner is deprecated in kafkajs v2. LegacyPartitioner preserves
  // the v1 behaviour; JavaCompatiblePartitioner matches the Java client's murmur2
  // hash. With 1 partition the choice has no routing effect, but we must be
  // explicit to avoid the deprecation warning.
  createPartitioner: Partitioners.LegacyPartitioner,
});

// log emits a single JSON line to stdout, following the Phase 01 structured
// logging convention: timestamp, level, service, message, plus optional fields.
function log(level: 'info' | 'warn' | 'error', message: string, extra?: Record<string, unknown>) {
  process.stdout.write(
    JSON.stringify({
      timestamp: new Date().toISOString(),
      level,
      service: 'ingestion-poller',
      message,
      ...extra,
    }) + '\n'
  );
}

const TOPIC = 'adsb.raw';

// Synthetic experiment payload. This is not the final OpenSky contract.
// The entity_id key field is the canonical Sentinel entity identifier.
const event = {
  entity_id: 'test-aircraft-1',
  timestamp_ms: Date.now(),
};

async function run() {
  log('info', 'producer connecting', { brokers });

  await producer.connect();
  log('info', 'producer connected');

  const results = await producer.send({
    topic: TOPIC,
    messages: [
      {
        // Key drives partition assignment when partitions > 1.
        key: event.entity_id,
        value: JSON.stringify(event),
      },
    ],
  });

  // results is an array of RecordMetadata, one entry per topic-partition.
  // With 1 partition there is exactly one entry.
  for (const meta of results) {
    log('info', 'kafka event produced', {
      topic: meta.topicName,
      partition: meta.partition,
      // offset is a string in kafkajs (Kafka offsets can exceed JS safe integer range).
      offset: meta.baseOffset,
      key: event.entity_id,
      payload: event,
    });
  }

  await producer.disconnect();
  log('info', 'producer disconnected');
}

run().catch((err: unknown) => {
  log('error', 'producer failed', {
    error: {
      name: err instanceof Error ? err.name : 'UnknownError',
      message: err instanceof Error ? err.message : String(err),
    },
  });
  process.exit(1);
});
