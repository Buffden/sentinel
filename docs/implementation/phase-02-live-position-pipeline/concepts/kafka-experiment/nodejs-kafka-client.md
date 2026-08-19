# Node.js Kafka Client: Setup and Decisions

## Client library

| | `kafkajs` | `@confluentinc/kafka-javascript` |
| --- | --- | --- |
| Implementation | Pure JavaScript | Wraps librdkafka (C) |
| TypeScript | Built-in | Built-in |
| Native compilation | None | Required (`node-gyp`, C++ toolchain) |
| Redpanda compatibility | Full | Full |
| Maintenance | Community | Confluent (official, active) |
| API transparency | Exposes consumer group join, offset commit, partition assignment directly | Higher-level abstractions |

**Decision: `kafkajs`.** No native build step. API maps 1:1 to the Kafka protocol operations visible in `rpk`. Replaceable without touching any service contract.

---

## Project structure

Two independent TypeScript projects. No shared package yet.

ADR-013 calls for a `packages/shared` package for Kafka config, topic constants, and `position.normalized` types. Introduced when services actually share types, not before.

```text
services/
  ingestion-poller/         ← standalone pnpm project
    package.json
    tsconfig.json
    src/produce.ts
  position-consumer/        ← standalone pnpm project
    package.json
    tsconfig.json
    src/consume.ts
```

---

## TypeScript runtime

`tsx` (esbuild-based): runs `.ts` files directly with no compilation step or extra `tsconfig` flags.

`ts-node` alternative requires additional ESM/Node 22 configuration and is slower to start.

---

## Broker address

```typescript
const brokers = (process.env['KAFKA_BROKERS'] ?? 'localhost:9092').split(',');
```

Host-side Node processes use `localhost:9092` — the externally-advertised `PLAINTEXT` listener mapped by Docker Compose. Container-to-container communication uses `redpanda:29092` (`PLAINTEXT_INTERNAL`). No `.env` file for CP1.

---

## Kafka instance

```typescript
const kafka = new Kafka({
  clientId: 'ingestion-poller',  // appears in broker logs; not a consumer group ID
  brokers,
  logLevel: 0,                   // suppress kafkajs logger; structured JSON replaces it
});
```

---

## Partitioner

```typescript
const producer = kafka.producer({
  createPartitioner: Partitioners.LegacyPartitioner,
});
```

kafkajs v2 requires an explicit partitioner. `LegacyPartitioner`: murmur2 hash of key mod partition count. `JavaCompatiblePartitioner`: matches the Java client — relevant for cross-language key routing. With one partition the choice has no routing effect; the import is explicit to suppress the deprecation warning.
