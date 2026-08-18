# Phase 01 Concepts

Concept notes and debrief records for Phase 01 checkpoints. Read in the order listed.

| File | Topic |
| --- | --- |
| [redpanda-and-kafka-basics.md](redpanda-and-kafka-basics.md) | What Kafka is, brokers, producers, consumers, topics, why Sentinel uses an event broker, and what Redpanda is |
| [docker-networking-for-kafka.md](docker-networking-for-kafka.md) | Why Kafka needs dual listeners in Docker: `localhost` vs service-name routing and the advertised-address problem |
| [timescaledb-migrations-prerequisites.md](timescaledb-migrations-prerequisites.md) | DDL transactions, `ON_ERROR_STOP`, migration order, hypertable rules, idempotency vs tracking |
| [timescaledb-migrations-debrief.md](timescaledb-migrations-debrief.md) | Debrief: schema applied, migration flow, transaction and failure guarantees, idempotency result, clean rebuild |
| [kafka-topics-partitions-offsets.md](kafka-topics-partitions-offsets.md) | Topics, partitions, records, offsets, high watermark, consumer groups, at-least-once processing |
| [neo4j-schema-and-graph-basics.md](neo4j-schema-and-graph-basics.md) | Graph primitives, why `Entity.id` is unique, constraint vs index, MATCH/CREATE/MERGE, idempotency model |
| [manual-store-verification.md](manual-store-verification.md) | Debrief: all four stores verified by direct CLI interaction before any application code |
| [structured-logging-convention.md](structured-logging-convention.md) | Mandatory fields, level semantics, message rules, contextual fields, error shape, secrets/PII rules |
| [health-readiness-convention.md](health-readiness-convention.md) | Liveness vs readiness probes, HTTP endpoints, readiness by service, graceful shutdown |

## Diagrams

| File | Shows |
| --- | --- |
| [kafka-basics.puml](kafka-basics.puml) | Kafka broker, producer, consumer topology |
| [migration-flow.puml](migration-flow.puml) | Migration script execution flow |
