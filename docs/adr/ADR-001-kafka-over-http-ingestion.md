# ADR-001: Kafka over Direct HTTP Ingestion

**Status:** Accepted
**Date:** 2026-08-05

---

## Context

The ingestion layer must receive positional telemetry from ADS-B and AIS feeds and deliver it to downstream consumers (position store, correlation worker). The feeds are polled on a fixed interval and produce bursty, variable-volume data  - coverage gaps, transponder dropouts, and polling-interval jitter mean the arrival rate is unpredictable.

Two approaches were considered:

1. Poller → HTTP POST directly to downstream services
2. Poller → Kafka topic → downstream consumers

---

## Decision

Use Kafka as the message broker between the ingestion poller and all downstream consumers.

- **Local development (Docker Compose):** Redpanda - single binary, no ZooKeeper/KRaft setup, Kafka-compatible API
- **Production (AWS):** Amazon MSK (managed Kafka) - operationally managed, integrates with IAM and VPC, same client code as local with no changes required

---

## Reasoning

**Decoupling producers from consumers.** With direct HTTP, the poller is coupled to the availability and throughput of every downstream service. If the position consumer is slow or restarting, the poller either blocks, drops data, or needs its own retry/buffer logic. Kafka eliminates this  - the poller publishes and moves on.

**Absorbing bursts.** ADS-B/AIS feeds can spike when a poller catches up after a gap. Kafka acts as a temporal buffer, letting consumers process at their own pace without back-pressure propagating to the producer.

**Fan-out without coordination.** Multiple consumers (position consumer, correlation worker) read the same topic independently. With direct HTTP, the poller would need to know about and call every consumer. With Kafka, new consumers can be added without changing the poller.

**Replay.** Kafka's log retention means a consumer can replay events from a past offset  - useful for backfilling Neo4j after a correlation worker restart, or re-running a corrected anomaly rule.

**Dead-letter queue.** Malformed events are routed to a DLQ topic rather than dropped or crashing the consumer  - this is natural with Kafka's topic model.

---

## Alternatives Considered

### Direct HTTP POST (rejected)
- Tight coupling between poller and consumers
- Poller must implement retry, backoff, and circuit breaking itself
- No replay capability
- Fan-out requires the poller to know all downstream addresses
- Simpler operationally (no broker to run), but the coupling cost is too high for a streaming system

### RabbitMQ (rejected)
- Message-queue model (point-to-point or pub/sub with exchanges) rather than a log
- No native replay from offset  - once a message is consumed and acked, it's gone
- Less natural fit for time-series event streams where replay and backfill are first-class concerns

---

## Consequences

- Adds operational complexity: Redpanda (local) or MSK (AWS) must be running before any consumer can process data
- Consumers must handle at-least-once delivery (Kafka does not guarantee exactly-once without additional configuration)
- All writes downstream must be idempotent  - addressed by ADR-007
