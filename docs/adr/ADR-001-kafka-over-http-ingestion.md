# ADR-001: Kafka over Direct HTTP Ingestion

**Status:** Accepted
**Date:** 2026-08-05

---

## Context

ADS-B/AIS polling produces bursty, variable-rate telemetry. Downstream services must be able to restart, lag temporarily, and consume independently without forcing the ingestion poller to coordinate their availability.

---

## Decision

Use Kafka semantics as the event backbone:

- Redpanda locally in Docker Compose;
- Amazon MSK as the intended AWS deployment target.

The ingestion poller publishes raw source records to `adsb.raw` / `ais.raw`; downstream stages communicate through documented Kafka topics rather than private service-to-service HTTP calls.

---

## Reasoning

### Decoupling and buffering

The producer can publish through downstream slowdowns/restarts while consumers catch up independently from retained offsets.

### Fan-out

`position.normalized` can be consumed independently by Correlation Worker and Deviation Detector without the Position Consumer knowing their addresses or synchronizing their execution.

### Replay

Kafka retention supports two explicit modes:

- crash recovery: resume from committed offsets and process normally;
- intentional backfill: use a separate consumer group/mode for an explicitly rebuildable target while suppressing live side effects.

Replay is **not** a promise that every derived store can be reconstructed by rewinding its live service unchanged. For example, a total Neo4j graph-loss rebuild requires dedicated offline recomputation because historical proximity detection depends on pair/spatial state; see US-10.

### DLQ

Malformed raw records can be preserved on source-specific DLQ topics with rejection metadata instead of being silently dropped or crashing the consumer.

---

## Alternatives Considered

### Direct HTTP POST — rejected

- couples producer throughput/availability to consumers;
- requires producer-side retry/buffering;
- poor fan-out model;
- no retained offset replay.

### RabbitMQ — rejected

A queue/exchange model can provide messaging, but retained log replay and independent offset-based consumers are more natural for Sentinel's streaming/recovery learning goals.

---

## Consequences

- Broker availability becomes a core infrastructure dependency.
- Consumers must assume at-least-once processing.
- Durable side effects require deterministic idempotency identities (ADR-007).
- Historical backfill behavior must be target-specific and isolated from the live path.
