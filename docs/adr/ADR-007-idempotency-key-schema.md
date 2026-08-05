# ADR-007: Idempotency Key Schema

**Status:** Accepted
**Date:** 2026-08-05

---

## Context

Kafka provides at-least-once delivery guarantees. This means any consumer may process the same message more than once  - due to consumer restarts, rebalances, or explicit replay from an earlier offset. Every write to every store (TimescaleDB, Neo4j, Redis) must be safe to repeat without producing duplicate or inconsistent state.

---

## Decision

Every write in the pipeline uses a deterministic idempotency key of the form:

```
{entity_id}:{timestamp_ms}
```

Where `entity_id` is the entity's canonical identifier (ICAO hex for aircraft, MMSI for vessels) and `timestamp_ms` is the Unix timestamp in milliseconds from the original telemetry message.

---

## Reasoning

**Deterministic from the source event.** The key is derived entirely from the original telemetry  - no random component, no write-time UUID. This means the same source event always produces the same key, regardless of how many times it is processed.

**Millisecond precision is sufficient.** ADS-B broadcasts at most once per ~0.5 seconds per entity; AIS at most once every few seconds. A millisecond-precision timestamp combined with entity_id produces a key that is unique per entity per broadcast  - collisions are not possible under normal operation.

**Per-store application:**
- **TimescaleDB:** `ON CONFLICT (entity_id, timestamp_ms) DO NOTHING`  - duplicate inserts are silently ignored
- **Neo4j:** `MERGE` on the idempotency key as a node/edge property  - creates if absent, does nothing if present
- **Redis:** `HSET` is naturally idempotent  - writing the same key/value twice has no effect

---

## Alternatives Considered

### UUID generated at processing time (rejected)
- Non-deterministic  - the same source event produces a different UUID on each processing attempt
- Cannot detect or deduplicate replays; every replay creates a new record
- Fundamentally incompatible with at-least-once semantics

### Sequence number from Kafka offset (rejected)
- Kafka offsets are per-partition and per-consumer-group  - not globally unique across partitions
- If a topic is repartitioned, offsets are reset  - historical idempotency keys become invalid
- Does not survive consumer group changes or topic recreation

### Content hash of the full event (rejected)
- More collision-resistant than timestamp alone, but more expensive to compute
- `entity_id + timestamp_ms` is already unique under normal ADS-B/AIS broadcast frequency  - the added complexity of hashing is not justified

---

## Consequences

- All consumers must extract `entity_id` and `timestamp_ms` from the normalised event before writing  - these fields must be present in the `position.normalized` Kafka message schema
- The key format `{entity_id}:{timestamp_ms}` must be used consistently across all services  - it is documented in CLAUDE.md conventions and enforced by code review
- Clock skew on the data source side could theoretically produce two events with the same timestamp for the same entity  - treated as acceptable and handled by `DO NOTHING` / `MERGE` (the second write is dropped, which is correct behaviour for a duplicate)
