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
- **TimescaleDB:** `ON CONFLICT (entity_id, timestamp_ms) DO NOTHING` — duplicate inserts are silently ignored
- **Neo4j:** `MERGE` on the idempotency key as a node/edge property — creates if absent, does nothing if present
- **Redis live state:** `HSET` is idempotent only when the same data is written twice. It does NOT protect against regressions: if a newer position has already been written and an older replayed event arrives, `HSET` will overwrite the newer value with the older one. A **timestamp guard** is required before any Redis live-state write: check the stored `last_seen_ms`; only write if `incoming.timestamp_ms > stored.last_seen_ms`. See Phase 02 for the guard implementation and ADR-007 for why.

**Two replay modes:**

The above idempotency guarantees apply differently depending on the replay context:

- **Crash recovery** (normal): the consumer resumes from its last committed Kafka offset. Normal processing applies — writes to TimescaleDB, Redis, Neo4j all proceed with the standard idempotency mechanisms above. Redis timestamp guard prevents state regressions from re-delivered events.
- **Historical backfill** (intentional): replaying from a much earlier offset to rebuild a specific store (e.g. Neo4j after a restart, TimescaleDB history after a schema migration). This mode runs under a separate consumer group or explicit `--mode=backfill` flag and **disables ephemeral side effects** that must not be triggered by historical data: Redis live-state overwrite, `position-updates` pub/sub, `deviation.candidates`, `proximity.candidates`, alerts. Only the target store is written. Mixing live and historical replay in the same consumer group is incorrect.

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

- All consumers must extract `entity_id` and `timestamp_ms` from the normalised event before writing — these fields must be present in the `position.normalized` Kafka message schema
- The key format `{entity_id}:{timestamp_ms}` is the **logical** idempotency key used across services. The TimescaleDB unique constraint is `(entity_id, timestamp_ms)`. The hypertable partition column is `observed_at TIMESTAMPTZ` — these are consistent: two events with the same `entity_id` and `timestamp_ms` always produce the same `observed_at`.
- **Pair-based writes** (proximity episodes) use a canonical pair key: `{min(a,b)}:{max(a,b)}:{episode_start_ms}`. This is the Neo4j PROXIMITY_EVENT edge idempotency key.
- Clock skew on the data source side could theoretically produce two events with the same timestamp for the same entity — treated as acceptable and handled by `DO NOTHING` / `MERGE` (the second write is dropped, which is correct for a duplicate).
- Redis live-state writes are NOT unconditionally idempotent. The timestamp guard (`incoming.timestamp_ms > stored.last_seen_ms`) must be applied before any `HSET entity:live:*` call to prevent out-of-order events from regressing live state.
- Historical backfill replay must use a separate consumer group and suppress side effects as documented above.
