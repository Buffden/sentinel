# ADR-004: Redis for Live Entity State

**Status:** Accepted
**Date:** 2026-08-05

---

## Context

The dashboard and alert evaluator both need the current position of every tracked entity with the lowest possible read latency. This is the highest-frequency read in the system  - every map refresh and every alert evaluation reads current state for potentially thousands of entities.

The source of truth for position history is TimescaleDB, but querying it for the latest row per entity on every dashboard tick is expensive and adds unnecessary load to the primary store.

---

## Decision

Use Redis as a live entity state cache. The position consumer writes the current position to Redis on every ingest, keyed as `entity:live:{entity_id}`.

---

## Reasoning

**Latency.** Redis sub-millisecond reads are the right fit for the highest-frequency read in the system. TimescaleDB is optimised for range queries, not point lookups at high frequency.

**Cache, not source of truth.** Redis holds only the latest position. If Redis is lost, the system re-warms from Kafka (replay) or TimescaleDB (latest row per entity). No data is permanently lost.

**Simple key schema.** `entity:live:{entity_id}` is a flat key pointing to a hash of position fields including `last_seen_ms`. No complex data structures required — straightforward GET/HGETALL on read, HSET on write.

**`last_seen_ms` for signal loss detection.** The hash stores `last_seen_ms` (Unix timestamp in milliseconds from the source telemetry). The alert evaluator runs on a fixed schedule and scans `entity:live:*` keys, flagging any where `now() - last_seen_ms > SIGNAL_LOSS_THRESHOLD_MS`. This is intentionally separate from the Redis TTL — the TTL controls dashboard ghost cleanup, not alert evaluation.

**TTL for dashboard ghost cleanup.** Entities that stop broadcasting have their Redis key expired automatically via TTL (set to `SIGNAL_LOSS_THRESHOLD_MS`). Once the key expires, the dashboard stops receiving position updates for that entity and its client-side staleness timer removes it from the map. The TTL expiry does not trigger alert detection — `last_seen_ms` does.

**Pub/sub for live position push.** The position consumer publishes every normalised position event to the `position-updates` Redis pub/sub channel after writing to the hash. All API instances subscribe to this channel and fan out updates to their in-memory WebSocket connections after applying per-operator scope filtering. This is what enables horizontal scaling of the API layer without a separate pub/sub infrastructure component — Redis serves both the hash cache and the broadcast channel.

---

## Alternatives Considered

### Query TimescaleDB directly for latest position (rejected)
- Correct, but expensive  - "latest row per entity" is a `DISTINCT ON` or window function query, not a point lookup
- Adds read load to the primary time-series store
- Higher latency than Redis for dashboard refresh rates

### Memcached (rejected)
- No native TTL per key (Redis has it)
- No hash data structure  - would require serialising the full position object to a string
- Less operational tooling and observability than Redis

---

## Consequences

- Redis is a cache, not a store — the position consumer must write to both TimescaleDB (durable) and Redis (fast read) on every ingest event
- Redis data is lost on restart unless persistence (RDB/AOF) is enabled — for v1, this is acceptable; the cache re-warms on next ingest
- The key schema `entity:live:{entity_id}` must be used consistently across all writers and readers — documented in CLAUDE.md conventions
- The hash must include `last_seen_ms` on every write — the alert evaluator depends on this field for signal loss detection
- The `position-updates` pub/sub channel must be published to by the position consumer on every ingest — all API instances subscribe to it for live WebSocket push
- TTL on `entity:live:{entity_id}` is set to `SIGNAL_LOSS_THRESHOLD_MS` — it drives dashboard ghost cleanup only, not alert detection
