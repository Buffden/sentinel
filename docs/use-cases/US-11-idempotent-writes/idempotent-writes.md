# US-11: Idempotent Writes Under Replay

**Actor:** System
**Status:** Defined

---

## Story

As the system, I want every write to every store to be safe to repeat so that replaying Kafka events or restarting consumers never produces duplicate or inconsistent records.

---

## Acceptance Criteria

- Writing the same position event to TimescaleDB twice produces exactly one row (`ON CONFLICT (entity_id, timestamp_ms) DO NOTHING`)
- Writing the same proximity episode to Neo4j twice produces exactly one edge (`MERGE` on `{pair_key}:{episode_start_ms}`)
- Writing an older event to Redis after a newer one has been stored does NOT overwrite the newer state — the timestamp guard (`incoming.timestamp_ms > stored.last_seen_ms`) is checked before every `HSET entity:live:*` write
- The TimescaleDB and Neo4j idempotency guarantees require no coordination between instances — enforced by the write operation itself
- The Redis timestamp guard requires a read-before-write; use a Lua script if atomicity is required

---

## Flow Diagrams

### Per-Store Idempotency

![Per-Store Idempotency](../../../diagrams/docs/use-cases/US-11-idempotent-writes/per-store-idempotency.svg)

Shows how each store enforces idempotency using a different mechanism: `ON CONFLICT DO NOTHING` in TimescaleDB, `MERGE` on the idempotency key in Neo4j, and natural overwrite semantics via `HSET` in Redis.

### Duplicate Delivery

![Duplicate Delivery](../../../diagrams/docs/use-cases/US-11-idempotent-writes/duplicate-delivery.svg)

End-to-end scenario where Kafka redelivers the same event twice after a consumer restart; the Position Consumer's writes to TimescaleDB and Redis are each a no-op on the second delivery, and the Correlation Worker applies the same guarantee for Neo4j - all without coordination between instances.

---

## Architectural Justification

Justifies: [ADR-007 - Idempotency Key Schema](../../adr/ADR-007-idempotency-key-schema.md)

Kafka provides at-least-once delivery — every consumer must handle duplicate message delivery. The idempotency key `{entity_id}:{timestamp_ms}` is derived deterministically from the source event. This key is used as a conflict target in TimescaleDB (`ON CONFLICT DO NOTHING`) and as a MERGE key in Neo4j. Redis live state is NOT unconditionally idempotent: `HSET` with an older value overwrites a newer one. A timestamp guard (`incoming.timestamp_ms > stored.last_seen_ms`) is required before every Redis live-state write to prevent out-of-order or replayed events from regressing the current position. Pair-based events (proximity) use a canonical pair key `{min(a,b)}:{max(a,b)}:{episode_start_ms}` for Neo4j idempotency.
