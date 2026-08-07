# US-11: Idempotent Writes Under Replay

**Actor:** System
**Status:** Defined

---

## Story

As the system, I want every write to every store to be safe to repeat so that replaying Kafka events or restarting consumers never produces duplicate or inconsistent records.

---

## Acceptance Criteria

- Writing the same position event to TimescaleDB twice produces exactly one row
- Writing the same proximity event to Neo4j twice produces exactly one edge
- Writing the same entity state to Redis twice has no observable side effect
- The idempotency guarantee requires no coordination between consumer instances - it is enforced by the write operation itself

---

## Flow Diagrams

**Per-store idempotency** - shows how each store enforces idempotency using a different mechanism: `ON CONFLICT DO NOTHING` in TimescaleDB, `MERGE` on the idempotency key in Neo4j, and natural overwrite semantics via `HSET` in Redis.

**Duplicate delivery** - end-to-end scenario where Kafka redelivers the same event twice after a consumer restart; all three stores handle the duplicate write as a no-op with no coordination required.

---

## Architectural Justification

Justifies: [ADR-007 - Idempotency Key Schema](../../adr/ADR-007-idempotency-key-schema.md)

Kafka provides at-least-once delivery - every consumer must handle duplicate message delivery. The idempotency key `{entity_id}:{timestamp_ms}` is derived deterministically from the source event, meaning the same event always produces the same key regardless of how many times it is processed. This key is used as a conflict target in TimescaleDB (`ON CONFLICT DO NOTHING`), as a MERGE key in Neo4j, and is naturally idempotent in Redis via HSET. No coordination between consumer instances is required.
