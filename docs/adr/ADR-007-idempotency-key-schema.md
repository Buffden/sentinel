# ADR-007: Deterministic Idempotency Identity

**Status:** Accepted
**Date:** 2026-08-05

---

## Context

Kafka processing is at-least-once. Consumers may process the same logical fact more than once because of retries, restarts, rebalances, or intentional replay. Sentinel therefore needs deterministic identities for every durable or replay-sensitive side effect.

A single universal key shape is not sufficient because Sentinel stores different kinds of logical facts: single-entity positions, pair episodes, and multiple alert types.

---

## Decision

Use a deterministic identity that matches the logical fact being written.

### Position history

Logical source identity:

```text
{entity_id}:{timestamp_ms}
```

TimescaleDB enforces this through unique `(entity_id, observed_at)`, where `observed_at` is deterministically derived from `timestamp_ms`. Duplicate inserts use `ON CONFLICT DO NOTHING`.

### Redis live state

Key identity is `entity_id`, but correctness additionally requires a monotonic source-time guard:

```text
incoming.timestamp_ms >= stored.last_seen_ms
```

A repeated equal-timestamp write is safe. An older event must never overwrite newer live state.

### Proximity episode / Neo4j evidence

```text
pair_key = min(a,b):max(a,b)
PROXIMITY_EVENT identity = {pair_key}:{episode_start_ms}
```

Neo4j writes use `MERGE` on this identity.

### Alerts

```text
SIGNAL_LOSS
{entity_id}:SIGNAL_LOSS:{dark_since_ms}

ROUTE_DEVIATION
{entity_id}:ROUTE_DEVIATION:{episode_start_ms}

UNSCHEDULED_PROXIMITY
{pair_key}:UNSCHEDULED_PROXIMITY:{episode_start_ms}

COMPOSITE
{pair_key}:COMPOSITE:{dark_since_ms}
```

Pair-based alert types include the canonical pair key so two simultaneous incidents involving the same primary entity but different counterparties cannot collide.

---

## Why This Is Not Exactly-Once Transport

Deterministic identity does not prevent Kafka from delivering an event twice. It makes repeated processing converge on one durable logical result.

Sentinel therefore describes its guarantees as:

- Kafka transport/processing: at-least-once;
- durable TimescaleDB/Neo4j side effects: idempotent, producing an exactly-once **effect** for one logical identity;
- Redis live state: monotonic by source event time;
- WebSocket lifecycle delivery: at-least-once; client deduplication required.

Leader election reduces concurrent duplicate alert evaluation but does not replace deterministic durable identity.

---

## Replay Modes

### Crash recovery

Consumer resumes from the last committed offset. Normal processing applies. Duplicate durable writes converge through the identities above; Redis source-time guards prevent live-state regression.

### Historical backfill

Historical rebuild uses a separate consumer group or explicit backfill mode. Ephemeral side effects are suppressed unless they are the specific rebuild target:

- no live Redis regression;
- no `position-updates` fan-out;
- no historical `deviation.candidates` / `proximity.candidates` into the live alert path;
- no alert re-notification solely because history is being rebuilt.

---

## Alternatives Considered

### Processing-time UUID — rejected

The same source fact receives a new identity on every retry, making deduplication impossible.

### Kafka offset as domain identity — rejected

Offsets are partition-local transport coordinates, not stable domain identities.

### One `{entity_id}:{timestamp_ms}` rule for every store — rejected

It cannot correctly identify pair episodes or pair-based alerts and can collide when one entity participates in multiple simultaneous pair incidents.

---

## Consequences

- Canonical pair ordering is required anywhere pair identity is used.
- Alert producers and database migrations must implement the per-type `alert_id` rules exactly.
- Tests must include duplicate delivery and out-of-order telemetry, not only duplicate equal-value writes.
- Documentation must say "exactly-once effect" only for idempotent durable side effects, never for the whole transport pipeline.
