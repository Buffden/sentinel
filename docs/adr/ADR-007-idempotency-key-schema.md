# ADR-007: Deterministic Idempotency Identity

**Status:** Accepted
**Date:** 2026-08-05

---

## Context

Kafka delivery is at-least-once. Consumers may process the same logical event more than once because of crashes, rebalances, retries, or explicit replay. Sentinel therefore requires deterministic identity for every replay-sensitive durable effect.

There is no single key shape that correctly identifies every domain object. Position pings, proximity episodes, and alerts have different natural identities.

---

## Decision

Use a deterministic identity derived from source event time and the domain object's canonical identity.

### Position events

Logical identity:

```text
{entity_id}:{timestamp_ms}
```

TimescaleDB enforces this through the unique constraint `(entity_id, observed_at)`, where `observed_at = to_timestamp(timestamp_ms / 1000.0)`.

### Proximity episodes

Canonical pair:

```text
{min(entity_a_id, entity_b_id)}:{max(entity_a_id, entity_b_id)}
```

Neo4j `PROXIMITY_EVENT` identity:

```text
{pair_key}:{episode_start_ms}
```

### Alerts

Alert identity is deterministic **per alert type**:

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

The same logical anomaly therefore produces the same `alert_id` on replay.

---

## Reasoning

**Identity follows the domain object.** A position ping belongs to one entity at one source timestamp. A proximity episode belongs to a canonical entity pair and an episode start. An alert belongs to the anomaly episode that caused it.

**Source event time makes replay deterministic.** Episode/window anchors come from telemetry-derived event time, not processing time. Reprocessing the same source facts therefore recreates the same identity.

**Canonical pair ordering prevents symmetric duplicates.** `(A,B)` and `(B,A)` must resolve to the same `pair_key` everywhere.

---

## Per-Store Application

### TimescaleDB position history

```sql
INSERT ...
ON CONFLICT (entity_id, observed_at) DO NOTHING
```

TimescaleDB requires the hypertable partition column in unique constraints, so `(entity_id, observed_at)` is the physical constraint while `{entity_id}:{timestamp_ms}` remains the logical identity.

### TimescaleDB alerts

`alert_id` is the primary key. Kafka replay may deliver the alert again, but `INSERT ... ON CONFLICT DO NOTHING` prevents a second durable alert row.

### Neo4j

`MERGE` on deterministic node/relationship identity prevents duplicate graph evidence under replay.

### Redis live state

Redis live state is not protected by a duplicate key alone. An older replayed event can overwrite a newer value, so writes require a timestamp guard:

```text
accept if incoming.timestamp_ms >= stored.last_seen_ms
reject if incoming.timestamp_ms < stored.last_seen_ms
```

The same timestamp may be accepted as an idempotent repeat, but an older timestamp must never regress either `entity:live:*` or its `geo-cell:*` spatial membership.

---

## Delivery Semantics

Sentinel does **not** claim exactly-once transport.

- Kafka processing is at-least-once.
- Redis pub/sub / WebSocket delivery may repeat after replay.
- Durable writes are idempotent, producing an exactly-once **durable effect** for the same logical identity.
- Dashboard clients deduplicate repeated alert delivery by `alert_id`.

Leader election reduces concurrent Alert Evaluator writers; deterministic identity remains the correctness backstop if replay or a lease race causes the same logical alert to be emitted again.

---

## Replay Modes

### Crash recovery

A consumer resumes from the last committed offset. Normal processing applies. Duplicate processing is safe because durable side effects are idempotent and Redis live state is timestamp guarded.

### Historical backfill

Historical rebuilds use a separate consumer group or explicit backfill mode and suppress ephemeral/live side effects unless they are the target of the rebuild. In particular, historical replay must not blindly republish live WebSocket events, regress Redis live state, or recreate historical anomaly notifications as if they were current.

---

## Alternatives Considered

### Processing-time UUIDs (rejected)

The same event would produce a different identity on every attempt, defeating replay deduplication.

### Kafka offset as domain identity (rejected)

Offsets are partition-specific broker positions, not stable business identity. They also do not survive topic recreation/repartitioning as a domain contract.

### One universal `{entity_id}:{timestamp_ms}` key (rejected)

It does not correctly identify pair-based proximity episodes or composite/proximity alerts involving two entities.

---

## Consequences

- `position.normalized` must preserve source `timestamp_ms`.
- Pair-based processing must canonicalize IDs before creating Redis, Neo4j, Kafka, or alert identities.
- Alert producers must use the exact type-specific `alert_id` formats above.
- `alerts.alert_id` is the durable deduplication key.
- Redis live-state and geo-cell writes require the same timestamp guard so a stale event cannot update one without the other.
- Tests must distinguish duplicate transport from duplicate durable effects.
