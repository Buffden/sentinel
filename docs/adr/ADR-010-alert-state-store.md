# ADR-010: Alert State Store for Lifecycle Management

**Status:** Accepted
**Date:** 2026-08-07

---

## Context

Alert events are delivered through Kafka, but operators need durable lifecycle state that survives restarts and supports querying, acknowledgement, resolution, and composite supersession.

Requirements:

- durable states: `NEW`, `ACKNOWLEDGED`, `RESOLVED`, `SUPERSEDED`;
- query by status, entity, type, and time;
- idempotent insertion under Kafka replay;
- atomic composite insertion + weaker-alert supersession;
- no separate database service for low alert volume.

---

## Decision

Store alert lifecycle state in a plain PostgreSQL table on the existing TimescaleDB instance.

The API is the sole writer of durable alert records:

- consumes canonical alert events from Kafka;
- inserts new alerts idempotently by deterministic `alert_id`;
- processes operator lifecycle updates;
- performs composite supersession transactionally;
- broadcasts resulting lifecycle events through Redis `alert-events` for WebSocket fan-out.

The table is not a hypertable because alert volume is low and lifecycle updates are relational/mutable rather than a high-rate append-only time series.

---

## State Machine

```text
NEW → ACKNOWLEDGED → RESOLVED
 │          │
 └──────────┴────→ SUPERSEDED
```

- `NEW` and `ACKNOWLEDGED` are active states.
- `RESOLVED` is an operator terminal state.
- `SUPERSEDED` is a system-only terminal state used when a stronger COMPOSITE incident replaces an active individual alert.
- A resolved alert is never reopened. A later anomaly episode creates a new deterministic `alert_id` and a new row.

---

## Deterministic Identity

Alert identity is defined per anomaly type in ADR-007 and `DATA_MODEL.md`.

Examples:

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

`alert_id` is the primary key, so replaying the same logical alert produces an `ON CONFLICT DO NOTHING` durable no-op.

---

## Composite Supersession

When the API consumes a COMPOSITE alert, one database transaction:

1. inserts the COMPOSITE idempotently;
2. updates each referenced individual alert that is still active (`NEW` or `ACKNOWLEDGED`) to `SUPERSEDED`;
3. sets `superseded_by` to the composite `alert_id`;
4. commits atomically.

After commit, the API publishes the appropriate `ALERT_CREATED` / `ALERT_SUPERSEDED` events to Redis pub/sub.

If a referenced alert is already `RESOLVED`, it remains resolved historical evidence rather than being rewritten.

---

## Delivery Semantics

Kafka and Redis/WebSocket delivery are not exactly-once.

The API consume order is:

```text
1. durable DB write / transaction
2. Redis alert-events publish
3. Kafka offset commit
```

A crash before step 3 can replay the same alert and republish a WebSocket event. The database remains duplicate-free because of deterministic identity; clients deduplicate repeated delivery by `alert_id`.

This is at-least-once transport with idempotent durable effects.

---

## Alternatives Considered

### Redis as the alert store (rejected)

Redis is appropriate for ephemeral anomaly-loop state and pub/sub, but durable mutable alert history requires stronger relational querying and transaction semantics.

### Dedicated PostgreSQL instance (rejected)

Correct at larger scale/isolation requirements, but unnecessary for v1 alert volume when TimescaleDB already provides PostgreSQL tables and transactions.

### Kafka-only state projection (rejected)

Would require a separate projection/compaction/CQRS layer for current lifecycle state with no benefit at this scale.

### Dedicated alert-state microservice (rejected)

Adds a deployment/failure boundary without improving the current access pattern; operator lifecycle already belongs at the API boundary.

---

## Consequences

- `alerts` is the source of truth for durable current lifecycle state.
- Redis `alert-state:*`, `recent-loss:*`, and `deviation-state:*` remain separate ephemeral anomaly-evaluation state.
- The API owns lifecycle transitions and composite supersession transactions.
- WebSocket clients must tolerate repeated lifecycle delivery.
- Tests must verify both replay-safe durable insertion and atomic supersession from active states.
