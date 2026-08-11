# ADR-010: Alert State Store for Lifecycle Management

**Status:** Accepted
**Date:** 2026-08-07

---

## Context

Alert events are produced by the Alert Evaluator and consumed by the API. Operator workflow requires durable state beyond transient Kafka delivery or Redis detection-loop state.

Required lifecycle states:

- `NEW`
- `ACKNOWLEDGED`
- `RESOLVED`
- `SUPERSEDED`

The state must survive restarts, support indexed queries, remain replay-safe, and allow composite correlation to replace active individual alerts atomically.

---

## Decision

Store alert lifecycle state in a regular PostgreSQL table on the existing TimescaleDB instance.

The table is not a hypertable because alert volume is low relative to position telemetry and lifecycle access is relational rather than chunk-oriented.

The API owns all durable alert writes:

- initial insert when consuming `alerts` from Kafka;
- operator acknowledgement/resolution;
- atomic composite insertion + active individual-alert supersession.

---

## Deterministic Alert Identity

Use type-specific deterministic IDs:

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

`pair_key = min(a,b):max(a,b)`.

This prevents pair-alert collisions and makes Kafka replay converge on one durable row via `INSERT ... ON CONFLICT DO NOTHING`.

---

## Lifecycle Rules

Operator transitions:

```text
NEW → ACKNOWLEDGED → RESOLVED
NEW → RESOLVED
```

System composite replacement:

```text
NEW → SUPERSEDED
ACKNOWLEDGED → SUPERSEDED
```

`RESOLVED` and `SUPERSEDED` are terminal.

A resolved alert is never reopened. A recurring anomaly creates a new episode/window identity and therefore a new alert row.

When a COMPOSITE is consumed, the API performs one DB transaction:

1. insert the COMPOSITE idempotently;
2. update every referenced active individual alert (`NEW` or `ACKNOWLEDGED`) to `SUPERSEDED`;
3. set `superseded_by = composite_alert_id`.

A referenced alert that is already `RESOLVED` remains resolved.

---

## Delivery Semantics

Kafka processing is at-least-once. The alerts table provides an idempotent exactly-once **durable effect**, not exactly-once transport.

After the DB transaction, the API publishes lifecycle updates to Redis `alert-events` so every API instance can fan them out to local WebSocket clients.

WebSocket lifecycle delivery is at-least-once. Duplicate lifecycle messages are allowed; clients converge by `alert_id` and current durable status/version semantics.

---

## Why TimescaleDB/PostgreSQL

- durable across application/Redis restart;
- indexed filtering by status/entity/type/time;
- transactional composite supersession;
- no additional infrastructure;
- low alert write volume fits a plain PostgreSQL table.

Redis remains appropriate for ephemeral in-loop state such as `alert-state`, `recent-loss`, and `deviation-state`, but not for durable operator lifecycle history.

---

## Consequences

- API is the only writer of durable alert lifecycle state.
- Alert Evaluator never reads lifecycle status to perform operator workflow.
- Composite supersession is transactional.
- Clients must tolerate duplicate lifecycle notifications.
- Tests must cover replay after DB write/before Kafka offset commit and ACKNOWLEDGED → SUPERSEDED behavior.
