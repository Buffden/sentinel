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

**Update (Pre-CP5B):** the sequence above assumes the referenced individual alert already exists when `COMPOSITE` arrives. It does not always: the signal-loss scan and the proximity-candidate consumer run concurrently in the Alert Evaluator, so `COMPOSITE` can reach the API before the `SIGNAL_LOSS` it references does. That case, the pending-supersession table and advisory-lock protocol that make it converge to the identical durable state regardless of arrival order, and the rule that post-commit publication must republish canonical current state on every delivery rather than only what a given attempt mutated, is resolved in full in `DATA_MODEL.md`'s "Pre-CP5B: composite supersession convergence protocol". This ADR's decision to keep durable alert state in PostgreSQL and to publish only after the DB transaction commits is unchanged; Pre-CP5B extends the mechanism, it doesn't revise the decision.

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
- Composite supersession is transactional, including the out-of-order arrival case (Pre-CP5B).
- Clients must tolerate duplicate lifecycle notifications, and must merge them monotonically: WebSocket delivery ordering between separately committed transactions is not guaranteed, so a stale `NEW` event can arrive after `SUPERSEDED`; `GET /alerts` remains the durable reconciliation source.
- Tests must cover replay after DB write/before Kafka offset commit, ACKNOWLEDGED → SUPERSEDED behavior, both arrival orders for composite supersession, concurrent-instance convergence under the advisory-lock protocol, and a Redis-publish failure between a composite's multiple post-commit publishes.
