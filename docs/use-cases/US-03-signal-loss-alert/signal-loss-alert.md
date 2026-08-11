# US-03: Signal Loss Alert

**Actor:** Operator
**Status:** Defined

---

## Story

As an operator, I want an alert when an entity has not broadcast within its configured liveness threshold so that I can investigate the loss of telemetry.

---

## Acceptance Criteria

- Alert Evaluator scans Redis `entity:live:*` on a schedule and compares current time with `last_seen_ms`.
- Signal-loss thresholds are configurable by entity type.
- Redis key TTL is a 24h safety net and is deliberately longer than the signal-loss threshold; TTL expiry is not the detector.
- A signal-loss episode emits one deterministic SIGNAL_LOSS logical alert keyed by `{entity_id}:SIGNAL_LOSS:{dark_since_ms}`.
- `alert-state:{entity_id}` suppresses repeated emission while the entity remains dark.
- Last-known position is read from TimescaleDB only for alert evidence/payload assembly.
- When the entity resumes, the Position Consumer writes bounded `recent-loss:{entity_id}` state before deleting `alert-state` so Phase 06 composite correlation can still occur.
- Durable alert persistence is idempotent under Kafka replay.

---

## Flow Diagrams

### Detection

![Detection](../../../diagrams/docs/use-cases/US-03-signal-loss-alert/detection.svg)

Signal loss is an absence-of-events rule, so it remains a scheduled Redis scan rather than a Kafka-only detector.

### Alert Delivery

![Alert Delivery](../../../diagrams/docs/use-cases/US-03-signal-loss-alert/alert-delivery.svg)

This diagram shows **final v1 delivery**. Phase 03 proves authenticated delivery through the API instance that consumes the alert. Workspace scoping is added in Phase 07 and multi-instance Redis `alert-events` fan-out is completed in Phase 08.

### Alert Suppression

![Alert Suppression](../../../diagrams/docs/use-cases/US-03-signal-loss-alert/alert-suppression.svg)

`alert-state` is detection-loop state, separate from the durable `alerts` table. The Redis key prevents repeated rule emission; the database prevents duplicate durable rows under replay.

---

## Architectural Justification

Justifies: [ADR-004 - Redis Live State](../../adr/ADR-004-redis-live-state.md), [ADR-005 - Leader Election](../../adr/ADR-005-leader-election-alert-evaluator.md), [ADR-010 - Alert State Store](../../adr/ADR-010-alert-state-store.md), [ADR-014 - Hybrid Input Model](../../adr/ADR-014-alert-evaluator-hybrid-input-model.md)

Redis is the correct detector input because the rule asks whether the latest live timestamp is too old. TimescaleDB is not scanned to discover signal loss; it is consulted only when assembling historical evidence such as the last known position.
