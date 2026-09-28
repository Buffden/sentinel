# US-03: Signal Loss Alert

**Actor:** Operator
**Status:** Defined

---

## Story

As an operator, I want an alert when an entity has not broadcast within its configured liveness threshold so that I can investigate the loss of telemetry.

---

## Acceptance Criteria

- Alert Evaluator scans Redis `entity:live:*` on a schedule. Signal loss is judged on **observed silence**: the accumulated coverage time of the entity's owning provider (its `provider` field) since `last_seen_ms`, read from the `{live-provider}` coverage timeline (ADR-022). Wall-clock time since `last_seen_ms` is not the rule.
- `SIGNAL_LOSS` fires only when observed silence reaches the threshold. Gaps when the provider was down, or the ingestion pipeline was stopped, contribute nothing, so an outage or a restart after downtime cannot by itself make aircraft look dark.
- Covered silence accumulated before an outage stays accumulated after it. An aircraft that remains absent can therefore alert after less than one threshold of renewed coverage, if it had already built up covered silence before the outage. The invariant is one full threshold of total observed coverage since `last_seen_ms`.
- A provider with no coverage timeline cannot generate signal-loss alerts: an entity whose provider is missing, unknown or uncovered observes no silence. The same holds while the timeline is uninitialized or cannot be read.
- Grounded entities (`on_ground` is `true`) remain excluded.
- `dark_since_ms` remains the source-time `last_seen_ms`.
- Signal-loss thresholds are configurable by entity type.
- Redis key TTL is a 24h safety net and is deliberately longer than the signal-loss threshold; TTL expiry is not the detector.
- A signal-loss episode emits one deterministic SIGNAL_LOSS logical alert keyed by `{entity_id}:SIGNAL_LOSS:{dark_since_ms}`.
- `alert-state:{entity_id}` suppresses repeated emission while the entity remains dark.
- The alert's last-known position and callsign are read from the entity's Redis live state (`entity:live:{entity_id}`) at scan time. Signal-loss evaluation does not read TimescaleDB.
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

Justifies: [ADR-004 - Redis Live State](../../adr/ADR-004-redis-live-state.md), [ADR-005 - Leader Election](../../adr/ADR-005-leader-election-alert-evaluator.md), [ADR-010 - Alert State Store](../../adr/ADR-010-alert-state-store.md), [ADR-014 - Hybrid Input Model](../../adr/ADR-014-alert-evaluator-hybrid-input-model.md), [ADR-022 - Live Provider Health and Failover](../../adr/ADR-022-live-provider-health-and-failover.md)

Redis is the correct detector input because the rule asks how long the latest live timestamp has gone unrefreshed while its provider was provably covering. Both the live timestamp and the coverage timeline are in Redis. TimescaleDB is not read during signal-loss evaluation: the last known position in the alert comes from the same live hash.
