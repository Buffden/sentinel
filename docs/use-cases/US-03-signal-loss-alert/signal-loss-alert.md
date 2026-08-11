# US-03: Signal Loss Alert

**Actor:** Operator
**Status:** Defined

---

## Story

As an operator, I want to receive an alert when an entity stops broadcasting beyond a configurable threshold so that I can investigate a potential signal-loss event.

---

## Acceptance Criteria

- Signal loss is detected from Redis `entity:live:*` using `last_seen_ms`, not Redis TTL expiry.
- Thresholds are configurable by entity type.
- One SIGNAL_LOSS alert is emitted per dark episode.
- The alert includes deterministic identity, last known position, and dark-since time.
- Alert persistence is replay-safe through deterministic `alert_id` + idempotent API insert.
- A later entity resume clears the active `alert-state:*` only after the Position Consumer writes `recent-loss:*` for the composite correlation window.

---

## Detection

![Detection](../../../diagrams/docs/use-cases/US-03-signal-loss-alert/detection.svg)

The leader Alert Evaluator periodically scans Redis live-state hashes and compares current time with `last_seen_ms`. A 24h Redis TTL is only a safety net; the key deliberately outlives the loss threshold so the evaluator can inspect it.

---

## Alert Delivery

![Alert Delivery](../../../diagrams/docs/use-cases/US-03-signal-loss-alert/alert-delivery.svg)

The final v1 flow is Kafka `alerts` → API → durable TimescaleDB alert row → Redis `alert-events` → all API instances → scope-matched WebSockets.

**Roadmap boundary:** Phase 03 proves the first vertical slice with one API instance and authenticated delivery. Cross-instance Redis fan-out is completed in Phase 08, while saved workspace scope and server-side filtering are introduced in Phase 07. The final-state diagram must not be interpreted as a requirement to implement those later capabilities during Phase 03.

Kafka/WebSocket transport can repeat after replay. The durable alert row is duplicate-free because `alert_id` is deterministic; clients deduplicate repeated delivery by `alert_id`.

---

## Alert Suppression

![Alert Suppression](../../../diagrams/docs/use-cases/US-03-signal-loss-alert/alert-suppression.svg)

After first emission, the evaluator writes `alert-state:{entity_id}` with `dark_since_ms`, `signal_loss_alert_id`, and `composite_issued=0`. Repeated scheduled scans skip re-emission for the same dark episode.

When the entity resumes, the Position Consumer writes `recent-loss:{entity_id}` before deleting `alert-state:{entity_id}`. This preserves a short correlation opportunity if an unscheduled proximity episode arrives shortly after resume.

---

## Architectural Justification

Signal-loss detection is an absence-of-events problem and therefore remains a scheduled Redis live-state scan (ADR-014). TimescaleDB is used only to retrieve the last-known historical position for the alert payload and to persist the resulting alert through the API.
