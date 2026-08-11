# US-13: Alert Lifecycle Management

**Actor:** Operator
**Status:** Defined

---

## Story

As an operator, I want to acknowledge and resolve alerts so that active investigation state is durable and visible, while system correlation can replace weaker active alerts with a stronger composite incident.

---

## Acceptance Criteria

- Every persisted alert starts in `NEW`.
- An operator may transition `NEW → ACKNOWLEDGED`.
- An operator may transition `ACKNOWLEDGED → RESOLVED`.
- Direct `NEW → RESOLVED` is allowed.
- `SUPERSEDED` is system-only and terminal.
- A stronger COMPOSITE may transition an active individual alert from either `NEW` or `ACKNOWLEDGED` to `SUPERSEDED`.
- `RESOLVED` alerts are historical and are not superseded later.
- If an anomaly recurs after resolution, a new deterministic `alert_id` is created for the new episode; the old row is never reopened or overwritten.
- Repeating an already-applied operator transition is idempotent.
- Lifecycle state survives restarts because it is stored durably in TimescaleDB.

---

## Flow Diagrams

### State Transitions

![State Transitions](../../../diagrams/docs/use-cases/US-13-alert-lifecycle/state-transitions.svg)

`NEW` and `ACKNOWLEDGED` are active states. `RESOLVED` and `SUPERSEDED` are terminal states. Supersession is a system correlation outcome, not an operator action.

### Acknowledge Flow

![Acknowledge Flow](../../../diagrams/docs/use-cases/US-13-alert-lifecycle/acknowledge-flow.svg)

The API persists acknowledgement metadata and broadcasts an `ALERT_STATUS_CHANGED` event so connected clients converge on the same lifecycle state.

### Resolution and Later Recurrence

![Resolution and Later Recurrence](../../../diagrams/docs/use-cases/US-13-alert-lifecycle/resolve-and-reopen.svg)

A resolved alert remains immutable historical evidence. If the anomaly occurs again in a new episode/window, the Alert Evaluator emits a new deterministic alert identity and the API inserts a new `NEW` row.

---

## Architectural Justification

Justifies: [ADR-010 - Alert State Store](../../adr/ADR-010-alert-state-store.md)

Alert lifecycle state is durable, mutable, and queryable. A regular PostgreSQL table on the TimescaleDB instance provides the required transactionality for composite supersession and the indexes needed for operator workflows, while Redis remains dedicated to ephemeral in-loop anomaly state and pub/sub fan-out.
