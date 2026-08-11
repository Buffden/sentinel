# US-13: Alert Lifecycle Management

**Actor:** Operator / System
**Status:** Defined

---

## Story

As an operator, I want alerts to have durable lifecycle state so that I can distinguish unreviewed, actively investigated, resolved, and system-superseded incidents.

---

## Acceptance Criteria

- New alerts start as `NEW`.
- Operator may transition `NEW → ACKNOWLEDGED`.
- Operator may transition `ACKNOWLEDGED → RESOLVED`.
- Operator may also resolve directly with `NEW → RESOLVED`.
- The system may transition an active individual alert from `NEW → SUPERSEDED` or `ACKNOWLEDGED → SUPERSEDED` when a COMPOSITE replaces it.
- `RESOLVED` and `SUPERSEDED` are terminal.
- Operators cannot manually set `SUPERSEDED`.
- A recurring anomaly creates a new deterministic episode/window alert; terminal rows are never reopened or overwritten.
- All lifecycle operations are idempotent and durable across restarts.
- WebSocket lifecycle delivery is at-least-once; duplicate events must converge on the same durable status.

---

## Flow Diagrams

### State Transitions

![State Transitions](../../../diagrams/docs/use-cases/US-13-alert-lifecycle/state-transitions.svg)

The state machine distinguishes operator handling from system correlation replacement. A COMPOSITE may supersede either a NEW or ACKNOWLEDGED individual alert, but never a RESOLVED alert.

### Acknowledge Flow

![Acknowledge Flow](../../../diagrams/docs/use-cases/US-13-alert-lifecycle/acknowledge-flow.svg)

The operator acknowledges through the API; the API updates TimescaleDB and broadcasts the resulting lifecycle state.

### Resolve and Recurrence

![Resolve and Recurrence](../../../diagrams/docs/use-cases/US-13-alert-lifecycle/resolve-and-reopen.svg)

The historical filename is retained for diagram-link stability, but the behavior is **not reopening**: once an alert is resolved it remains terminal. A later anomaly episode produces a new `alert_id` and a new row.

---

## Architectural Justification

Justifies: [ADR-010 - Alert State Store](../../adr/ADR-010-alert-state-store.md)

The alerts table is the durable source of truth for lifecycle state. Redis alert-state keys serve detection-loop suppression/correlation and are not substitutes for durable operator workflow state.
