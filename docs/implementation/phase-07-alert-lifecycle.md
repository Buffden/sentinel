# Phase 07 — Alert Lifecycle

## Goal

Operators can acknowledge and resolve alerts. Status changes are persisted as an audit trail in TimescaleDB. The dashboard alert panel reflects status changes in real time. Resolving an alert does not block re-detection — if the same entity triggers the same anomaly later, a fresh alert is created.

## Dependencies

- Phase 04 (alerts flowing into TimescaleDB and visible on dashboard)

## Tasks

### API

- [ ] `PATCH /alerts/:alert_id` — update alert status
  - `{ status: ACKNOWLEDGED }` → sets `acknowledged_at = now()`, `acknowledged_by = user_id`
  - `{ status: RESOLVED }` → sets `resolved_at = now()`, `resolved_by = user_id`
  - Invalid transitions rejected (e.g. RESOLVED → ACKNOWLEDGED not allowed)
- [ ] After status update: push updated alert state over WebSocket to scoped connections
- [ ] Alert records are never deleted — permanent audit trail

### Dashboard

- [ ] Acknowledge and Resolve buttons on each alert row in the alert panel
- [ ] Status change reflected immediately in the UI on API response
- [ ] Resolved alerts remain visible in a separate history view

### Re-detection Behaviour

- [ ] Resolving an alert only updates the `alerts` table row — no change to `alert-state:{entity_id}` in Redis
- [ ] `alert-state` is deleted only by the position consumer when the entity resumes broadcasting
- [ ] If the same entity triggers the same anomaly in a new time window: new `alert_id` with new `window_start_ms`, new row in `alerts` table — no reopen logic, just a fresh record

## Done When

- Operator can acknowledge and resolve alerts from the dashboard
- `acknowledged_at` and `resolved_at` are persisted correctly in TimescaleDB
- Status change pushed over WebSocket and reflected in the UI without a page refresh
- Resolved alert row remains in the database
- When the same entity goes dark again after resolution: a new alert row appears with a distinct `alert_id`
