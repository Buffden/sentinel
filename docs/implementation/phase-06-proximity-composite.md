# Phase 06 — Proximity + Composite Alerts

## Goal

The Alert Evaluator gains two Neo4j-backed rules: unscheduled proximity (US-05) and composite alerts (US-06). The composite rule correlates a signal loss event with an unscheduled proximity event in the same window and emits a single elevated alert instead of two individual ones.

## Dependencies

- Phase 04 (alert evaluator running with leader election and `alert-state` keys in Redis)
- Phase 05 (correlation worker populating `PROXIMITY_EVENT` edges in Neo4j)

## Tasks

### Unscheduled Proximity (US-05)

- [ ] On each evaluation cycle, query Neo4j for `PROXIMITY_EVENT` edges written since last cycle without a `KNOWN_ASSOCIATE` between the pair
- [ ] Before emitting: check whether a composite condition exists for the same event (see below) — if so, skip individual emission
- [ ] Publish: `{ alert_id, alert_type: UNSCHEDULED_PROXIMITY, entity_a, entity_b, location, distance_metres, timestamp_ms }`
- [ ] `alert_id`: `{entity_a}:{entity_b}:UNSCHEDULED_PROXIMITY:{window_start_ms}`

### Composite Alert (US-06)

- [ ] On each evaluation cycle, for each entity with an active `alert-state` key:
  - Read `dark_since_ms` from `alert-state:{entity_id}` (the value of the key)
  - Query Neo4j for `PROXIMITY_EVENT` edges involving the dark entity since `dark_since_ms`, excluding known associates
  - If results found: both conditions met
    - Emit ONE composite alert — suppress both the individual `SIGNAL_LOSS` and `UNSCHEDULED_PROXIMITY` for this event
    - Publish: `{ alert_id, alert_type: COMPOSITE, priority: ELEVATED, entity_b (dark entity), entity_a (proximity entity), correlation_window_ms, window_start_ms, location, timestamp_ms }`
    - `alert_id`: `{entity_id}:COMPOSITE:{window_start_ms}`

## Done When

- `UNSCHEDULED_PROXIMITY` alert emitted when two unrelated entities are within threshold
- No alert emitted for a pair with a `KNOWN_ASSOCIATE` edge
- When a dark entity has an unscheduled proximity within the loss window: ONE `COMPOSITE` alert on the dashboard, no individual `SIGNAL_LOSS` or `UNSCHEDULED_PROXIMITY` for that event
- When only one condition is met (signal loss with no proximity, or proximity with no signal loss): individual alert emitted, no composite
