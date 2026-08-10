# Phase 06 — Proximity + Composite Alerts

## Goal

The Alert Evaluator gains two stream-sourced rules: unscheduled proximity (US-05) and composite alerts (US-06). Proximity inputs arrive from the Correlation Worker via `proximity.candidates` — the evaluator no longer polls Neo4j for recent edges. The composite rule correlates a signal loss event with an unscheduled proximity event in the same window and emits a single elevated alert instead of two individual ones.

## Dependencies

- Phase 04 (alert evaluator running with leader election and `alert-state` keys in Redis)
- Phase 05 (correlation worker publishing `PROXIMITY_EVENT` edges to Neo4j and `proximity.candidates` events to Kafka)

## Tasks

### Unscheduled Proximity (US-05)

- [ ] Consume `proximity.candidates` (consumer group: `alert-evaluator`)
- [ ] On each event: check whether a composite condition exists for the same entity pair (see below) — if so, suppress individual emission
- [ ] Publish: `{ alert_id, alert_type: UNSCHEDULED_PROXIMITY, entity_a, entity_b, location, distance_metres, timestamp_ms }`
- [ ] `alert_id`: `{entity_a}:{entity_b}:UNSCHEDULED_PROXIMITY:{window_start_ms}`

### Composite Alert (US-06)

- [ ] On each `proximity.candidates` event, check whether the proximity entity has an active `alert-state:{entity_id}` key in Redis (signal loss active)
  - Read `dark_since_ms` from `alert-state:{entity_id}` (the value of the key)
  - If the proximity event `timestamp_ms` falls within the signal loss window: both conditions met
    - Query Neo4j for the full proximity window start time and relationship context (targeted lookup on the specific entity pair — not a scan; see ADR-014)
    - Emit ONE composite alert — suppress both the individual `SIGNAL_LOSS` and `UNSCHEDULED_PROXIMITY` for this event
    - Publish: `{ alert_id, alert_type: COMPOSITE, priority: ELEVATED, entity_b (dark entity), entity_a (proximity entity), correlation_window_ms, window_start_ms, location, timestamp_ms }`
    - `alert_id`: `{entity_id}:COMPOSITE:{window_start_ms}`

## Done When

- `UNSCHEDULED_PROXIMITY` alert emitted when a `proximity.candidates` event arrives for a pair with no active composite condition
- No alert emitted for a pair with a `KNOWN_ASSOCIATE` edge (filtered upstream by the correlation worker before reaching `proximity.candidates`)
- When a `proximity.candidates` event arrives for a dark entity: ONE `COMPOSITE` alert on the dashboard, no individual `SIGNAL_LOSS` or `UNSCHEDULED_PROXIMITY` for that event
- When only one condition is met (signal loss with no proximity, or proximity with no signal loss): individual alert emitted, no composite
