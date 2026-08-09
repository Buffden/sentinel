# Phase 04 — Alert Pipeline: Signal Loss + Route Deviation

## Goal

The first two anomaly rules are live and visible. An operator on the dashboard sees a real-time alert panel populated by alerts flowing through Kafka. This phase proves the entire alert path: detection → Kafka → API → WebSocket → dashboard.

## Dependencies

- Phase 03 (auth + scoped WebSocket in place)

## Tasks

### Alert Evaluator (`services/alert-evaluator/`)

**Leader Election (ADR-005)**
- [ ] Node.js + TypeScript service
- [ ] On startup: `SET alert-evaluator:leader {instance_id} NX PX {LEADER_TTL_MS}`
- [ ] Leader runs the evaluation loop; followers stay warm and idle, polling for leader expiry
- [ ] Leader renews the lease on each heartbeat

**Signal Loss (US-03)**
- [ ] Scheduled scan every `SIGNAL_LOSS_SCAN_INTERVAL_MS` (10s)
- [ ] `SCAN entity:live:*` + `HGETALL` each key — read `last_seen_ms`
- [ ] If `now() - last_seen_ms > SIGNAL_LOSS_THRESHOLD_MS`:
  - `GET alert-state:{entity_id}` — if key absent: emit alert, set key
  - If key present: skip — already active
- [ ] Query `position_history` for last known position before publishing
- [ ] Publish: `{ alert_id, alert_type: SIGNAL_LOSS, entity_id, last_known_position, duration_ms }`
- [ ] `alert_id`: `{entity_id}:SIGNAL_LOSS:{dark_since_ms}`
- [ ] `SET alert-state:{entity_id} {dark_since_ms}` (no TTL)

**Route Deviation (US-04)**
- [ ] On each cycle, for each live entity:
  - Compare current position (from Redis) against `route_baseline` for the current time bucket
  - `INCR deviation-counter:{entity_id}` if outside threshold; `DEL` when back within range
  - Emit alert only after `DEVIATION_SUSTAINED_PINGS` consecutive out-of-baseline pings
- [ ] Publish: `{ alert_id, alert_type: ROUTE_DEVIATION, entity_id, current_position, baseline_position, deviation_metres }`
- [ ] `alert_id`: `{entity_id}:ROUTE_DEVIATION:{window_start_ms}`
- [ ] `Dockerfile` + added to `docker-compose.yml`

### API — Alert Consumer + REST

- [ ] Consumer group: `api`; consume `alerts` topic
- [ ] `INSERT INTO alerts ... ON CONFLICT (alert_id) DO NOTHING` — idempotent
- [ ] After insert: push alert to scoped WebSocket connections
  - Scope filter: `scope.geo_region` bounds + `scope.entity_types` + `scope.alert_types`
- [ ] `GET /alerts` — list alerts with filters: `status`, `entity_id`, `alert_type`; paginated; sorted by `detected_at DESC`
- [ ] `GET /alerts/:alert_id` — single alert detail including full `payload` JSONB

### Dashboard — Alert Panel

- [ ] Alert panel component alongside the map
- [ ] Alerts pushed over WebSocket appear in real time — no page refresh
- [ ] Each row: entity ID, alert type, time detected, status (NEW)
- [ ] Entity marker on map flagged visually when it has an active alert

## Done When

- Only one evaluator instance becomes leader; a second instance takes over when the leader is stopped
- Signal loss alert appears on the `alerts` Kafka topic after an entity goes silent beyond threshold
- No duplicate alert emitted while the entity stays dark
- Route deviation alert emitted after sustained deviation; single transient ping does not trigger it
- Alert appears in the dashboard alert panel in real time without a page refresh
- Alert panel only shows alerts matching the operator's workspace scope
