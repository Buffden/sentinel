# Phase 04 — Alert Pipeline: Signal Loss + Route Deviation

## Goal

The first two anomaly rules are live and visible. An operator on the dashboard sees a real-time alert panel populated by alerts flowing through Kafka. This phase proves the entire alert path: detection → Kafka → API → WebSocket → dashboard.

## Dependencies

- Phase 03 (auth + scoped WebSocket in place)

## Tasks

### Alert Evaluator (`services/alert-evaluator/`)

**Leader Election (ADR-005)**
- [ ] Node.js + TypeScript service
- [ ] On startup: attempt `SET alert-evaluator:leader {instance_id} NX PX {LEADER_TTL_MS}`
  - If acquired: create Kafka consumer, subscribe to `deviation.candidates` + `proximity.candidates`, start evaluation loop
  - If not acquired: do not create or join the Kafka consumer group — idle followers must not join or they trigger rebalances
- [ ] Leader renews lease on each heartbeat using Lua compare-and-expire: `if GET('alert-evaluator:leader') == instance_id then PEXPIRE('alert-evaluator:leader', LEADER_TTL_MS)` — `SET XX PX` is not safe for renewal; it would overwrite the holder value without checking ownership, allowing a slow old leader to steal a lease already acquired by a new leader
- [ ] On lease loss (renewal fails):
  1. Stop accepting new evaluation work
  2. Stop / pause Kafka consumption
  3. Wait for or cancel the current in-flight evaluation
  4. Close Kafka consumer and leave consumer group
  5. Return to follower poll loop
- [ ] On clean shutdown: compare-before-DEL (Lua script: `if GET == instance_id then DEL`) — prevents deleting a lease a new leader has already acquired
- [ ] Follower poll loop: attempt NX acquire every `LEADER_POLL_INTERVAL_MS`; on success, create consumer and start

**Signal Loss (US-03)**
- [ ] Scheduled scan every `SIGNAL_LOSS_SCAN_INTERVAL_MS` (10s)
- [ ] `SCAN entity:live:*` + `HGETALL` each key — read `last_seen_ms` (key TTL is 24h, guaranteed present)
- [ ] If `now() - last_seen_ms > SIGNAL_LOSS_THRESHOLD_MS`:
  - `HGET alert-state:{entity_id} dark_since_ms` — if key absent: emit alert, write hash
  - If key present: skip — already active (in-loop suppression)
- [ ] Query `position_history` for last known position before publishing
- [ ] Publish: `{ alert_id, alert_type: SIGNAL_LOSS, entity_id, last_known_position: { lat, lon }, dark_since_ms }`
- [ ] `alert_id`: `{entity_id}:SIGNAL_LOSS:{dark_since_ms}`
- [ ] `HSET alert-state:{entity_id} dark_since_ms {dark_since_ms} signal_loss_alert_id {alert_id} composite_issued 0` (no TTL)

**Route Deviation (US-04)**
- [ ] Consume `deviation.candidates` (consumer group: `alert-evaluator`)
- [ ] On `OUT_OF_RANGE` event:
  - Replay guard: `HGET deviation-state:{entity_id} last_processed_ms` — if `event.timestamp_ms <= stored`, skip
  - `HINCRBY deviation-state:{entity_id} count 1`
  - If count == 1: `HSET deviation-state:{entity_id} episode_start_ms {timestamp_ms} alert_emitted 0`
  - `HSET deviation-state:{entity_id} last_processed_ms {timestamp_ms}`
  - Apply safety TTL: `EXPIRE deviation-state:{entity_id} {DEVIATION_STATE_TTL_MS / 1000}` (default: 24h; decoupled from signal-loss timing — see ADR-014)
  - If count >= `DEVIATION_SUSTAINED_PINGS` AND `alert_emitted == 0`: emit alert; `HSET deviation-state:{entity_id} alert_emitted 1`
- [ ] On `IN_RANGE` event: `DEL deviation-state:{entity_id}` — reset episode; no alert
- [ ] Publish: `{ alert_id, alert_type: ROUTE_DEVIATION, entity_id, current_position, nearest_segment_index, deviation_metres, sustained_cycles }`
- [ ] `alert_id`: `{entity_id}:ROUTE_DEVIATION:{episode_start_ms}`
- [ ] `Dockerfile` + added to `docker-compose.yml`

### Deviation Detector (`services/deviation-detector/`)

- [ ] Scaffold Node.js + TypeScript service under `services/deviation-detector/`
- [ ] Consumer group: `deviation-detector`; consume `position.normalized`
- [ ] On each event:
  - Look up entity in `route_references` table — if no row: skip (real ADS-B/AIS with no assigned route)
  - Fetch `route_reference_points` for that `route_id`
  - Find the nearest route segment by computing minimum perpendicular distance from the entity's position to each segment (point[i] → point[i+1]); if the perpendicular foot falls outside the segment, use the distance to the nearer endpoint
  - If `deviation_metres > corridor_threshold_metres`: publish `OUT_OF_RANGE` to `deviation.candidates` with `nearest_segment_index` (index of segment start) and `deviation_metres`
  - Otherwise: publish `IN_RANGE` to `deviation.candidates` (every in-range ping — stateless; no `BACK_IN_RANGE`)
- [ ] Does not write to Redis, Neo4j, or the `alerts` topic
- [ ] `Dockerfile` + added to `docker-compose.yml`

### API — Alert Consumer + REST

- [ ] Consumer group: `api`; consume `alerts` topic
- [ ] On new SIGNAL_LOSS / ROUTE_DEVIATION / UNSCHEDULED_PROXIMITY alert: **commit ordering** — (1) `INSERT INTO alerts ... ON CONFLICT (alert_id) DO NOTHING`; (2) publish `{ type: ALERT_CREATED, payload: {...} }` to `alert-events` (always publish, even if ON CONFLICT no-op — ensures WebSocket delivery after replay); (3) commit Kafka offset. Committing before pub/sub loses the push on crash; dashboard deduplicates by `alert_id`.
- [ ] On COMPOSITE alert (from Kafka): in one DB transaction — INSERT COMPOSITE alert + UPDATE referenced SIGNAL_LOSS alert to `SUPERSEDED` (set `superseded_by = composite_alert_id`); publish `{ type: ALERT_CREATED, payload: composite }` AND `{ type: ALERT_SUPERSEDED, payload: superseded_alert }` to `alert-events`
- [ ] On startup: subscribe to Redis `alert-events` channel (alongside `position-updates`)
- [ ] On `alert-events` message: fan out to scope-matched WebSocket connections
  - Scope filter: use immutable detection-location in alert payload (not current Redis position); check `scope.geo_region` bounds + `scope.entity_types` + `scope.alert_types`
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
