# Phase 06 — Proximity + Composite Alerts

## Goal

The Alert Evaluator gains two stream-sourced rules: unscheduled proximity (US-05) and composite alerts (US-06). Proximity inputs arrive from the Correlation Worker via `proximity.candidates` (one event per episode). The composite rule uses an **alert supersession model**: SIGNAL_LOSS is emitted immediately; if proximity arrives within the correlation window referencing the same entity, the SIGNAL_LOSS is superseded by a COMPOSITE alert at that point.

## Dependencies

- Phase 04 (alert evaluator running with leader election and `alert-state` hashes in Redis)
- Phase 05 (correlation worker publishing `PROXIMITY_EVENT` edges to Neo4j and `proximity.candidates` events to Kafka — one per episode)

## Tasks

### Unscheduled Proximity (US-05)

- [ ] Consume `proximity.candidates` (consumer group: `alert-evaluator`)
- [ ] On each event, check both `entity_a_id` and `entity_b_id`:
  - For each entity: check `HGETALL alert-state:{entity_id}` (entity is currently dark) OR `HGETALL recent-loss:{entity_id}` (entity was dark, has since resumed)
  - If a matching signal loss is found AND `episode_start_ms` falls within `COMPOSITE_CORRELATION_WINDOW_MS` of `dark_since_ms`:
    - Emit `COMPOSITE` alert (ELEVATED); include `supersedes_alert_ids: [signal_loss_alert_id]`
    - `alert_id`: `{entity_id}:COMPOSITE:{dark_since_ms}`; do NOT emit UNSCHEDULED_PROXIMITY
  - If no signal loss correlation found: emit `UNSCHEDULED_PROXIMITY` alert
- [ ] UNSCHEDULED_PROXIMITY: `{ alert_id, alert_type: UNSCHEDULED_PROXIMITY, pair_key, entity_a_id, entity_b_id, lat, lon, distance_at_detection, episode_start_ms }`
- [ ] `alert_id` for UNSCHEDULED_PROXIMITY: `{pair_key}:UNSCHEDULED_PROXIMITY:{episode_start_ms}`

### Composite Alert (US-06) — Supersession Model

SIGNAL_LOSS is never held back. Composite correlation happens when proximity arrives and finds a matching signal loss episode.

**Position Consumer (Phase 04 extension):**
- [ ] When entity resumes broadcasting and `alert-state:{entity_id}` exists:
  - `HSET recent-loss:{entity_id} dark_since_ms {dark_since_ms} resumed_at_ms {now} signal_loss_alert_id {alert_id}` + `EXPIRE COMPOSITE_CORRELATION_WINDOW_MS / 1000`
  - `DEL alert-state:{entity_id}`

**Alert Evaluator (composite detection):**
- [ ] COMPOSITE alert: query Neo4j for relationship context (targeted lookup on entity pair)
- [ ] Publish: `{ alert_id, alert_type: COMPOSITE, priority: ELEVATED, entity_id, signal_loss: { dark_since_ms, last_lat, last_lon }, proximity: { pair_key, entity_b_id, lat, lon, distance_at_detection }, correlation_window_ms, supersedes_alert_ids }`

**API (atomic supersession):**
- [ ] On COMPOSITE alert from Kafka:
  - DB transaction: `INSERT INTO alerts (composite)` + `UPDATE alerts SET status = 'SUPERSEDED', superseded_by = {composite_alert_id} WHERE alert_id IN (supersedes_alert_ids)`
  - Commit; then publish TWO messages to `alert-events`: `{ type: ALERT_CREATED, payload: composite }` and `{ type: ALERT_SUPERSEDED, payload: superseded_signal_loss_alert }`
- [ ] Dashboard shows COMPOSITE as the active incident; superseded SIGNAL_LOSS appears in the evidence/history view linked from the composite

## Done When

- `UNSCHEDULED_PROXIMITY` alert emitted when proximity arrives with no signal loss correlation
- No `proximity.candidates` event emitted for `KNOWN_ASSOCIATE` pairs (filtered upstream)
- When proximity arrives for an active dark entity: COMPOSITE emitted and SIGNAL_LOSS marked SUPERSEDED atomically; dashboard shows COMPOSITE as the active incident
- When proximity arrives for an entity that was dark but has since resumed (within `COMPOSITE_CORRELATION_WINDOW_MS`): same composite + supersession behaviour via `recent-loss`
- When only one condition is met: individual alert emitted, no composite
