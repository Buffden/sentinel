# Phase 08 — Entity Investigation

## Goal

Operators can click any entity or alert and open an evidence panel that assembles a full picture from all three stores in parallel: current live state from Redis, alert record and position history from TimescaleDB, and relationship context from Neo4j. This is the most architecturally interesting read path in the system — it demonstrates why three different stores exist.

## Dependencies

- Phase 06 (proximity + composite alerts — Neo4j graph populated, composite alert_id in use)
- Phase 07 (alert lifecycle — resolved alerts queryable as history)

## Tasks

### API — Investigation Endpoints

- [ ] `GET /entities/:entity_id` — fetch current live state from `entity:live:{entity_id}` Redis hash
- [ ] `GET /entities/:entity_id/history` — fetch position track from `position_history` (TimescaleDB), bounded by time range query params
- [ ] `GET /entities/:entity_id/alerts` — fetch alert history for entity from `alerts` table, sorted by `detected_at DESC`
- [ ] `GET /alerts/:alert_id/evidence` — assemble evidence panel in parallel:
  - Redis: `HGETALL entity:live:{entity_id}` — is the entity still in the anomalous state right now?
  - TimescaleDB: `SELECT * FROM alerts WHERE alert_id = $1` — full alert record and payload
  - Neo4j: known associates + recent proximity events within the alert window
  - Return all three as a single response: `{ current_state, alert_detail, known_associates, proximity_events }`
- [ ] All three fetches run in parallel — do not chain them sequentially

### Dashboard — Evidence Panel

- [ ] Clicking an entity marker or alert row opens an investigation sidebar
- [ ] Evidence panel shows:
  - Current live state (is entity still dark / still deviating?)
  - Alert detail: type, detected time, payload-specific fields (duration, deviation metres, proximity distance)
  - Position history track rendered on the map for the alert window
  - Known associates list from Neo4j
  - Recent proximity events from Neo4j, flagged as known or unrelated
- [ ] Panel works for all alert types (SIGNAL_LOSS, ROUTE_DEVIATION, UNSCHEDULED_PROXIMITY, COMPOSITE)

## Done When

- `GET /alerts/:alert_id/evidence` returns data from all three stores in a single response
- Redis, TimescaleDB, and Neo4j fetches execute in parallel (confirm with request timing)
- Evidence panel opens in the dashboard when an alert is clicked
- For a COMPOSITE alert: panel shows signal loss duration, proximity entity flagged as unrelated, and last known position on the map
- For an entity that has resumed broadcasting: Redis field shows current position (not nil)
