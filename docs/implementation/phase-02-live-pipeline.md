# Phase 02 — Live Pipeline: Moving Dots on a Map

## Goal

The full data pipeline working end-to-end — a real aircraft position flows from OpenSky through Kafka, gets normalised, lands in Redis and TimescaleDB, and causes a marker to move on a live map in the browser. No auth yet. This is proof of life for the entire system.

## Dependencies

- Phase 01 (infra + schema)

## Tasks

### Shared Package (`packages/shared/`)

- [ ] Shared TypeScript types (normalised position schema, alert payload types, Kafka message envelopes)
- [ ] Shared Kafka client config (broker URL, SSL, auth) — imported by ingestion poller, position consumer, correlation worker, deviation detector, alert evaluator, and API
- [ ] AIS ingestion (AISHub) is out of scope for v1 — the pipeline is designed to support it but only ADS-B (OpenSky) is wired up

### Ingestion Poller (`services/ingestion-poller/`)

- [ ] Node.js + TypeScript service
- [ ] Poll OpenSky Network REST API (`/api/states/all`) on a fixed interval (`POLL_INTERVAL_MS`)
- [ ] Publish each state vector as a message to `adsb.raw`
  - Key: `icao24` (entity_id for aircraft)
  - Value: raw JSON as received — no parsing
- [ ] Authenticate via OAuth2 client credentials (`OPENSKY_CLIENT_ID` / `OPENSKY_CLIENT_SECRET`); cache bearer token and refresh before expiry
- [ ] Use bounding-box endpoint (`/states/all?lamin=...`) configured via `OPENSKY_BBOX` — cheaper than global `/states/all`
- [ ] Respect OpenSky rate limits; document credit cost per request
- [ ] Graceful shutdown — flush in-flight messages before exit
- [ ] `Dockerfile` + added to `docker-compose.yml`

### Position Consumer (`services/position-consumer/`)

- [ ] Node.js + TypeScript service, consumer group: `position-consumer`
- [ ] Consume `adsb.raw`; parse each state vector into normalised schema:
  - `entity_id`, `entity_type`, `timestamp_ms`, `lat`, `lon`, `altitude`, `source`
  - Compute `observed_at = to_timestamp(timestamp_ms / 1000.0)` at ingest for the TimescaleDB write
  - Compute `history_geo_cell` (H3 at `HISTORY_H3_RESOLUTION`; default 5) for the `geo_cell` column in TimescaleDB
  - Compute `live_geo_cell` (H3 at `LIVE_H3_RESOLUTION`) for the Redis `geo-cell:*` sorted set key; both fields included in the `position.normalized` event (see ADR-006)
- [ ] Events with null or missing `time_position` (or `latitude`/`longitude`) → skip with metric increment (`states_without_position_total`), never DLQ — null position is a valid OpenSky source state (transponder not yet reporting position), not a parse failure; continue to next state vector
- [ ] Events where `time_position` is more than `STALE_POSITION_THRESHOLD_MS` old → skip with metric (`states_stale_position_total`); these represent outdated tracks that would mislead staleness detection — do not DLQ (they are parseable; log at debug level)
- [ ] Use `time_position` as `timestamp_ms` — not `time` (server collection time); `time_position` is when the transponder last reported position
- [ ] Truly malformed events (unparseable JSON, missing required non-position fields) → `adsb.dlq` with rejection reason — never dropped, never crash the consumer
- [ ] `INSERT INTO position_history (entity_id, entity_type, observed_at, timestamp_ms, geo_cell, lat, lon, altitude, source) VALUES (...) ON CONFLICT (entity_id, observed_at) DO NOTHING` (idempotency constraint: `(entity_id, observed_at)`; `geo_cell` column receives `history_geo_cell`)
- [ ] **Timestamp guard:** before writing, `HGET entity:live:{entity_id} last_seen_ms`; only proceed if `incoming.timestamp_ms >= stored value` (prevents replay or out-of-order delivery from regressing Redis state)
- [ ] `HSET entity:live:{entity_id} last_seen_ms {ms} lat {lat} lon {lon} live_geo_cell {live_geo_cell} entity_type {entity_type}` + TTL = 24h (stores `live_geo_cell` so the Correlation Worker can use the correct resolution for geo-cell lookups)
- [ ] Update geo-cell spatial index using `live_geo_cell`: `ZREM geo-cell:{previous_live_geo_cell} {entity_id}` + `ZADD geo-cell:{new_live_geo_cell} {last_seen_ms} {entity_id}` (previous cell retrieved from prior hash value via HGET before overwriting)
- [ ] On entity resume after signal loss (first write after `alert-state` exists): write `recent-loss:{entity_id}` hash (`dark_since_ms`, `resumed_at_ms = event.timestamp_ms`, `signal_loss_alert_id`) with TTL = `COMPOSITE_CORRELATION_WINDOW_MS`; then `DEL alert-state:{entity_id}` — use source event time for `resumed_at_ms` so composite correlation windows are consistent regardless of processing latency
- [ ] `PUBLISH position-updates {normalised_event_json}` after every write
- [ ] `PRODUCE position.normalized` — consumed by Correlation Worker later
- [ ] `Dockerfile` + added to `docker-compose.yml`

### API — Core (`services/api/`)

- [ ] Node.js + Express + TypeScript service
- [ ] No auth in this phase — all endpoints open (auth added in Phase 03)
- [ ] `GET /entities` — returns all `entity:live:*` hashes from Redis for initial map load
- [ ] Subscribe to Redis `position-updates` on startup
- [ ] WebSocket endpoint: on connect, fan out every `position-updates` event to all connected clients (no scope filtering yet)
- [ ] `Dockerfile` + added to `docker-compose.yml`

### Dashboard — Live Map (`services/dashboard/`)

- [ ] Angular + TypeScript app
- [ ] Leaflet map as the root view — no login screen yet
- [ ] On load: call `GET /entities` and render each entity as a marker
- [ ] Open WebSocket; on each position event update the corresponding marker
- [ ] Differentiate aircraft vs vessel by icon or marker colour
- [ ] Client-side entity staleness timer: track `last_seen_ms` per entity locally; when `now() - last_seen_ms > SIGNAL_LOSS_THRESHOLD_MS`, fade and remove the marker — no server push needed, the API never sends a "remove entity" message (US-01)
- [ ] Reconnect on WebSocket disconnect
- [ ] `Dockerfile` (nginx) + added to `docker-compose.yml`

## Done When

- `docker compose up -d` brings the full stack up
- Redpanda Console shows messages flowing on `adsb.raw` and `position.normalized`
- `HGETALL entity:live:{icao24}` in Redis returns a current position
- `SELECT COUNT(*) FROM position_history` grows over time
- Opening the dashboard in a browser shows entity markers on the map
- Markers move without a page refresh as new positions arrive
- A malformed test message on `adsb.raw` appears on `adsb.dlq`
