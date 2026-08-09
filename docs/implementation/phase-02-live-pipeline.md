# Phase 02 — Live Pipeline: Moving Dots on a Map

## Goal

The full data pipeline working end-to-end — a real aircraft position flows from OpenSky through Kafka, gets normalised, lands in Redis and TimescaleDB, and causes a marker to move on a live map in the browser. No auth yet. This is proof of life for the entire system.

## Dependencies

- Phase 01 (infra + schema)

## Tasks

### Shared Package (`packages/shared/`)

- [ ] Shared TypeScript types (normalised position schema, alert payload types, Kafka message envelopes)
- [ ] Shared Kafka client config (broker URL, SSL, auth) — imported by ingestion poller, position consumer, correlation worker, alert evaluator, and API
- [ ] AIS ingestion (AISHub) is out of scope for v1 — the pipeline is designed to support it but only ADS-B (OpenSky) is wired up

### Ingestion Poller (`services/ingestion-poller/`)

- [ ] Node.js + TypeScript service
- [ ] Poll OpenSky Network REST API (`/api/states/all`) on a fixed interval (`POLL_INTERVAL_MS`)
- [ ] Publish each state vector as a message to `adsb.raw`
  - Key: `icao24` (entity_id for aircraft)
  - Value: raw JSON as received — no parsing
- [ ] Respect OpenSky rate limits; support optional `OPENSKY_USERNAME` / `OPENSKY_PASSWORD` for higher limits
- [ ] Graceful shutdown — flush in-flight messages before exit
- [ ] `Dockerfile` + added to `docker-compose.yml`

### Position Consumer (`services/position-consumer/`)

- [ ] Node.js + TypeScript service, consumer group: `position-consumer`
- [ ] Consume `adsb.raw`; parse each state vector into normalised schema:
  - `entity_id`, `entity_type`, `timestamp_ms`, `lat`, `lon`, `altitude`, `source`, `time_bucket`, `geo_cell` (H3 resolution 5)
- [ ] Malformed events → `adsb.dlq` with rejection reason — never dropped, never crash the consumer
- [ ] `INSERT INTO position_history ... ON CONFLICT DO NOTHING` (idempotency key: `{entity_id}:{timestamp_ms}`)
- [ ] `HSET entity:live:{entity_id} last_seen_ms {ms} lat {lat} lon {lon}` + TTL = `SIGNAL_LOSS_THRESHOLD_MS`
- [ ] On entity resume after signal loss: `DEL alert-state:{entity_id}`
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
