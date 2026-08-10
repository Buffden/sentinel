# Phase 01 — Infra + Schema

## Goal

All backing services start with a single command and all schemas are initialised automatically. Nothing works without this phase — it is the foundation everything else builds on.

## Dependencies

None.

## Tasks

### Docker Compose

- [ ] `docker-compose.yml` at repo root with:
  - **Redpanda** — single broker, Kafka-compatible
  - **Redpanda Console** — UI for topic inspection
  - **TimescaleDB** — PostgreSQL + TimescaleDB extension
  - **Neo4j** — community edition
  - **Redis** — single instance
- [ ] Named volumes for each stateful service — data survives container restarts
- [ ] Health checks so dependent services wait for readiness
- [ ] `.env.example` with all required environment variables and placeholder values

### Kafka Topics

- [ ] Create all topics on Redpanda before any service starts (init container or startup script):
  - `adsb.raw`, `ais.raw` — raw ingestion
  - `adsb.dlq`, `ais.dlq` — dead letter queues
  - `position.normalized` — normalised position stream
  - `deviation.candidates` — per-entity out-of-baseline status events (Deviation Detector → Alert Evaluator)
  - `proximity.candidates` — unscheduled proximity pairs (Correlation Worker → Alert Evaluator)
  - `alerts` — alert events
- [ ] Set `retention.ms` on all topics — consumers must be able to replay missed events after a crash (US-08, US-10); align with 30-day TimescaleDB retention window
- [ ] Set short retention (1h) on `deviation.candidates` and `proximity.candidates` — stale derived signals have no replay value; underlying facts remain durable in TimescaleDB and Neo4j (see ADR-014)

### TimescaleDB Schema

Init script mounted at `/docker-entrypoint-initdb.d/` — applied automatically on first start.

- [ ] `position_history` hypertable
  - Columns: `entity_id`, `entity_type`, `timestamp_ms`, `time_bucket`, `geo_cell`, `lat`, `lon`, `altitude`, `source`
  - Partitioned by `timestamp_ms`, daily chunks
  - Unique constraint on `(entity_id, timestamp_ms)` — idempotency key
  - Index on `(entity_id, time_bucket)` — single-entity timeline queries
  - Index on `(geo_cell, time_bucket)` — regional time-window queries
  - Retention policy: drop chunks older than 30 days
- [ ] `route_baseline` continuous aggregate over `position_history`
  - Columns: `entity_id`, `time_bucket`, `avg_lat`, `avg_lon`, `stddev_metres`, `sample_count`
  - 1-hour buckets; automated background refresh; 30-day look-back
- [ ] `alerts` table
  - Columns: `alert_id` (PK), `entity_id`, `entity_type`, `alert_type`, `priority`, `status`, `payload` (JSONB), `detected_at`, `updated_at`, `acknowledged_at`, `acknowledged_by`, `resolved_at`, `resolved_by`
  - Index on `(entity_id, detected_at DESC)` — investigation panel
  - Index on `(status, detected_at DESC)` — operator alert feed
  - Index on `(alert_type, detected_at DESC)` — alert type filter
- [ ] `users` table
  - Columns: `user_id` (PK UUID), `google_sub` (unique), `email`, `last_login_at`, `created_at`
- [ ] `user_workspaces` table
  - Columns: `user_id` (PK, FK → users), `scope` (JSONB), `updated_at`
  - `scope` holds: `geo_region` bounds, `entity_types` array, `alert_types` array

### Neo4j Schema

- [ ] Uniqueness constraint on `Entity` nodes: `entity_id`
- [ ] Index on `PROXIMITY_EVENT` edges: `idempotency_key`
- [ ] Index on `PROXIMITY_EVENT` edges: `timestamp_ms`

## Done When

- `docker compose up -d` starts all services without errors
- All 8 Kafka topics exist in Redpanda Console (`adsb.raw`, `ais.raw`, `adsb.dlq`, `ais.dlq`, `position.normalized`, `deviation.candidates`, `proximity.candidates`, `alerts`)
- TimescaleDB accepts a `psql` connection; all tables exist with correct columns
- `route_baseline` continuous aggregate is registered
- Neo4j uniqueness constraint on `Entity.entity_id` is in place
- Redis responds to `PING`
- `docker compose down -v` cleanly tears everything down
