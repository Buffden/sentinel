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
  - Columns: `entity_id`, `entity_type`, `observed_at` (TIMESTAMPTZ — hypertable partition column), `timestamp_ms` (BIGINT — source metadata + idempotency key), `geo_cell`, `lat`, `lon`, `altitude`, `source`
  - Partitioned by `observed_at`, daily chunks — Sentinel chooses TIMESTAMPTZ for its time semantics and deterministic derivation (`observed_at = to_timestamp(timestamp_ms / 1000.0)` computed at ingest)
  - Unique constraint on `(entity_id, observed_at)` — idempotency constraint (ADR-007); TimescaleDB requires the partition column (`observed_at`) in any unique constraint on a hypertable; `observed_at` is deterministically derived from `timestamp_ms` so the constraint is equivalent in practice
  - Index on `(entity_id, observed_at DESC)` — single-entity timeline queries
  - Index on `(geo_cell, observed_at DESC)` — regional time-window queries; `geo_cell` is an index column, not a partition dimension (see ADR-006)
  - Retention policy: drop chunks older than 30 days
- [ ] `route_references` table (route deviation uses deterministic reference routes, not a continuous aggregate — see ADR-015)
  - Columns: `route_id` (PK), `entity_id`, `route_name`, `corridor_threshold_metres`, `source`, `created_at`
  - Index on `entity_id`
- [ ] `route_reference_points` table
  - Columns: `route_id` (FK → route_references), `sequence_no`, `lat`, `lon`
  - Primary key on `(route_id, sequence_no)`
  - Both tables seeded from synthetic generator route definition at startup
- [ ] `alerts` table
  - Columns: `alert_id` (PK), `entity_id`, `counterparty_entity_id` (nullable — second participant for UNSCHEDULED_PROXIMITY and COMPOSITE), `entity_type`, `alert_type`, `priority`, `status` (NEW | ACKNOWLEDGED | RESOLVED | SUPERSEDED), `superseded_by` (nullable FK → alerts.alert_id), `payload` (JSONB), `detected_at`, `updated_at`, `acknowledged_at`, `acknowledged_by`, `resolved_at`, `resolved_by`
  - Full schema in DATA_MODEL.md
  - Index on `(entity_id, detected_at DESC)` — find alerts by primary entity
  - Index on `(counterparty_entity_id, detected_at DESC)` — find alerts by counterparty (proximity/composite)
  - Index on `(status, detected_at DESC)` — operator alert feed
  - Index on `(alert_type, detected_at DESC)` — alert type filter
- [ ] `users` table
  - Columns: `user_id` (PK UUID), `google_sub` (unique), `email`, `last_login_at`, `created_at`
- [ ] `user_workspaces` table
  - Columns: `user_id` (PK, FK → users), `scope` (JSONB), `updated_at`
  - `scope` holds: `geo_region` bounds, `entity_types` array, `alert_types` array

### Neo4j Schema

- [ ] Uniqueness constraint on `Entity` nodes: `entity_id`
- [ ] Index on `PROXIMITY_EVENT` edges: `idempotency_key` — supports MERGE deduplication and lookup by episode
- [ ] Index on `PROXIMITY_EVENT` edges: `episode_start_ms` — supports investigation time-range queries

## Done When

- `docker compose up -d` starts all services without errors
- All 8 Kafka topics exist in Redpanda Console (`adsb.raw`, `ais.raw`, `adsb.dlq`, `ais.dlq`, `position.normalized`, `deviation.candidates`, `proximity.candidates`, `alerts`)
- TimescaleDB accepts a `psql` connection; all tables exist with correct columns (`position_history`, `route_references`, `route_reference_points`, `alerts`, `users`, `user_workspaces`)
- No `route_baseline` continuous aggregate — route deviation uses `route_reference_points`
- Neo4j uniqueness constraint on `Entity.entity_id` is in place
- Redis responds to `PING`
- `docker compose down -v` cleanly tears everything down
