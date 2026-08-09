# Phase 05 — Correlation Worker

## Goal

A Node.js service that reads the live position stream and writes proximity evidence into Neo4j. This phase can run in parallel with Phase 04 — it has no dependency on the alert evaluator. The graph it builds is what Phase 06 queries to detect unscheduled proximity and composite anomalies.

## Dependencies

- Phase 02 (position consumer publishing `position.normalized` and maintaining `entity:live:*` in Redis)

## Tasks

- [ ] Scaffold Node.js + TypeScript service under `services/correlation-worker/`
- [ ] Consumer group: `correlation-worker`; consume `position.normalized`
- [ ] On each event:
  - `HGETALL entity:live:*` — fetch all current positions from Redis
  - Compute pairwise Haversine distances between incoming entity and all others
  - For each pair within `PROXIMITY_THRESHOLD_METRES`:
    - Query Neo4j for a `KNOWN_ASSOCIATE` edge between the two entities
    - If none: `MERGE` a `PROXIMITY_EVENT` edge with `idempotency_key`, `timestamp_ms`, `lat`, `lon`, `distance_metres`
    - If `KNOWN_ASSOCIATE` exists: skip — routine proximity, not anomalous
  - `MERGE Entity` nodes if they don't exist yet
- [ ] Does not write to TimescaleDB, Redis, or Kafka
- [ ] `Dockerfile` + added to `docker-compose.yml`

## Done When

- Service consumes `position.normalized` without errors
- `PROXIMITY_EVENT` edge appears in Neo4j when two unrelated entities come within threshold
- Running the same event twice does not create a duplicate edge (MERGE confirmed)
- No `PROXIMITY_EVENT` written when a `KNOWN_ASSOCIATE` edge exists
- Neo4j browser shows the entity graph growing as entities are seen
