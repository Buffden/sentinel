# Phase 05 — Correlation Worker

## Goal

A Node.js service that reads the live position stream, writes proximity evidence into Neo4j, and publishes unscheduled proximity pairs to `proximity.candidates`. This phase can run in parallel with Phase 04 — it has no dependency on the alert evaluator. Phase 06 consumes `proximity.candidates` from Kafka (not by polling Neo4j); the Neo4j graph remains the durable store queried by the API investigation panel and for composite alert context.

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
    - If none: `MERGE` a `PROXIMITY_EVENT` edge with `idempotency_key`, `timestamp_ms`, `lat`, `lon`, `distance_metres`; then publish a `proximity.candidates` event to Kafka (see ADR-014)
    - If `KNOWN_ASSOCIATE` exists: write Neo4j edge only — do not publish to `proximity.candidates`
  - `MERGE Entity` nodes if they don't exist yet
- [ ] Does not write to TimescaleDB or Redis
- [ ] `Dockerfile` + added to `docker-compose.yml`

## Done When

- Service consumes `position.normalized` without errors
- `PROXIMITY_EVENT` edge appears in Neo4j when two unrelated entities come within threshold
- Running the same event twice does not create a duplicate edge (MERGE confirmed)
- No `PROXIMITY_EVENT` written when a `KNOWN_ASSOCIATE` edge exists
- `proximity.candidates` Kafka event published for each unscheduled pair; no event published for known associates
- Neo4j browser shows the entity graph growing as entities are seen
