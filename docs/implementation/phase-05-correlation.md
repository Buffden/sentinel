# Phase 05 — Correlation Worker

## Goal

A Node.js service that reads the live position stream, writes proximity evidence into Neo4j, and publishes unscheduled proximity pairs to `proximity.candidates`. This phase can run in parallel with Phase 04 — it has no dependency on the alert evaluator. Phase 06 consumes `proximity.candidates` from Kafka (not by polling Neo4j); the Neo4j graph remains the durable store queried by the API investigation panel and for composite alert context.

## Dependencies

- Phase 02 (position consumer publishing `position.normalized` and maintaining `entity:live:*` in Redis)

## Tasks

- [ ] Scaffold Node.js + TypeScript service under `services/correlation-worker/`
- [ ] Consumer group: `correlation-worker`; consume `position.normalized`
- [ ] On each event:
  - Extract `live_geo_cell` from the incoming event (computed at `LIVE_H3_RESOLUTION` by the Position Consumer; use this field — not `history_geo_cell` — for Redis sorted set lookups)
  - Compute k-ring radius from `PROXIMITY_THRESHOLD_METRES` and cell edge length at `LIVE_H3_RESOLUTION` — do not hardcode k-ring(1)
  - `ZRANGEBYSCORE geo-cell:{cell} {(now - LIVE_PROXIMITY_MAX_AGE_MS)} +inf` for each cell in the k-ring → union of fresh entity_ids
  - Remove the incoming entity_id from candidates
  - `HGETALL entity:live:{entity_id}` for each candidate; recheck `last_seen_ms` — skip if stale
  - Compute Haversine distances between the incoming entity and each fresh candidate
  - For each pair within `PROXIMITY_THRESHOLD_METRES`:
    - **Canonicalize pair:** `pair_key = min(entity_id, candidate_id) + ':' + max(entity_id, candidate_id)`
    - `HGETALL proximity-episode:{pair_key}` — if key exists (active episode):
      - `HSET proximity-episode:{pair_key} last_seen_ms {now}` + refresh TTL
      - Optionally update Neo4j edge `min_distance_metres` if this ping is closer
      - Skip: do NOT publish another `proximity.candidates` event
    - If no active episode:
      - Query Neo4j for a `KNOWN_ASSOCIATE` edge between the two entities
      - If none (unscheduled new episode):
        1. `episode_start_ms = now`
        2. **Write Neo4j first:** `MERGE PROXIMITY_EVENT` with `idempotency_key = {pair_key}:{episode_start_ms}`, `episode_start_ms`, `lat`, `lon`, `distance_at_detection`
        3. If Neo4j succeeds: `HSET proximity-episode:{pair_key} episode_start_ms {episode_start_ms} last_seen_ms {now} candidate_published 0` + `EXPIRE PROXIMITY_EPISODE_GAP_MS / 1000`
        4. Publish `proximity.candidates` to Kafka: `{ pair_key, entity_a_id: min, entity_b_id: max, episode_start_ms, lat, lon, distance_at_detection }`
        5. If Kafka publish succeeds: `HSET proximity-episode:{pair_key} candidate_published 1`
        6. If Kafka publish fails: leave `candidate_published=0`; on the next ping for this pair, re-attempt the publish before refreshing TTL
      - If `KNOWN_ASSOCIATE` exists: write Neo4j `PROXIMITY_EVENT` edge (same MERGE) + create/refresh `proximity-episode:{pair_key}` hash (TTL only — no `candidate_published`) — do not publish to `proximity.candidates`. The episode hash prevents a Neo4j MERGE on every ping within the encounter.
  - `MERGE Entity` nodes if they don't exist yet
- [ ] Writes to Redis: `proximity-episode:{pair_key}` hash only; does not write to TimescaleDB
- [ ] `Dockerfile` + added to `docker-compose.yml`

## Done When

- Service consumes `position.normalized` without errors
- `PROXIMITY_EVENT` edge appears in Neo4j for both unscheduled pairs AND known associates (all close pairs write the edge; the graph is the durable proximity evidence store)
- Running the same event twice does not create a duplicate Neo4j edge (MERGE on `idempotency_key` confirmed)
- `proximity.candidates` Kafka event published once per episode for unscheduled pairs only; no event published for known associates
- `proximity-episode:{pair_key}` hash exists while entities are within threshold for ALL pairs (unscheduled and known associates); `candidate_published` field set to `1` after successful Kafka publish for unscheduled pairs; TTL refreshes on each ping; new episode starts after TTL expires
- If the service crashes after Neo4j write but before Kafka publish (`candidate_published=0`): next ping triggers retry publish
- Neo4j browser shows the entity graph growing as entities are seen
