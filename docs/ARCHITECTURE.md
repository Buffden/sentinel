# Architecture

This document defines the service boundaries, component contracts, data flow, and persistence ownership for Sentinel. It is the authoritative reference for which service reads from and writes to which store, which Kafka topics each service produces and consumes, and what each service is explicitly not allowed to do.

![Architecture Diagram](../diagrams/docs/architecture.svg)

---

## Services

### Ingestion Poller

**Runtime:** Node.js (ADR-013)
**Concern:** Fetch raw positional telemetry from external feeds and forward it to Kafka without any parsing or business logic.

| Direction | What |
| --- | --- |
| Reads from | OpenSky Network REST API (ADS-B), AISHub (AIS) |
| Publishes to | `adsb.raw`, `ais.raw` |
| Writes to stores | — |

**Contract:**
- Publishes raw event bytes as received. No parsing, no field extraction, no DLQ routing.
- Does not know about downstream consumers. The Kafka topic is the only coupling point.
- Polls on a fixed interval. Rate limit compliance is the poller's responsibility.

---

### Position Consumer

**Runtime:** Node.js
**Concern:** Normalise raw positional events, write to the persistence layer, and broadcast to the API layer via Redis pub/sub.

| Direction | What |
| --- | --- |
| Consumes | `adsb.raw`, `ais.raw` (consumer group: `position-consumer`) |
| Publishes to | `position.normalized`, `adsb.dlq`, `ais.dlq` |
| Writes to TimescaleDB | `position_history` (INSERT ON CONFLICT DO NOTHING) |
| Writes to Redis | `entity:live:{entity_id}` hash (HSET with `last_seen_ms`, lat, lon, `live_geo_cell`, entity_type; TTL = 24h safety-net; **timestamp guard**: only write if `incoming.timestamp_ms >= stored.last_seen_ms` to prevent replay regressions) |
| Writes to Redis | `geo-cell:{h3_cell_id}` sorted set — ZREM old cell, ZADD new cell with score=`last_seen_ms`; enables O(cell-density) proximity scoping without stale-member accumulation |
| Publishes to Redis | `position-updates` pub/sub channel (every normalised event) |
| Writes to Redis | `recent-loss:{entity_id}` hash (TTL = COMPOSITE_CORRELATION_WINDOW_MS) — written when entity resumes after a signal loss, before deleting `alert-state`; contains `dark_since_ms`, `resumed_at_ms`, `signal_loss_alert_id` |
| Deletes from Redis | `alert-state:{entity_id}` — after writing `recent-loss`; the correlation window survives the entity coming back online |

**Contract:**
- Every write uses the idempotency key `{entity_id}:{timestamp_ms}`. Replay is safe by construction.
- Computes two H3 cell IDs per ping: `history_geo_cell` at `HISTORY_H3_RESOLUTION` (for the `geo_cell` column in `position_history`) and `live_geo_cell` at `LIVE_H3_RESOLUTION` (for the Redis `geo-cell:*` sorted set key). Both are included in the `position.normalized` event so downstream consumers use the correct cell for their access pattern.
- Malformed or unparseable events go to the appropriate DLQ (`adsb.dlq` or `ais.dlq`) — never dropped, never crash the consumer. Events with null or missing position data are skipped with a metric increment, not DLQ'd (null position is a valid source state, not a parse failure).
- `last_seen_ms` must be written on every Redis hash update. The alert evaluator depends on this field for signal loss detection.
- **Timestamp guard on Redis writes:** before writing, check the stored `last_seen_ms`. Only update if `incoming.timestamp_ms >= stored.last_seen_ms`. Prevents Kafka replay or out-of-order delivery from regressing Redis state to an older position and publishing stale positions over WebSocket. "Same write twice" is idempotent; "old write after new write" is not.
- **Replay mode:** when replaying from an earlier Kafka offset for historical rebuilds, suppress publication to `position-updates` and skip the Redis hash write. Replay uses a separate consumer group and writes only to TimescaleDB.
- Deletes `alert-state:{entity_id}` on first successful write for an entity that was previously dark. This is the only service that deletes this key.
- Does not evaluate anomaly rules. Does not write to Neo4j.

---

### Correlation Worker

**Runtime:** Node.js
**Concern:** Detect entity proximity from the normalised position stream, maintain the entity relationship graph in Neo4j, and publish unscheduled proximity candidates to Kafka.

| Direction | What |
| --- | --- |
| Consumes | `position.normalized` (consumer group: `correlation-worker`) |
| Reads from Redis | `geo-cell:{h3_cell_id}` sorted sets — `ZRANGEBYSCORE` for fresh members only (score >= now - SIGNAL_LOSS_THRESHOLD_MS) in same-cell + k-ring(1) cells |
| Reads from Redis | `entity:live:{entity_id}` hashes — positions of the candidate entities identified via geo-cell lookup; rechecks `last_seen_ms` after fetch and skips stale candidates |
| Reads from Redis | `proximity-episode:{pair_key}` — episode check before publishing to `proximity.candidates`; if episode active, update `last_seen_ms` + refresh TTL; no new publish |
| Writes to Redis | `proximity-episode:{pair_key}` — hash with `episode_start_ms`, `last_seen_ms`, `candidate_published`; TTL = PROXIMITY_EPISODE_GAP_MS; created on first detection; refreshed on subsequent pings within episode |
| Writes to Neo4j | `PROXIMITY_EVENT` edges via MERGE (idempotent) |
| Publishes to | `proximity.candidates` — unscheduled proximity pairs, pre-filtered to exclude known associates |

**Contract:**

- Does not write to TimescaleDB. Writes to Redis only for `proximity-episode:{pair_key}` (episode state hash).
- Proximity candidates are scoped using H3: `ZRANGEBYSCORE geo-cell:{cell}` for the incoming entity's cell + k-ring(1) neighbours, score-filtered to fresh members only. After fetching positions from `entity:live:*`, rechecks `last_seen_ms` and skips stale candidates. Comparison is O(cell density) rather than O(total entities).
- **Pair canonicalization:** all pair identifiers use `min(a,b):max(a,b)` ordering — `proximity-episode` key, Neo4j `idempotency_key`, and `proximity.candidates` `entity_a_id`/`entity_b_id` (a is always lexicographically smaller).
- **k-ring constraint:** The Correlation Worker computes the required k-ring radius from `PROXIMITY_THRESHOLD_METRES` and the cell edge length at `LIVE_H3_RESOLUTION`, rather than hardcoding k-ring(1). k-ring(1) at H3 resolution 5 only guarantees no missed pairs when `PROXIMITY_THRESHOLD_METRES < 9850m` — document and validate this in POC-03.
- **H3 resolutions:** `LIVE_H3_RESOLUTION` (used for Redis `geo-cell:*` sorted sets) and `HISTORY_H3_RESOLUTION` (used for TimescaleDB `geo_cell` column) are separate configuration values. The Correlation Worker uses `LIVE_H3_RESOLUTION` for geo-cell lookups. They may differ; validate the right values in POC-03.
- **Episode model:** on first detection of a proximity pair within threshold: (1) Neo4j MERGE edge (idempotency key: `{pair_key}:{episode_start_ms}`); (2) create `proximity-episode:{pair_key}` hash with `candidate_published=0`; (3) publish ONE `proximity.candidates` event to Kafka; (4) set `candidate_published=1`. On subsequent pings within the same episode: if `candidate_published=1`, update `last_seen_ms` + refresh TTL; do NOT publish another event. If `candidate_published=0` (crash recovery — Neo4j written but Kafka publish failed), retry the Kafka publish.
- On detecting a new unscheduled episode: **Neo4j MERGE first, then set `candidate_published=0`, then Kafka publish, then set `candidate_published=1`.** If Neo4j fails, do not create the episode hash or publish to Kafka. If Kafka publish fails: set `candidate_published=0`, retry on next ping. Accepted failure mode: alert may be delayed by one ping interval.
- On detecting a pair with a `KNOWN_ASSOCIATE` edge: write Neo4j edge only, do not create proximity-episode, do not publish to `proximity.candidates`.
- Edge writes use MERGE with an idempotency key to ensure replay does not create duplicate edges.
- Does not emit alerts. Does not evaluate composite anomaly rules.

---

### Deviation Detector

**Runtime:** Node.js
**Concern:** Compare each normalised position against the reference route and publish deviation status events to Kafka. Does not evaluate alert rules.

| Direction | What |
| --- | --- |
| Consumes | `position.normalized` (consumer group: `deviation-detector`) |
| Reads from TimescaleDB | `route_reference_points` (via `route_references` header) — reference waypoints for the incoming entity (synthetic entities only); see ADR-015 |
| Publishes to | `deviation.candidates` — `OUT_OF_RANGE` and `IN_RANGE` status events per entity, one per eligible ping |

**Contract:**

- **Stateless:** classifies every eligible ping independently — emits `OUT_OF_RANGE` or `IN_RANGE` on every `position.normalized` event for an entity that has an assigned route. Does not track prior state. Episode state lives entirely in the Alert Evaluator via `deviation-state:{entity_id}`.
- Finds the nearest route segment in `route_reference_points` for the entity, computes minimum perpendicular distance (point-to-segment). If distance > `corridor_threshold_metres` from `route_references`: `OUT_OF_RANGE`. Otherwise: `IN_RANGE`. Published event carries `nearest_segment_index` (index of segment start waypoint) and `deviation_metres`.
- **Synthetic entities only (v1):** if no row exists in `route_references` for the entity, skip — no event published. Real ADS-B/AIS entities are not covered by route deviation in v1 (ADR-015).
- Does not apply the `DEVIATION_SUSTAINED_PINGS` filter — that logic belongs to the Alert Evaluator.
- Does not write to Redis, Neo4j, or the `alerts` topic.

---

### Alert Evaluator

**Runtime:** Node.js
**Concern:** Evaluate anomaly rules against live state and the entity graph, and emit alerts exactly once.

| Direction | What |
| --- | --- |
| Consumes | `deviation.candidates` (consumer group: `alert-evaluator`) — `OUT_OF_RANGE` / `IN_RANGE` events from Deviation Detector |
| Consumes | `proximity.candidates` (consumer group: `alert-evaluator`) — new proximity episode starts from Correlation Worker |
| Reads from Redis | `entity:live:*` (scheduled scan — `last_seen_ms` for signal loss detection) |
| Reads from Redis | `alert-state:{entity_id}` hash, `recent-loss:{entity_id}` hash, `deviation-state:{entity_id}` hash, `alert-evaluator:leader` |
| Reads from TimescaleDB | `position_history` — last known position before signal loss, included in alert payload |
| Reads from Neo4j | `KNOWN_ASSOCIATE` and `PROXIMITY_EVENT` edges — composite alert context only (targeted lookup per entity pair, not a scan) |
| Publishes to | `alerts` |
| Writes to Redis | `alert-state:{entity_id}` hash on first signal loss emission: `{ dark_since_ms, signal_loss_alert_id }`; no TTL |
| Writes to Redis | `deviation-state:{entity_id}` hash — HINCRBY count on `OUT_OF_RANGE`; DEL on `IN_RANGE`; safety TTL = `DEVIATION_STATE_TTL_MS` (default: 24h; decoupled from signal-loss timing) |
| Holds Redis lease | `alert-evaluator:leader` (SET NX PX pattern; Lua compare-and-expire on heartbeat; compare-before-DEL on release) |

**Contract:**

- **Leader election and Kafka:** only the current lease holder creates and joins the `alert-evaluator` Kafka consumer group. Followers do not join — an unpolled member triggers group rebalances. On lease acquisition: create consumer, subscribe, start polling. On lease loss: (1) stop accepting new work; (2) stop/pause Kafka; (3) wait for or cancel in-flight evaluation; (4) close consumer; (5) return to follower poll loop. **Lease renewal** uses a Lua compare-and-expire script: `if GET(key) == instance_id then PEXPIRE(key, LEADER_TTL_MS)` — `SET XX PX` is not safe for renewal because it overwrites the value without checking the current holder; a slow old leader could accidentally extend a lease already held by a new leader. **Lease release** uses compare-before-DEL: `if GET(key) == instance_id then DEL(key)`.
- Signal loss detection is a scheduled Redis scan — the evaluator reads `last_seen_ms` from `entity:live:*` directly. This is intentional: signal loss is an absence of events and cannot be driven by a Kafka stream (ADR-014).
- **Route deviation:** inputs arrive via `deviation.candidates` with status `OUT_OF_RANGE` or `IN_RANGE`. Replay guard: ignore event if `timestamp_ms <= deviation-state:{entity_id}.last_processed_ms`. State in `deviation-state:{entity_id}` (hash: `count`, `episode_start_ms`, `last_processed_ms`, `alert_emitted`). On `OUT_OF_RANGE`: HINCRBY count; if count==1 set episode_start_ms; if count >= DEVIATION_SUSTAINED_PINGS and alert_emitted==0: emit ROUTE_DEVIATION, set alert_emitted=1; update last_processed_ms. On `IN_RANGE`: DEL.
- **Proximity (supersession model):** inputs arrive via `proximity.candidates` (one per new episode). SIGNAL_LOSS is already emitted immediately when detected — never held back. When proximity arrives, check `alert-state:{entity_id}` (entity still dark) OR `recent-loss:{entity_id}` (entity was dark, has since resumed) for both entities. If a matching signal loss is found within the correlation window: emit COMPOSITE with `supersedes_alert_ids`; if not: emit UNSCHEDULED_PROXIMITY.
- The API handles composite supersession atomically: INSERT COMPOSITE + UPDATE SIGNAL_LOSS to SUPERSEDED in one transaction; broadcast both via `alert-events` (ALERT_CREATED for composite, ALERT_SUPERSEDED for the old alert).
- Neo4j is queried only when assembling composite alert context — targeted lookup on a specific entity pair. Not a scan.
- Reads `position_history` only to fetch last known position for the signal loss alert payload. Does not read route reference tables.
- Does not delete `alert-state:{entity_id}`. That is the Position Consumer's responsibility.
- Every alert payload carries the immutable detection-location for scope filtering. The current Redis position is not queried for scope decisions (ADR-012).

---

### API

**Runtime:** Node.js / Express (ADR-008)
**Concern:** Serve the dashboard over REST and WebSocket, consuming alerts from Kafka, authenticating operators, and fanning live updates to scoped connections.

| Direction | What |
| --- | --- |
| Consumes | `alerts` (consumer group: `api`) |
| Reads from Redis | `entity:live:{entity_id}` hash (initial map load, investigation panel) |
| Subscribes to Redis | `position-updates` pub/sub channel (live position WebSocket fan-out) |
| Subscribes to Redis | `alert-events` pub/sub channel (alert WebSocket fan-out — all instances subscribe) |
| Publishes to Redis | `alert-events` — the instance that consumes an alert from Kafka publishes it here after writing to TimescaleDB, so all instances can push to their WebSocket connections |
| Reads from TimescaleDB | `position_history`, `alerts`, `user_workspaces`, `users` |
| Reads from Neo4j | `PROXIMITY_EVENT`, `KNOWN_ASSOCIATE` edges (investigation pivot) |
| Writes to TimescaleDB | `alerts` (INSERT ON CONFLICT DO NOTHING on Kafka consume), `users`, `user_workspaces` |
| Authenticates via | Google OAuth 2.0 ID token verification (ADR-011) |

**Contract:**

- Sole consumer of the `alerts` Kafka topic. **Kafka commit ordering:** (1) write to TimescaleDB (`INSERT ... ON CONFLICT DO NOTHING`); (2) publish to `alert-events` Redis pub/sub (always — even if the DB insert was a no-op due to ON CONFLICT, the pub/sub ensures WebSocket delivery after replay); (3) commit the Kafka offset. Committing before publishing means a crash between commit and publish loses the WebSocket push with no recovery. The dashboard deduplicates by `alert_id` so a replayed pub/sub event is harmless.
- **Composite supersession:** when a COMPOSITE alert is consumed from Kafka, the API performs a single atomic DB transaction: INSERT COMPOSITE alert + UPDATE the referenced SIGNAL_LOSS alert to `SUPERSEDED` (setting `superseded_by = composite_alert_id`). Then broadcasts two `alert-events` messages: `ALERT_CREATED` (for the composite) and `ALERT_SUPERSEDED` (for the old alert). The dashboard shows the COMPOSITE as the active incident and links the superseded alert in the history view.
- **Alert status changes:** `PATCH /alerts/:alert_id` updates the `alerts` table and publishes an `ALERT_STATUS_CHANGED` message to `alert-events` so all instances can update their WebSocket clients.
- All routes and WebSocket upgrades require a valid JWT. `POST /auth/google` is the only unauthenticated endpoint.
- Subscribes to `position-updates` and `alert-events` on startup. Fans each event to all WebSocket connections whose saved scope matches.
- Scope filtering on position events: entity position must be within `scope.geo_region.bounds`. Scope filtering on alerts: use the immutable detection-location in the alert payload (not the current Redis position — see ADR-012).
- `GET /entities` must also apply workspace scope server-side — returns only entities within the operator's geo region (Phase 03+).
- Alert fan-out uses `alert-events` pub/sub rather than direct WebSocket push from the Kafka-consuming instance — all instances must be able to deliver to their own WebSocket clients.
- Does not write to Neo4j. Writes to Redis only via pub/sub publish (not a persistent key write).
- In-memory WebSocket connection map is rebuilt on restart — scope is reloaded from `user_workspaces` on each new WebSocket upgrade. Scope update → WebSocket reconnect (not an in-band message on the existing connection).

---

### Dashboard

**Runtime:** Angular (ADR-009)
**Concern:** Render the live map, alert feed, and investigation panel.

| Direction | What |
| --- | --- |
| Connects to | API via WebSocket (live position and alert push) |
| Calls | API REST endpoints (investigation, alert lifecycle, workspace management) |

**Contract:**

- Communicates only with the API. Never reads from persistence stores directly.
- Scope is configured in the workspace and applied server-side. The dashboard renders what it receives.

---

## Kafka Topics

| Topic | Producer | Consumer(s) | Purpose |
| --- | --- | --- | --- |
| `adsb.raw` | Ingestion Poller | Position Consumer | Raw ADS-B events, unmodified bytes |
| `ais.raw` | Ingestion Poller | Position Consumer | Raw AIS events, unmodified bytes |
| `adsb.dlq` | Position Consumer | Operator (manual inspection) | Malformed ADS-B events with rejection reason |
| `ais.dlq` | Position Consumer | Operator (manual inspection) | Malformed AIS events with rejection reason |
| `position.normalized` | Position Consumer | Correlation Worker, Deviation Detector | Parsed, normalised position events |
| `deviation.candidates` | Deviation Detector | Alert Evaluator | `OUT_OF_RANGE` / `IN_RANGE` status per entity per ping; short retention (1h) |
| `proximity.candidates` | Correlation Worker | Alert Evaluator | New proximity episode starts for unscheduled pairs; short retention (1h) |
| `alerts` | Alert Evaluator | API | Alert events with `alert_id`, type, entity, payload |

**Consumer groups:**

| Consumer group | Service | Topic(s) |
| --- | --- | --- |
| `position-consumer` | Position Consumer | `adsb.raw`, `ais.raw` |
| `correlation-worker` | Correlation Worker | `position.normalized` |
| `deviation-detector` | Deviation Detector | `position.normalized` |
| `alert-evaluator` | Alert Evaluator | `deviation.candidates`, `proximity.candidates` |
| `api` | API | `alerts` |

---

## Persistence Ownership

### TimescaleDB

| Table / Object | Owner (writes) | Readers |
| --- | --- | --- |
| `position_history` | Position Consumer | Alert Evaluator (last known position for signal loss payload), API |
| `route_references` + `route_reference_points` | Manual seed / synthetic generator | Deviation Detector |
| `alerts` | API (on Kafka consume) | API |
| `users` | API | API |
| `user_workspaces` | API | API |

`route_baseline` continuous aggregate: **not used**. Route deviation in v1 uses `route_references` + `route_reference_points` (static tables, seeded at startup). See ADR-015.

### Neo4j

| Object | Owner (writes) | Readers |
| --- | --- | --- |
| `Entity` nodes | Correlation Worker | Alert Evaluator, API |
| `PROXIMITY_EVENT` edges | Correlation Worker | Alert Evaluator, API |
| `KNOWN_ASSOCIATE` edges | Manual / future import | Alert Evaluator, API |

### Redis

| Key / Channel | Writer | Reader | Notes |
| --- | --- | --- | --- |
| `entity:live:{entity_id}` | Position Consumer | Alert Evaluator, Correlation Worker, API | Hash: lat, lon, `last_seen_ms`; TTL = 24h (safety-net only — key must outlive `SIGNAL_LOSS_THRESHOLD_MS` so the evaluator can scan it; dashboard cleanup is client-side) |
| `geo-cell:{h3_cell_id}` | Position Consumer | Correlation Worker | Sorted set; score = `last_seen_ms`; ZRANGEBYSCORE filters to fresh members; stale members age out logically via score filter |
| `proximity-episode:{pair_key}` | Correlation Worker | Correlation Worker | Proximity episode state; hash with `episode_start_ms`, `last_seen_ms`, `candidate_published`; TTL = PROXIMITY_EPISODE_GAP_MS; canonical pair = `min(a,b):max(a,b)` |
| `recent-loss:{entity_id}` | Position Consumer | Alert Evaluator | Short-lived signal loss record; hash with `dark_since_ms`, `resumed_at_ms`, `signal_loss_alert_id`; TTL = COMPOSITE_CORRELATION_WINDOW_MS; enables composite after entity resumes |
| `deviation-state:{entity_id}` | Alert Evaluator | Alert Evaluator | Hash: `count`, `episode_start_ms`, `last_processed_ms`, `alert_emitted`; safety TTL = `DEVIATION_STATE_TTL_MS` (default: 24h); DEL on IN_RANGE |
| `alert-state:{entity_id}` | Alert Evaluator | Alert Evaluator, Correlation Worker | Hash: `dark_since_ms`, `signal_loss_alert_id`; no TTL; deleted by Position Consumer (after writing recent-loss) on entity resume |
| `alert-evaluator:leader` | Alert Evaluator | Alert Evaluator | SET NX PX lease (acquire); Lua compare-and-expire on heartbeat (ownership-safe renewal); compare-before-DEL on release |
| `position-updates` (pub/sub) | Position Consumer | API | Broadcast channel; every normalised position event |
| `alert-events` (pub/sub) | API (consuming instance) | API (all instances) | Broadcast channel; every alert after TimescaleDB write; all instances fan out to scope-matched WebSocket connections |

---

## Data Flow Summary

```text
External Feeds
  └─ Ingestion Poller ──► adsb.raw / ais.raw
                                │
                         Position Consumer
                          ├─► TimescaleDB (position_history)
                          ├─► Redis (entity:live:{id}, last_seen_ms; TTL=24h)
                          ├─► Redis (geo-cell:{h3_cell_id} sets — SREM old, SADD new)
                          ├─► Redis pub/sub (position-updates) ──► API ──► WebSocket (scoped)
                          ├─► adsb.dlq / ais.dlq (malformed)
                          └─► position.normalized
                                    ├─► Correlation Worker
                                    │     ├─ reads Redis geo-cell:{id} + entity:live:{id} (H3 cell scoped)
                                    │     ├─► Neo4j (PROXIMITY_EVENT edges)
                                    │     └─► proximity.candidates
                                    └─► Deviation Detector
                                          ├─ reads TimescaleDB (route_reference_points)
                                          └─► deviation.candidates

Alert Evaluator (leader-elected)
  ├─ consumes deviation.candidates
  ├─ consumes proximity.candidates
  ├─ reads Redis (entity:live:* scan → signal loss detection)
  ├─ reads TimescaleDB (position_history → last known position for signal loss payload)
  ├─ reads Neo4j (composite alert context — targeted lookup only)
  └─► alerts ──► API (one instance, Kafka consumer group)
                    ├─► TimescaleDB (alerts table, idempotent)
                    └─► Redis pub/sub alert-events ──► all API instances ──► WebSocket (scoped)

Dashboard ◄──► API (REST + WebSocket)
               ├─ reads Redis (entity:live:* for initial map load)
               ├─ reads TimescaleDB (alerts, position_history, workspaces)
               └─ reads Neo4j (investigation pivot)
```

---

## ADR Index

| ADR | Scope |
| --- | --- |
| [ADR-001](adr/ADR-001-kafka-over-http-ingestion.md) | Kafka over direct HTTP ingestion |
| [ADR-002](adr/ADR-002-timescaledb-over-cassandra.md) | TimescaleDB for position history |
| [ADR-003](adr/ADR-003-neo4j-entity-graph.md) | Neo4j for entity relationship graph |
| [ADR-004](adr/ADR-004-redis-live-state.md) | Redis for live entity state, pub/sub, and leader election |
| [ADR-005](adr/ADR-005-leader-election-alert-evaluator.md) | Leader election for alert evaluator |
| [ADR-006](adr/ADR-006-geo-cell-sharding-key.md) | Geo-cell sharding key design |
| [ADR-007](adr/ADR-007-idempotency-key-schema.md) | Idempotency key schema |
| [ADR-008](adr/ADR-008-express-api-layer.md) | Express (Node.js) for the API layer |
| [ADR-009](adr/ADR-009-angular-dashboard.md) | Angular + Leaflet for the dashboard |
| [ADR-010](adr/ADR-010-alert-state-store.md) | Alert lifecycle state in TimescaleDB |
| [ADR-011](adr/ADR-011-google-oauth-operator-auth.md) | Google OAuth 2.0 for operator authentication |
| [ADR-012](adr/ADR-012-workspace-scope-alert-filtering.md) | Workspace scope and server-side alert filtering |
| [ADR-013](adr/ADR-013-nodejs-ingestion-poller.md) | Node.js for the ingestion poller |
| [ADR-014](adr/ADR-014-alert-evaluator-hybrid-input-model.md) | Hybrid input model for the Alert Evaluator |
| [ADR-015](adr/ADR-015-v1-reference-route-model.md) | v1 reference route model for deviation detection |
