# Data Model

Canonical schemas for every store Sentinel uses. This is the reference for schema migrations, POC validation, and service implementation. All field names here are authoritative — use them consistently in code.

---

## TimescaleDB

TimescaleDB runs as a PostgreSQL extension (`timescale/timescaledb-ha`). It hosts hypertables and continuous aggregates for time-series data, and plain PostgreSQL tables for relational data that does not need time-based chunking.

---

### `position_history` (hypertable)

Primary write target for every normalised position ping. Partitioned by TimescaleDB on `observed_at`. Chunk interval: **1 day**. Retention: **30 days**.

| Column | Type | Nullable | Description |
|---|---|---|---|
| `entity_id` | TEXT | No | Entity identifier — ICAO hex for aircraft, MMSI for vessels |
| `entity_type` | TEXT | No | `aircraft` or `vessel` or `synthetic` |
| `observed_at` | TIMESTAMPTZ | No | Canonical hypertable time column — `to_timestamp(timestamp_ms / 1000.0)` computed at ingest; TimescaleDB partitions on this column |
| `timestamp_ms` | BIGINT | No | Unix ms from source telemetry; idempotency key component; preserved as source metadata |
| `geo_cell` | TEXT | No | H3 cell ID at resolution 5 (~252 km² per cell); computed at ingest from lat/lon |
| `lat` | DOUBLE PRECISION | No | Decimal degrees |
| `lon` | DOUBLE PRECISION | No | Decimal degrees |
| `altitude` | REAL | Yes | Metres; NULL for vessels |
| `source` | TEXT | No | `adsb`, `ais`, or `synthetic` |

**Constraints and indexes:**
- Unique constraint on `(entity_id, observed_at)` — `ON CONFLICT DO NOTHING` makes every insert idempotent (ADR-007). TimescaleDB requires the partition column (`observed_at`) in every unique constraint on a hypertable. `timestamp_ms` is the logical idempotency key; `observed_at = to_timestamp(timestamp_ms / 1000.0)` is deterministically derived from it, so the constraint is equivalent in practice.
- Index on `(entity_id, observed_at DESC)` — serves single-entity timeline queries
- Index on `(geo_cell, observed_at DESC)` — serves regional time-window queries

**Design notes:**
- `observed_at` is the TimescaleDB partition column. It must be TIMESTAMPTZ — TimescaleDB cannot partition on BIGINT. `timestamp_ms` is kept as source metadata and the idempotency key component; it is not the partition column.
- `geo_cell` is a query index column, not a partition dimension. TimescaleDB partitions on time only (`observed_at`); geo_cell narrows queries within a chunk. See ADR-006.
- `geo_cell` is computed by the position consumer using the H3 library before the insert. The database never derives it.
- Daily chunks are chosen for the expected write rate (1–10 pings/s across hundreds to thousands of entities). POC-03 validates this choice.
- Retention is 30 days. Older data has no active query pattern in v1.

---

### `route_references` (plain table)

Route header records for synthetic entities. Seeded from the synthetic generator at startup. Only synthetic entities have reference routes — real ADS-B and AIS entities do not have route deviation detection in v1. See ADR-015.

| Column | Type | Nullable | Description |
|---|---|---|---|
| `route_id` | TEXT | No | Primary key — unique route identifier |
| `entity_id` | TEXT | No | Synthetic entity identifier |
| `route_name` | TEXT | No | Human-readable route label |
| `corridor_threshold_metres` | REAL | No | Acceptable lateral deviation from the route before `OUT_OF_RANGE` is emitted |
| `source` | TEXT | No | `synthetic` or `manual` |
| `created_at` | TIMESTAMPTZ | No | Creation timestamp |

**Constraints and indexes:**
- Primary key on `route_id`
- Index on `entity_id` — Deviation Detector fetches the route for an entity by ID

---

### `route_reference_points` (plain table)

Ordered waypoints for each route. The Deviation Detector fetches all points for a route and computes the minimum perpendicular distance from the current position to each route segment (point[i] → point[i+1]).

| Column | Type | Nullable | Description |
|---|---|---|---|
| `route_id` | TEXT | No | FK → `route_references.route_id` |
| `sequence_no` | INTEGER | No | Waypoint order (0-based) — adjacent pairs define route segments |
| `lat` | DOUBLE PRECISION | No | Waypoint latitude |
| `lon` | DOUBLE PRECISION | No | Waypoint longitude |

**Constraints and indexes:**
- Primary key on `(route_id, sequence_no)`

**Design notes:**
- The Deviation Detector fetches the full waypoint list, then for each ping finds the nearest route **segment** (not nearest waypoint) by computing minimum perpendicular distance from the entity's position to each segment (point[i] → point[i+1]). If the perpendicular foot falls outside the segment, the distance is the minimum of the two endpoint distances.
- Route deviation detection is **synthetic-only in v1**. Real entities produce no deviation alerts. This eliminates the 30-day cold-start problem of statistical baselines and gives deterministic, injectable anomalies for demos.
- The `route_baseline` continuous aggregate originally planned here was dropped — averaging lat/lon per hour does not produce a meaningful route corridor.

---

### `alerts` (plain table)

Durable alert lifecycle state. Written by the API on Kafka consume. Not a hypertable — alert volume is low and does not warrant time-based chunking. See ADR-010.

| Column | Type | Nullable | Description |
|---|---|---|---|
| `alert_id` | TEXT | No | Primary key — format: `{entity_id}:{alert_type}:{window_start_ms}` |
| `entity_id` | TEXT | No | The entity the alert is about |
| `entity_type` | TEXT | No | `aircraft` or `vessel` |
| `alert_type` | TEXT | No | `SIGNAL_LOSS`, `ROUTE_DEVIATION`, `UNSCHEDULED_PROXIMITY`, or `COMPOSITE` |
| `priority` | TEXT | No | `STANDARD` or `ELEVATED` (COMPOSITE alerts are always ELEVATED) |
| `status` | TEXT | No | `NEW`, `ACKNOWLEDGED`, `RESOLVED`, or `SUPERSEDED`; defaults to `NEW` |
| `superseded_by` | TEXT | Yes | FK → `alerts.alert_id`; set when a COMPOSITE alert supersedes this alert |
| `payload` | JSONB | No | Type-specific fields (see Kafka alert schema below) |
| `detected_at` | TIMESTAMPTZ | No | When the anomaly was first detected |
| `updated_at` | TIMESTAMPTZ | No | Last status change timestamp |
| `acknowledged_at` | TIMESTAMPTZ | Yes | When the alert was acknowledged; NULL until acknowledged |
| `acknowledged_by` | UUID | Yes | FK → `users.user_id`; NULL until acknowledged |
| `resolved_at` | TIMESTAMPTZ | Yes | When the alert was resolved; NULL until resolved |
| `resolved_by` | UUID | Yes | FK → `users.user_id`; NULL until resolved |

**Constraints and indexes:**
- Primary key on `alert_id`
- Index on `(entity_id, detected_at DESC)` — investigation panel queries
- Index on `(status, detected_at DESC)` — operator alert feed filtered by status
- Index on `(alert_type, detected_at DESC)` — alert type filter

**Design notes:**
- `alert_id` is deterministic: derived from the Kafka alert event, so `ON CONFLICT DO NOTHING` makes Kafka replay fully idempotent.
- `SUPERSEDED` status: a SIGNAL_LOSS or UNSCHEDULED_PROXIMITY alert may be superseded by a COMPOSITE alert after correlation. When the API inserts the COMPOSITE alert, it atomically marks the referenced individual alert(s) as `SUPERSEDED` and records the composite `alert_id` in `superseded_by`. The dashboard shows the COMPOSITE as the active incident; superseded alerts appear in the evidence/history view linked from the composite.
- This table is the durable dedup layer (Kafka replay idempotency). It is distinct from `alert-state:{entity_id}` in Redis, which is the in-loop suppression layer.

---

### `users` (plain table)

One row per authenticated operator. Written by the API on first Google OAuth login. See ADR-011.

| Column | Type | Nullable | Description |
|---|---|---|---|
| `user_id` | UUID | No | Primary key; generated at insert |
| `google_sub` | TEXT | No | Stable Google user ID from ID token `sub` claim; unique |
| `email` | TEXT | No | From verified Google ID token |
| `last_login_at` | TIMESTAMPTZ | No | Updated on every login |
| `created_at` | TIMESTAMPTZ | No | Set once at first login |

---

### `user_workspaces` (plain table)

One row per operator. Holds the operator's saved scope for server-side alert filtering and dashboard state restore. See ADR-012.

| Column | Type | Nullable | Description |
|---|---|---|---|
| `user_id` | UUID | No | Primary key; FK → `users.user_id` |
| `scope` | JSONB | No | See scope shape below |
| `updated_at` | TIMESTAMPTZ | No | Updated on every scope change |

**Scope JSONB shape:**

| Field | Type | Description |
|---|---|---|
| `geo_region.name` | string | Named region (e.g. `France`) |
| `geo_region.bounds` | object | `min_lat`, `max_lat`, `min_lon`, `max_lon` |
| `entity_types` | string[] | `aircraft`, `vessel`, or both |
| `alert_types` | string[] | Subset of `SIGNAL_LOSS`, `ROUTE_DEVIATION`, `UNSCHEDULED_PROXIMITY`, `COMPOSITE` — must use `UNSCHEDULED_PROXIMITY` (not `PROXIMITY`) to match the canonical `alert_type` enum |

---

## Neo4j

Neo4j stores the entity relationship graph. All writes use `MERGE` on an idempotency key — replay safe. See ADR-003.

---

### Node: `Entity`

One node per tracked entity. Created by the correlation worker on first encounter.

| Property | Type | Description |
|---|---|---|
| `id` | String | Entity identifier — ICAO hex for aircraft, MMSI for vessels |
| `type` | String | `aircraft` or `vessel` |
| `name` | String | Callsign or vessel name; nullable |

Unique index on `Entity.id`.

---

### Edge: `PROXIMITY_EVENT`

Written by the correlation worker when a new proximity episode begins between two entities. One edge per episode — not one per ping. `MERGE` on `idempotency_key` — replay safe.

| Property | Type | Description |
|---|---|---|
| `idempotency_key` | String | `{min(entity_a_id, entity_b_id)}:{max(entity_a_id, entity_b_id)}:{episode_start_ms}` — one edge per proximity episode; canonical pair ordering ensures (A,B) and (B,A) map to the same key |
| `episode_start_ms` | Long | Unix ms when this proximity episode began |
| `last_seen_ms` | Long | Unix ms of the most recent ping confirming proximity (updated during episode) |
| `min_distance_metres` | Float | Closest approach distance observed during the episode (updated during episode) |
| `lat` | Float | Latitude of the midpoint at episode start |
| `lon` | Float | Longitude of the midpoint at episode start |
| `distance_at_detection` | Float | Distance between the two entities at first detection |

---

### Edge: `KNOWN_ASSOCIATE`

Marks a pre-existing, expected relationship between two entities (e.g. same fleet). The alert evaluator filters out `KNOWN_ASSOCIATE` pairs when evaluating composite alerts — proximity between known associates is not anomalous.

| Property | Type | Description |
|---|---|---|
| `established_at` | Long | Unix ms when the relationship was recorded |
| `relationship_type` | String | e.g. `same_fleet`, `scheduled_route_pair` |

In v1, `KNOWN_ASSOCIATE` edges are seeded manually. No service creates them at runtime.

---

## Redis

Redis holds high-frequency ephemeral state. All keys and their lifecycle are defined here and in CLAUDE.md. See ADR-004.

---

### `entity:live:{entity_id}` (hash)

Current position and liveness state for each tracked entity.

| Field | Type | Description |
|---|---|---|
| `lat` | String (float) | Latest latitude |
| `lon` | String (float) | Latest longitude |
| `altitude` | String (float) | Latest altitude in metres; absent for vessels |
| `entity_type` | String | `aircraft` or `vessel` |
| `last_seen_ms` | String (int) | Unix ms of the last received ping from source telemetry |

- **Writer:** Position consumer on every normalised ping
- **Readers:** Alert evaluator (scheduled scan for signal loss detection), Correlation Worker (position fetch for H3-scoped proximity candidates), API (initial map load, investigation panel, scope geo-check)
- **TTL:** 24h — safety-net only; prevents permanent ghost keys if an entity disappears before the evaluator detects it. The key must outlive `SIGNAL_LOSS_THRESHOLD_MS` or the evaluator's scan finds nothing to inspect. Dashboard cleanup is client-side via `last_seen_ms` comparison.

---

### `geo-cell:{h3_cell_id}` (sorted set)

Spatial index for live proximity candidate scoping. One key per occupied H3 cell (resolution 5); value is a sorted set of `entity_id` members with score = `last_seen_ms`.

- **Writer:** Position consumer — on each normalised ping: `ZREM geo-cell:{old_geo_cell} {entity_id}` then `ZADD geo-cell:{new_geo_cell} {last_seen_ms} {entity_id}` (old cell retrieved from prior hash value)
- **Reader:** Correlation Worker — `ZRANGEBYSCORE geo-cell:{cell} {(now - SIGNAL_LOSS_THRESHOLD_MS)} +inf` for same-cell + k-ring(1) cells (7 cells); returns only fresh members. After fetching positions from `entity:live:*`, rechecks `last_seen_ms` and skips any stale candidate.
- **TTL:** None — stale members age out logically via the `ZRANGEBYSCORE` lower-bound score filter. Members from permanently disappeared entities are never returned.

**Design notes:**
- Using a sorted set (score = `last_seen_ms`) replaces the plain SET. A plain SET accumulates members from permanently disappeared entities with no way to age them out — "overwritten naturally as entities move cells" was incorrect. Entities that stop broadcasting never trigger a `ZREM`. The score filter makes staleness implicit without requiring explicit cleanup.
- k-ring(1) at H3 resolution 5 covers 7 cells (~1764 km²). This only guarantees no misses when `PROXIMITY_THRESHOLD_METRES < 9850m` (H3 resolution-5 average edge length ~9.85 km). If the threshold is larger, increase k-ring size. For v1, document this constraint in CLAUDE.md.
- H3 resolution 5 matches the `geo_cell` field already computed by the Position Consumer — no new computation required.

---

### `alert-state:{entity_id}` (hash)

In-loop alert suppression flag. Prevents the alert evaluator from re-emitting a signal loss alert on every evaluation cycle while an entity remains dark. Also carries the `alert_id` of the emitted signal loss alert for use in composite supersession.

| Field | Type | Description |
|---|---|---|
| `dark_since_ms` | String (int) | Unix ms when the entity was first detected as dark |
| `signal_loss_alert_id` | String | The `alert_id` of the SIGNAL_LOSS alert emitted for this dark episode |

- **Writer:** Alert evaluator on first signal loss emission
- **Reader:** Alert evaluator (suppression check), Correlation Worker (composite supersession — checks for active dark entity when proximity arrives)
- **TTL:** None — key lives indefinitely
- **Deleted by:** Position consumer — before deleting, writes `recent-loss:{entity_id}` (see below) so the correlation window survives the entity coming back online; then DEL this key

---

### `recent-loss:{entity_id}` (hash)

Short-lived record of a recently resolved signal loss episode. Written by the Position Consumer when an entity resumes broadcasting, so that a subsequent proximity event arriving after the entity comes back online can still trigger a COMPOSITE alert within the correlation window.

| Field | Type | Description |
|---|---|---|
| `dark_since_ms` | String (int) | Unix ms when the dark episode began |
| `resumed_at_ms` | String (int) | Unix ms when the entity first resumed broadcasting |
| `signal_loss_alert_id` | String | The `alert_id` of the SIGNAL_LOSS alert emitted for this episode |

- **Writer:** Position consumer — written atomically before deleting `alert-state:{entity_id}` when the entity resumes
- **Reader:** Alert Evaluator — checked when a `proximity.candidates` event arrives; if present and proximity timestamp falls within the correlation window, emit COMPOSITE and supersede the referenced SIGNAL_LOSS alert
- **TTL:** `COMPOSITE_CORRELATION_WINDOW_MS` — expires automatically; after expiry a proximity event with the same entity produces UNSCHEDULED_PROXIMITY only (no composite)

---

### `deviation-state:{entity_id}` (hash)

Route deviation episode state for sustained deviation detection (US-04). Owns all episode state on behalf of the stateless Deviation Detector.

| Field | Type | Description |
|---|---|---|
| `count` | String (int) | Consecutive `OUT_OF_RANGE` ping count in the current episode |
| `episode_start_ms` | String (int) | Unix ms when the current deviation episode began (first OUT_OF_RANGE ping) |
| `last_processed_ms` | String (int) | Unix ms of the last `deviation.candidates` event processed — replay guard |
| `alert_emitted` | String (`0` or `1`) | Whether an alert has been emitted for the current episode — prevents re-emission on subsequent pings |

- **Writer:** Alert evaluator — on `OUT_OF_RANGE`: guard `timestamp_ms > last_processed_ms`; `HINCRBY count 1`; set `episode_start_ms` on count==1; set `alert_emitted=1` after emitting; update `last_processed_ms`; on `IN_RANGE`: DEL
- **Reader:** Alert evaluator only
- **TTL:** Safety TTL of `DEVIATION_STATE_TTL_MS` (default: 24h) — prevents abandoned state if the entity goes dark or the Deviation Detector stops publishing; decoupled from signal-loss timing so deviation state survives brief gaps; the evaluator also deletes explicitly on `IN_RANGE`

**Design notes:**
- The Deviation Detector is stateless — it emits `OUT_OF_RANGE` or `IN_RANGE` on **every** eligible ping, not just on transitions. `BACK_IN_RANGE` does not exist; the Detector simply classifies each ping. Episode state lives entirely here.
- `last_processed_ms` is the replay guard: if a `deviation.candidates` event arrives with `timestamp_ms <= last_processed_ms`, it is ignored. This prevents out-of-order or replayed events from resetting or double-incrementing the counter.
- `alert_emitted` prevents re-emission: once the threshold is crossed and an alert is emitted, subsequent `OUT_OF_RANGE` pings increment `count` but do not re-emit until the episode resets via `IN_RANGE`.

---

### `proximity-episode:{pair_key}` (hash)

Active proximity episode state for a specific entity pair. Defines an encounter episode — a continuous period during which two entities remain within threshold of each other. Prevents the Correlation Worker from publishing repeated `proximity.candidates` events for one sustained encounter.

| Field | Type | Description |
|---|---|---|
| `episode_start_ms` | String (int) | Unix ms when this encounter episode began |
| `last_seen_ms` | String (int) | Unix ms of the most recent position ping that confirmed proximity |
| `candidate_published` | String (`0` or `1`) | Whether a `proximity.candidates` event has been successfully published for this episode — set to `0` immediately after creating the hash (before Kafka publish), updated to `1` after successful publish; if `0` on next ping, triggers a re-publish retry |

- **Canonical pair key:** `{min(entity_a_id, entity_b_id)}:{max(entity_a_id, entity_b_id)}` — alphabetically ordered; symmetric regardless of which entity triggered detection
- **Writer:** Correlation Worker — created on first proximity detection for the pair; `last_seen_ms` updated on each subsequent ping within the episode
- **Reader:** Correlation Worker — checked before publishing to `proximity.candidates`; if episode exists and `candidate_published=1`, refresh TTL and update `last_seen_ms` instead of publishing a new candidate; if `candidate_published=0`, retry the Kafka publish
- **TTL:** `PROXIMITY_EPISODE_GAP_MS` — refreshed on each within-threshold detection; when it expires the pair is treated as a new episode on the next detection

**Design notes:**
- Without this key, every `position.normalized` ping from either entity while they are within threshold triggers another `proximity.candidates` event, producing repeated alerts for one encounter.
- The canonical pair ordering (`min:max`) ensures (A,B) and (B,A) map to the same key regardless of which entity's ping arrived first.
- The `candidate_published` field makes Kafka publish recoverable: if the Correlation Worker creates the episode hash and writes to Neo4j but crashes before Kafka publish succeeds, the next ping for the pair finds `candidate_published=0` and retries the publish. Without this field, a crash between Neo4j write and Kafka publish leaves the pair silently unalerted.
- Write order: Neo4j MERGE first → set `candidate_published=0` → Kafka publish → set `candidate_published=1`.
- If the TTL expires and the pair later comes close again, that is a distinct episode with a new `episode_start_ms`. The new episode produces a new Neo4j edge and a new `proximity.candidates` event.
- Neo4j idempotency key for the edge: `{pair_key}:{episode_start_ms}` — one edge per episode, not one per ping.

---

### `alert-evaluator:leader` (string)

Leader election lease key. Held by the active alert evaluator instance. See ADR-005.

| Value | Description |
|---|---|
| `{instance_id}` | Unique identifier of the holding instance |

- **Pattern:** SET NX PX — atomic acquire with TTL; renewed on each heartbeat
- **Expires:** Automatically if the leader crashes; follower acquires on next poll

---

### `position-updates` (pub/sub channel)

Broadcast channel for live position events. Every normalised ping is published here after the Redis hash write.

- **Publisher:** Position consumer
- **Subscribers:** All API instances — fan out to scoped WebSocket connections

**Message fields:** `entity_id`, `entity_type`, `lat`, `lon`, `altitude`, `last_seen_ms`

---

### `alert-events` (pub/sub channel)

Broadcast channel for alert lifecycle events. Published by the API instance that consumed the alert from Kafka or processed a status change, after writing to TimescaleDB. All API instances subscribe and fan out to scope-matched WebSocket connections.

- **Publisher:** API (the instance that wrote to TimescaleDB)
- **Subscribers:** All API instances — fan out to scope-matched WebSocket connections

**Why this exists:** The `alerts` Kafka topic is consumed by consumer group `api`, so Kafka assigns each alert to exactly one API instance. WebSocket connections are local to each instance. Without this channel, users connected to non-consuming instances would never receive alert pushes. This mirrors the `position-updates` pattern. Status changes (acknowledge, resolve, supersede) must also fan out via this channel so all instances update their WebSocket clients.

**Message envelope:**

```json
{
  "type": "ALERT_CREATED | ALERT_STATUS_CHANGED | ALERT_SUPERSEDED",
  "payload": { ... full alert fields ... }
}
```

- `ALERT_CREATED`: new alert consumed from Kafka and written to TimescaleDB
- `ALERT_STATUS_CHANGED`: operator acknowledged or resolved an alert
- `ALERT_SUPERSEDED`: a COMPOSITE alert was created; the superseded SIGNAL_LOSS or UNSCHEDULED_PROXIMITY alert is marked SUPERSEDED

**Message fields in `payload`:** `alert_id`, `entity_id`, `entity_type`, `alert_type`, `priority`, `status`, `detected_at_ms`, `payload`

---

## Kafka Event Schemas

TypeScript field definitions for these schemas live in a shared internal package imported by the ingestion poller, position consumer, correlation worker, deviation detector, alert evaluator, and API. See ADR-013.

---

### `adsb.raw` / `ais.raw`

Raw bytes from the source feed, unparsed. No schema constraint imposed by Sentinel — format fidelity only. The position consumer owns parsing.

---

### `position.normalized`

Published by the position consumer. Consumed by the correlation worker and deviation detector.

| Field | Type | Description |
|---|---|---|
| `entity_id` | string | ICAO hex or MMSI |
| `entity_type` | string | `aircraft` or `vessel` |
| `timestamp_ms` | number | Unix ms from source telemetry — idempotency key component |
| `lat` | number | Decimal degrees |
| `lon` | number | Decimal degrees |
| `altitude` | number \| null | Metres; null for vessels |
| `source` | string | `adsb`, `ais`, or `synthetic` |
| `history_geo_cell` | string | H3 cell ID at `HISTORY_H3_RESOLUTION` — written to the `geo_cell` column in `position_history` (TimescaleDB) |
| `live_geo_cell` | string | H3 cell ID at `LIVE_H3_RESOLUTION` — used as the `geo-cell:*` sorted set key suffix in Redis; may differ from `history_geo_cell` if the two resolutions are configured differently (see ADR-006) |

---

### `deviation.candidates`

Published by the deviation detector. Consumed by the alert evaluator. Retention: **1 hour** (short — stale candidates have no value; durable facts remain in `position_history` and `reference_routes`). See ADR-014.

| Field | Type | Description |
| --- | --- | --- |
| `entity_id` | string | ICAO hex or MMSI |
| `timestamp_ms` | number | Unix ms of the position ping |
| `status` | string | `OUT_OF_RANGE` or `IN_RANGE` — emitted on **every** eligible ping, not just transitions; the Detector is stateless |
| `current_position` | object | `{ lat, lon }` — position at time of event |
| `nearest_segment_index` | number | Index `i` of the first waypoint of the nearest route segment (segment from `route_reference_points[i]` to `route_reference_points[i+1]`); omitted on `IN_RANGE` |
| `deviation_metres` | number | Minimum perpendicular distance from the current position to the nearest route segment; omitted on `IN_RANGE` |

**Design notes:**
- `BACK_IN_RANGE` is replaced by `IN_RANGE`. The Detector no longer tracks transition state — it classifies every ping independently. The Alert Evaluator owns episode state via `deviation-state:{entity_id}`.
- `baseline_position` is removed — route deviation now compares against `route_reference_points` segments using minimum perpendicular distance (point-to-segment), not a lat/lon average. The nearest segment index is included so the evaluator can log which segment of the route was deviated from.

---

### `proximity.candidates`

Published by the correlation worker when a **new** proximity episode begins between an unscheduled pair (no `KNOWN_ASSOCIATE` edge). One event per episode — not one per ping. Consumed by the alert evaluator. Retention: **1 hour** (short — stale candidates have no value; the durable edge remains in Neo4j). See ADR-014.

| Field | Type | Description |
| --- | --- | --- |
| `pair_key` | string | `{min(a,b)}:{max(a,b)}` — canonical pair ordering |
| `entity_a_id` | string | Lexicographically smaller entity identifier |
| `entity_b_id` | string | Lexicographically larger entity identifier |
| `episode_start_ms` | number | Unix ms when this proximity episode began |
| `lat` | number | Latitude of the midpoint at first detection |
| `lon` | number | Longitude of the midpoint |
| `distance_at_detection` | number | Distance between the two entities at first detection |

---

### `alerts`

Published by the alert evaluator. Consumed by the API.

| Field | Type | Description |
|---|---|---|
| `alert_id` | string | `{entity_id}:{alert_type}:{window_start_ms}` |
| `entity_id` | string | |
| `entity_type` | string | `aircraft` or `vessel` |
| `alert_type` | string | `SIGNAL_LOSS`, `ROUTE_DEVIATION`, `UNSCHEDULED_PROXIMITY`, or `COMPOSITE` |
| `priority` | string | `STANDARD` or `ELEVATED` |
| `detected_at_ms` | number | Unix ms |
| `payload` | object | Type-specific fields — see below |

**Payload fields by alert type:**

| Alert type | Payload fields |
|---|---|
| `SIGNAL_LOSS` | `dark_since_ms`, `last_lat`, `last_lon` |
| `ROUTE_DEVIATION` | `current_lat`, `current_lon`, `nearest_segment_index`, `deviation_metres`, `sustained_cycles` |
| `UNSCHEDULED_PROXIMITY` | `pair_key`, `entity_b_id`, `lat`, `lon`, `distance_at_detection`, `episode_start_ms` |
| `COMPOSITE` | `signal_loss` (nested — `dark_since_ms`, `last_lat`, `last_lon`), `proximity` (nested — `pair_key`, `entity_b_id`, `lat`, `lon`, `distance_at_detection`), `correlation_window_ms`, `supersedes_alert_ids` (array of alert_ids being superseded) |

---

### `adsb.dlq` / `ais.dlq`

Dead-letter queue for events the position consumer could not parse. See ADR-001.

| Field | Type | Description |
|---|---|---|
| `raw_payload` | string (base64) | Original unparseable bytes |
| `rejection_reason` | string | Human-readable parse failure description |
| `source_topic` | string | `adsb.raw` or `ais.raw` |
| `source_offset` | number | Kafka partition offset of the original event |
| `consumer_id` | string | Instance ID of the consuming process |
| `timestamp_ms` | number | Unix ms when the event was routed to the DLQ |
