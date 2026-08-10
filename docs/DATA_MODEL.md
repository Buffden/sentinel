# Data Model

Canonical schemas for every store Sentinel uses. This is the reference for schema migrations, POC validation, and service implementation. All field names here are authoritative — use them consistently in code.

---

## TimescaleDB

TimescaleDB runs as a PostgreSQL extension (`timescale/timescaledb-ha`). It hosts hypertables and continuous aggregates for time-series data, and plain PostgreSQL tables for relational data that does not need time-based chunking.

---

### `position_history` (hypertable)

Primary write target for every normalised position ping. Partitioned by TimescaleDB on `time_bucket`. Chunk interval: **1 day**. Retention: **30 days**.

| Column | Type | Nullable | Description |
|---|---|---|---|
| `entity_id` | TEXT | No | Entity identifier — ICAO hex for aircraft, MMSI for vessels |
| `entity_type` | TEXT | No | `aircraft` or `vessel` or `synthetic` |
| `timestamp_ms` | BIGINT | No | Unix ms from source telemetry; idempotency key component |
| `time_bucket` | TIMESTAMPTZ | No | `timestamp_ms` truncated to day boundary; hypertable partition column |
| `geo_cell` | TEXT | No | H3 cell ID at resolution 5 (~252 km² per cell); computed at ingest from lat/lon |
| `lat` | DOUBLE PRECISION | No | Decimal degrees |
| `lon` | DOUBLE PRECISION | No | Decimal degrees |
| `altitude` | REAL | Yes | Metres; NULL for vessels |
| `source` | TEXT | No | `adsb`, `ais`, or `synthetic` |

**Constraints and indexes:**
- Unique constraint on `(entity_id, timestamp_ms)` — `ON CONFLICT DO NOTHING` makes every insert idempotent (ADR-007)
- Index on `(geo_cell, time_bucket)` — serves regional time-window queries
- Index on `(entity_id, time_bucket)` — serves single-entity timeline queries

**Design notes:**
- `geo_cell` is computed by the position consumer using the H3 library before the insert. The database never derives it. See ADR-006.
- Daily chunks are chosen for the expected write rate (1–10 pings/s across hundreds to thousands of entities). POC-03 validates this choice.
- Retention is 30 days to match the `route_baseline` look-back window. Older data has no active query pattern in v1.

---

### `route_baseline` (continuous aggregate)

A TimescaleDB continuous aggregate materialised from `position_history`. Computes the average track per entity per 1-hour bucket. **No service writes to this table** — TimescaleDB refreshes it automatically in the background. See ADR-002.

| Column | Type | Description |
|---|---|---|
| `entity_id` | TEXT | Entity identifier |
| `time_bucket` | TIMESTAMPTZ | 1-hour bucket derived from `position_history.time_bucket` |
| `avg_lat` | DOUBLE PRECISION | Mean latitude across all pings in the bucket |
| `avg_lon` | DOUBLE PRECISION | Mean longitude across all pings in the bucket |
| `stddev_metres` | DOUBLE PRECISION | Approximate standard deviation of positions in metres across the bucket (derived from lat/lon stddev) |
| `sample_count` | BIGINT | Number of pings contributing to this bucket |

**Design notes:**
- Look-back window: **30 days**. An entity needs at least 30 days of position history for a meaningful baseline. New entities produce no route deviation alerts until sufficient history accumulates.
- Refresh policy: every hour, covering up to the last 30 days.
- The Deviation Detector queries by `(entity_id, time_bucket)` and reads `avg_lat`, `avg_lon`, `stddev_metres` to compare the incoming position against the expected route. See ADR-014.

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
| `status` | TEXT | No | `NEW`, `ACKNOWLEDGED`, or `RESOLVED`; defaults to `NEW` |
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
- This table is the durable dedup layer (Kafka replay idempotency). It is distinct from `alert-state:{entity_id}` in Redis, which is the in-loop suppression layer. See doc-gaps.md decision #3.

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
| `alert_types` | string[] | Subset of `SIGNAL_LOSS`, `ROUTE_DEVIATION`, `UNSCHEDULED_PROXIMITY`, `COMPOSITE` |

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

Written by the correlation worker when two entities are detected within the configured distance threshold. `MERGE` on `idempotency_key` — replay safe.

| Property | Type | Description |
|---|---|---|
| `idempotency_key` | String | `{entity_a_id}:{timestamp_ms}` — unique per detection event |
| `timestamp_ms` | Long | Unix ms when proximity was detected |
| `lat` | Float | Latitude of the midpoint between the two entities |
| `lon` | Float | Longitude of the midpoint |
| `distance_metres` | Float | Distance between the two entities at detection time |

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

### `geo-cell:{h3_cell_id}` (set)

Spatial index for live proximity candidate scoping. One key per occupied H3 cell (resolution 5); value is the set of entity_ids currently in that cell.

- **Writer:** Position consumer — on each normalised ping: `SREM geo-cell:{old_geo_cell} {entity_id}` then `SADD geo-cell:{new_geo_cell} {entity_id}` (old cell tracked via prior hash value)
- **Reader:** Correlation Worker — reads same-cell + k-ring(1) sets (7 cells, ~1764 km² coverage) to get candidate entity_ids before fetching their positions from `entity:live:*`
- **TTL:** None — set membership reflects the current ping stream; stale entries are overwritten naturally as entities move between cells. A periodic cleanup pass may be needed in production but is out of scope for v1.

**Design notes:**
- Replaces a full `entity:live:*` keyspace scan in the Correlation Worker. Without this index, proximity detection requires O(n) Redis reads and O(n²) Haversine comparisons. With it, comparisons are bounded by cell density — typically a small fraction of total tracked entities.
- H3 resolution 5 (~252 km² per cell) matches the `geo_cell` field already computed by the Position Consumer and stored in `position_history`. No new computation is needed — the cell is read directly from the `position.normalized` event.

---

### `alert-state:{entity_id}` (string)

In-loop alert suppression flag. Prevents the alert evaluator from re-emitting a signal loss alert on every evaluation cycle while an entity remains dark.

| Value | Description |
|---|---|
| `{dark_since_ms}` | Unix ms when the entity was first detected as dark |

- **Writer:** Alert evaluator on first signal loss emission
- **Reader:** Alert evaluator (suppression check + composite alert loss-window start via US-06)
- **TTL:** None — key lives indefinitely
- **Deleted by:** Position consumer on the entity's next successful write (entity resumes broadcasting)

---

### `deviation-counter:{entity_id}` (string / integer)

Consecutive out-of-baseline ping count for sustained route deviation detection (US-04).

- **Writer:** Alert evaluator — INCR on each `OUT_OF_RANGE` event from `deviation.candidates`; DEL on `BACK_IN_RANGE` event
- **Reader:** Alert evaluator only
- **TTL:** None — managed explicitly by the evaluator

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

Broadcast channel for alert events. Published by the API instance that consumed the alert from Kafka, after writing to the `alerts` TimescaleDB table. All API instances subscribe and fan out to scope-matched WebSocket connections.

- **Publisher:** API (the Kafka-consuming instance)
- **Subscribers:** All API instances — fan out to scope-matched WebSocket connections

**Why this exists:** The `alerts` Kafka topic is consumed by consumer group `api`, so Kafka assigns each alert to exactly one API instance. WebSocket connections are local to each instance. Without this channel, users connected to non-consuming instances would never receive alert pushes. This mirrors the `position-updates` pattern.

**Message fields:** full alert event — `alert_id`, `entity_id`, `entity_type`, `alert_type`, `priority`, `detected_at_ms`, `payload`

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
| `geo_cell` | string | H3 cell ID at resolution 5; computed by position consumer |

---

### `deviation.candidates`

Published by the deviation detector. Consumed by the alert evaluator. Retention: **1 hour** (short — stale candidates have no value; durable facts remain in `position_history` and `route_baseline`). See ADR-014.

| Field | Type | Description |
| --- | --- | --- |
| `entity_id` | string | ICAO hex or MMSI |
| `timestamp_ms` | number | Unix ms of the position ping |
| `status` | string | `OUT_OF_RANGE` or `BACK_IN_RANGE` |
| `current_position` | object | `{ lat, lon }` — position at time of event |
| `baseline_position` | object | `{ avg_lat, avg_lon }` — expected position from `route_baseline`; omitted on `BACK_IN_RANGE` |
| `deviation_metres` | number | Distance between current and baseline position; omitted on `BACK_IN_RANGE` |

---

### `proximity.candidates`

Published by the correlation worker for unscheduled proximity pairs (no `KNOWN_ASSOCIATE` edge). Consumed by the alert evaluator. Retention: **1 hour** (short — stale candidates have no value; the durable edge remains in Neo4j). See ADR-014.

| Field | Type | Description |
| --- | --- | --- |
| `entity_a_id` | string | First entity ICAO hex or MMSI |
| `entity_b_id` | string | Second entity ICAO hex or MMSI |
| `timestamp_ms` | number | Unix ms when proximity was detected |
| `lat` | number | Latitude of the midpoint between the two entities |
| `lon` | number | Longitude of the midpoint |
| `distance_metres` | number | Distance between the two entities at detection time |

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
| `ROUTE_DEVIATION` | `current_lat`, `current_lon`, `baseline_lat`, `baseline_lon`, `deviation_metres`, `sustained_cycles` |
| `UNSCHEDULED_PROXIMITY` | `entity_b_id`, `lat`, `lon`, `distance_metres` |
| `COMPOSITE` | `signal_loss` (nested), `proximity` (nested), `correlation_window_ms` |

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
