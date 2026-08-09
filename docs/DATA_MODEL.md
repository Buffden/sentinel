# Data Model

Canonical schemas for every store Sentinel uses. This is the reference for schema migrations, POC validation, and service implementation. All field names here are the authoritative names — use them consistently in code.

---

## TimescaleDB

TimescaleDB runs as a PostgreSQL extension (`timescale/timescaledb-ha`). It hosts two categories of objects: hypertables and continuous aggregates for time-series data, and plain PostgreSQL tables for relational data that does not benefit from time-based chunking.

---

### `position_history` (hypertable)

The primary write target for every normalised position ping. Partitioned automatically by TimescaleDB on `time_bucket`.

```sql
CREATE TABLE position_history (
    entity_id    TEXT              NOT NULL,
    entity_type  TEXT              NOT NULL,  -- 'aircraft' | 'vessel' | 'synthetic'
    timestamp_ms BIGINT            NOT NULL,  -- Unix ms from source telemetry; idempotency key component
    time_bucket  TIMESTAMPTZ       NOT NULL,  -- timestamp_ms truncated to chunk interval; hypertable partition column
    geo_cell     TEXT              NOT NULL,  -- H3 cell ID at resolution 5 (computed at ingest, ~252 km² per cell)
    lat          DOUBLE PRECISION  NOT NULL,
    lon          DOUBLE PRECISION  NOT NULL,
    altitude     REAL,                        -- metres; NULL for vessels
    source       TEXT              NOT NULL,  -- 'adsb' | 'ais' | 'synthetic'

    CONSTRAINT position_history_pkey UNIQUE (entity_id, timestamp_ms)  -- idempotency: ON CONFLICT DO NOTHING
);

SELECT create_hypertable('position_history', 'time_bucket', chunk_time_interval => INTERVAL '1 day');

CREATE INDEX ON position_history (geo_cell, time_bucket);   -- regional time-window queries
CREATE INDEX ON position_history (entity_id, time_bucket);  -- single-entity timeline queries
```

**Design notes:**
- `time_bucket` is computed at ingest as `date_trunc('day', to_timestamp(timestamp_ms / 1000.0))`. Daily chunks keep each chunk bounded in size for the expected write rate (~1–10 pings/s per entity, hundreds to thousands of entities).
- `geo_cell` is an H3 cell ID at resolution 5. Computed by the position consumer using the H3 library from `lat` and `lon`. See ADR-006.
- The unique constraint is on `(entity_id, timestamp_ms)`. `ON CONFLICT (entity_id, timestamp_ms) DO NOTHING` makes every insert idempotent. See ADR-007.
- **Retention:** 30 days. Matches the `route_baseline` look-back window.

---

### `route_baseline` (continuous aggregate)

A TimescaleDB continuous aggregate materialised from `position_history`. Computes the average track per entity per 1-hour bucket. No service writes to this table — TimescaleDB refreshes it automatically in the background. See ADR-002.

```sql
CREATE MATERIALIZED VIEW route_baseline
WITH (timescaledb.continuous) AS
SELECT
    entity_id,
    time_bucket(INTERVAL '1 hour', time_bucket)  AS time_bucket,
    avg(lat)                                      AS avg_lat,
    avg(lon)                                      AS avg_lon,
    stddev(lat)                                   AS stddev_lat,
    stddev(lon)                                   AS stddev_lon,
    count(*)                                      AS sample_count
FROM position_history
GROUP BY entity_id, time_bucket(INTERVAL '1 hour', time_bucket);

SELECT add_continuous_aggregate_policy(
    'route_baseline',
    start_offset => INTERVAL '30 days',
    end_offset   => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour'
);
```

**Design notes:**
- The alert evaluator queries this view by `(entity_id, time_bucket)` to retrieve the expected position range for a given entity at a given time of day.
- `stddev_lat` and `stddev_lon` are used to compute `stddev_metres` at query time — the evaluator converts degree-based stddev to metres using the entity's latitude.
- 30-day look-back window: an entity needs at least 30 days of position history to have a meaningful baseline. New entities produce no route deviation alerts until sufficient history accumulates.

---

### `alerts` (plain table)

Durable alert lifecycle state. Written by the API on Kafka consume, updated by API on operator actions. Not a hypertable — alert volume is low relative to position pings and does not need time-based chunking. See ADR-010.

```sql
CREATE TABLE alerts (
    alert_id        TEXT        PRIMARY KEY,  -- '{entity_id}:{alert_type}:{window_start_ms}'
    entity_id       TEXT        NOT NULL,
    entity_type     TEXT        NOT NULL,     -- 'aircraft' | 'vessel'
    alert_type      TEXT        NOT NULL,     -- 'SIGNAL_LOSS' | 'ROUTE_DEVIATION' | 'UNSCHEDULED_PROXIMITY' | 'COMPOSITE'
    priority        TEXT        NOT NULL,     -- 'STANDARD' | 'ELEVATED'
    status          TEXT        NOT NULL DEFAULT 'NEW',  -- 'NEW' | 'ACKNOWLEDGED' | 'RESOLVED'
    payload         JSONB       NOT NULL,     -- type-specific fields (see Kafka alert schema below)
    detected_at     TIMESTAMPTZ NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    acknowledged_by UUID        REFERENCES users(user_id),  -- NULL until acknowledged
    resolved_by     UUID        REFERENCES users(user_id)   -- NULL until resolved
);

CREATE INDEX ON alerts (entity_id, detected_at DESC);
CREATE INDEX ON alerts (status, detected_at DESC);
CREATE INDEX ON alerts (alert_type, detected_at DESC);
```

**Design notes:**
- `alert_id` is deterministic: `{entity_id}:{alert_type}:{window_start_ms}`. Derived from the Kafka alert event, so `INSERT ... ON CONFLICT DO NOTHING` makes Kafka replay fully idempotent.
- `payload` JSONB holds type-specific fields — see alert payload shapes in the Kafka schema section below.

---

### `users` (plain table)

One row per authenticated operator. Written by the API on first Google OAuth login, updated on subsequent logins. See ADR-011.

```sql
CREATE TABLE users (
    user_id       UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    google_sub    TEXT        NOT NULL UNIQUE,  -- stable Google user ID from ID token 'sub' claim
    email         TEXT        NOT NULL,
    last_login_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

---

### `user_workspaces` (plain table)

One row per operator. Holds the operator's saved scope for server-side alert filtering and dashboard restore. See ADR-012.

```sql
CREATE TABLE user_workspaces (
    user_id    UUID        PRIMARY KEY REFERENCES users(user_id) ON DELETE CASCADE,
    scope      JSONB       NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

**`scope` JSONB shape:**

```json
{
  "geo_region": {
    "name": "France",
    "bounds": { "min_lat": 41.3, "max_lat": 51.1, "min_lon": -5.2, "max_lon": 9.6 }
  },
  "entity_types": ["aircraft", "vessel"],
  "alert_types":  ["SIGNAL_LOSS", "ROUTE_DEVIATION", "UNSCHEDULED_PROXIMITY", "COMPOSITE"]
}
```

---

## Neo4j

Neo4j stores the entity relationship graph. All writes use `MERGE` for idempotency. See ADR-003.

---

### Node: `Entity`

One node per tracked entity. Created by the correlation worker on first encounter.

| Property | Type | Description |
|---|---|---|
| `id` | String | Entity identifier — ICAO hex for aircraft, MMSI for vessels |
| `type` | String | `'aircraft'` or `'vessel'` |
| `name` | String | Callsign or vessel name (nullable) |

```cypher
CREATE CONSTRAINT entity_id_unique FOR (e:Entity) REQUIRE e.id IS UNIQUE;
CREATE INDEX entity_type FOR (e:Entity) ON (e.type);
```

---

### Edge: `PROXIMITY_EVENT`

Written by the correlation worker when two entities are detected within the configured distance threshold. Uses `MERGE` on `idempotency_key` — replay safe.

| Property | Type | Description |
|---|---|---|
| `idempotency_key` | String | `{entity_a_id}:{timestamp_ms}` — unique per detection event |
| `timestamp_ms` | Long | Unix ms when the proximity was detected |
| `lat` | Float | Latitude of the proximity event midpoint |
| `lon` | Float | Longitude of the proximity event midpoint |
| `distance_metres` | Float | Distance between the two entities at detection time |

```cypher
MERGE (a)-[e:PROXIMITY_EVENT {idempotency_key: $key}]->(b)
ON CREATE SET e.timestamp_ms = $timestamp_ms,
              e.lat = $lat, e.lon = $lon,
              e.distance_metres = $distance_metres
```

---

### Edge: `KNOWN_ASSOCIATE`

Marks a pre-existing, expected relationship between two entities (e.g. same fleet, same operator). The alert evaluator filters out `KNOWN_ASSOCIATE` pairs when evaluating composite alerts — proximity between known associates is not anomalous.

| Property | Type | Description |
|---|---|---|
| `established_at` | Long | Unix ms when the relationship was recorded |
| `relationship_type` | String | e.g. `'same_fleet'`, `'scheduled_route_pair'` |

In v1, `KNOWN_ASSOCIATE` edges are seeded manually or via import. No service creates them at runtime.

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
| `entity_type` | String | `'aircraft'` or `'vessel'` |
| `last_seen_ms` | String (int) | Unix ms of the last received ping (from source telemetry) |

- **Writer:** Position consumer (HSET on every normalised ping)
- **Readers:** Alert evaluator (scheduled scan for signal loss), API (initial map load, investigation panel, scope filtering)
- **TTL:** `SIGNAL_LOSS_THRESHOLD_MS` — drives dashboard ghost cleanup only. Does not trigger alert detection.

---

### `alert-state:{entity_id}` (string)

In-loop alert suppression flag. Prevents the alert evaluator from re-emitting a signal loss alert on every evaluation cycle while an entity remains dark.

| Value | Description |
|---|---|
| `{dark_since_ms}` | Unix ms when the entity was first detected as dark |

- **Writer:** Alert evaluator (SET on first signal loss emission)
- **Reader:** Alert evaluator (suppression check); composite alert evaluator reads `dark_since_ms` to bound the Neo4j proximity query window (US-06)
- **TTL:** None — key lives indefinitely
- **Deleted by:** Position consumer, on the entity's next successful write (i.e. when it resumes broadcasting)

---

### `deviation-counter:{entity_id}` (string / integer)

Consecutive out-of-baseline ping count for sustained route deviation detection (US-04).

- **Writer:** Alert evaluator (INCR each cycle the entity is outside baseline; DEL when back within baseline or alert emitted)
- **Reader:** Alert evaluator only
- **TTL:** None — managed explicitly by the evaluator

---

### `alert-evaluator:leader` (string)

Leader election lease key. Held by the active alert evaluator instance.

| Value | Description |
|---|---|
| `{instance_id}` | Unique identifier of the holding instance |

- **Pattern:** `SET alert-evaluator:leader {instance_id} NX PX {TTL_MS}`
- **Renewed:** By the active leader on each heartbeat before TTL expires
- **Expires:** Automatically if the leader crashes; follower acquires on next poll

---

### `position-updates` (pub/sub channel)

Broadcast channel for live position events. Every normalised ping is published here by the position consumer. All API instances subscribe and fan out to scoped WebSocket connections.

- **Publisher:** Position consumer (on every normalised ping, after Redis hash write)
- **Subscribers:** All API instances

**Message shape:**
```json
{
  "entity_id":   "ABC123",
  "entity_type": "aircraft",
  "lat":         51.5,
  "lon":         -0.1,
  "altitude":    35000,
  "last_seen_ms": 1700000000000
}
```

---

## Kafka Event Schemas

TypeScript types for these schemas live in a shared internal package imported by the ingestion poller, position consumer, and API. See ADR-013.

---

### `adsb.raw` / `ais.raw`

Raw bytes from the source feed, unparsed. No schema constraint imposed by Sentinel — format fidelity only. The position consumer owns parsing.

---

### `position.normalized`

Published by the position consumer. Consumed by the correlation worker.

```json
{
  "entity_id":    "ABC123",
  "entity_type":  "aircraft",
  "timestamp_ms": 1700000000000,
  "lat":          51.5,
  "lon":          -0.1,
  "altitude":     35000,
  "source":       "adsb",
  "geo_cell":     "8512b08bfffffff"
}
```

| Field | Type | Notes |
|---|---|---|
| `entity_id` | string | ICAO hex (aircraft) or MMSI (vessel) |
| `entity_type` | string | `'aircraft'` or `'vessel'` |
| `timestamp_ms` | number | Unix ms from source telemetry — idempotency key component |
| `lat` | number | Decimal degrees |
| `lon` | number | Decimal degrees |
| `altitude` | number \| null | Metres; null for vessels |
| `source` | string | `'adsb'` \| `'ais'` \| `'synthetic'` |
| `geo_cell` | string | H3 cell ID at resolution 5, computed by position consumer |

---

### `alerts`

Published by the alert evaluator. Consumed by the API.

```json
{
  "alert_id":      "ABC123:SIGNAL_LOSS:1700000000000",
  "entity_id":     "ABC123",
  "entity_type":   "aircraft",
  "alert_type":    "SIGNAL_LOSS",
  "priority":      "STANDARD",
  "detected_at_ms": 1700000000000,
  "payload":       { }
}
```

| Field | Type | Notes |
|---|---|---|
| `alert_id` | string | `{entity_id}:{alert_type}:{window_start_ms}` — deterministic, used for TimescaleDB idempotency |
| `entity_id` | string | |
| `entity_type` | string | `'aircraft'` or `'vessel'` |
| `alert_type` | string | `SIGNAL_LOSS` \| `ROUTE_DEVIATION` \| `UNSCHEDULED_PROXIMITY` \| `COMPOSITE` |
| `priority` | string | `STANDARD` \| `ELEVATED` (COMPOSITE alerts are always ELEVATED) |
| `detected_at_ms` | number | Unix ms |
| `payload` | object | Type-specific fields — see below |

**Payload shapes by alert type:**

```json
// SIGNAL_LOSS
{ "dark_since_ms": 1700000000000, "last_lat": 51.5, "last_lon": -0.1 }

// ROUTE_DEVIATION
{ "current_lat": 51.5, "current_lon": -0.1, "baseline_lat": 51.3, "baseline_lon": -0.2,
  "deviation_metres": 45000, "sustained_cycles": 5 }

// UNSCHEDULED_PROXIMITY
{ "entity_b_id": "DEF456", "lat": 48.8, "lon": 2.3, "distance_metres": 800 }

// COMPOSITE
{ "signal_loss": { "dark_since_ms": 1700000000000 },
  "proximity":   { "entity_a_id": "GHI789", "lat": 48.8, "lon": 2.3, "distance_metres": 800 },
  "correlation_window_ms": 2400000 }
```

---

### `adsb.dlq` / `ais.dlq`

Dead-letter queue for events the position consumer could not parse. See ADR-001.

```json
{
  "raw_payload":       "<base64-encoded original bytes>",
  "rejection_reason":  "missing required field: lat",
  "source_topic":      "adsb.raw",
  "source_offset":     10432,
  "consumer_id":       "position-consumer-0",
  "timestamp_ms":      1700000000000
}
```
