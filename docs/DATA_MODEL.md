# Data Model

Canonical schemas for Sentinel's persistent stores, Redis state, and Kafka contracts. Field names and identity rules here are authoritative for implementation.

---

## Global Time and Replay Rules

- Source telemetry time is carried as `timestamp_ms`.
- Episode anchors, replay guards, correlation windows, and deterministic identities use source event time.
- Operational audit timestamps such as database `created_at` / `updated_at` may use processing time.
- Kafka processing is at-least-once. Durable writes must therefore be idempotent.
- Historical backfill uses a separate consumer group/mode and suppresses ephemeral live side effects.

---

## TimescaleDB

### `position_history` — hypertable

Partitioned on `observed_at` only. Default chunk interval: 1 day. Default retention: 30 days.

| Column | Type | Nullable | Description |
| --- | --- | --- | --- |
| `entity_id` | TEXT | No | ICAO hex, MMSI, or synthetic entity ID |
| `entity_type` | TEXT | No | `aircraft` or `vessel` |
| `observed_at` | TIMESTAMPTZ | No | `to_timestamp(timestamp_ms / 1000.0)`; hypertable time column |
| `timestamp_ms` | BIGINT | No | Source event time in Unix ms |
| `geo_cell` | TEXT | No | H3 cell at `HISTORY_H3_RESOLUTION`; indexed query column, not partition dimension |
| `lat` | DOUBLE PRECISION | No | Decimal degrees |
| `lon` | DOUBLE PRECISION | No | Decimal degrees |
| `altitude` | REAL | Yes | Metres; null for vessels |
| `source` | TEXT | No | `adsb`, `ais`, or `synthetic` |

Constraints/indexes:

- unique `(entity_id, observed_at)`; duplicates use `ON CONFLICT DO NOTHING`;
- `(entity_id, observed_at DESC)`;
- `(geo_cell, observed_at DESC)`.

TimescaleDB does **not** create separate chunks by `geo_cell`. H3 narrows rows inside time chunks through the index.

### `route_references`

| Column | Type | Nullable |
| --- | --- | --- |
| `route_id` | TEXT PK | No |
| `entity_id` | TEXT | No |
| `route_name` | TEXT | No |
| `corridor_threshold_metres` | REAL | No |
| `source` | TEXT | No |
| `created_at` | TIMESTAMPTZ | No |

Index `entity_id`.

### `route_reference_points`

| Column | Type | Nullable |
| --- | --- | --- |
| `route_id` | TEXT | No |
| `sequence_no` | INTEGER | No |
| `lat` | DOUBLE PRECISION | No |
| `lon` | DOUBLE PRECISION | No |

Primary key `(route_id, sequence_no)`. Adjacent points define route segments. Route deviation uses minimum point-to-segment distance.

### `alerts`

Plain PostgreSQL table on the TimescaleDB instance; not a hypertable.

| Column | Type | Nullable | Description |
| --- | --- | --- | --- |
| `alert_id` | TEXT PK | No | Deterministic logical alert identity; rules below |
| `entity_id` | TEXT | No | Primary entity |
| `counterparty_entity_id` | TEXT | Yes | Second entity for proximity/composite alerts |
| `entity_type` | TEXT | No | `aircraft` or `vessel` |
| `alert_type` | TEXT | No | `SIGNAL_LOSS`, `ROUTE_DEVIATION`, `UNSCHEDULED_PROXIMITY`, `COMPOSITE` |
| `priority` | TEXT | No | `STANDARD` or `ELEVATED` |
| `status` | TEXT | No | `NEW`, `ACKNOWLEDGED`, `RESOLVED`, `SUPERSEDED` |
| `superseded_by` | TEXT | Yes | FK to composite `alert_id` when system-superseded |
| `payload` | JSONB | No | Type-specific evidence |
| `detected_at` | TIMESTAMPTZ | No | Persistence/detection processing timestamp |
| `updated_at` | TIMESTAMPTZ | No | Last lifecycle update |
| `acknowledged_at` | TIMESTAMPTZ | Yes | Operator acknowledgement time |
| `acknowledged_by` | UUID | Yes | FK to user |
| `resolved_at` | TIMESTAMPTZ | Yes | Resolution time |
| `resolved_by` | UUID | Yes | FK to user |

Canonical deterministic `alert_id` rules:

```text
SIGNAL_LOSS
{entity_id}:SIGNAL_LOSS:{dark_since_ms}

ROUTE_DEVIATION
{entity_id}:ROUTE_DEVIATION:{episode_start_ms}

UNSCHEDULED_PROXIMITY
{pair_key}:UNSCHEDULED_PROXIMITY:{episode_start_ms}

COMPOSITE
{pair_key}:COMPOSITE:{dark_since_ms}
```

`pair_key = min(entity_a_id, entity_b_id):max(entity_a_id, entity_b_id)`.

These type-specific identities avoid collisions between two simultaneous pair incidents involving the same primary entity.

Lifecycle:

- operator path: `NEW → ACKNOWLEDGED → RESOLVED`, with optional `NEW → RESOLVED`;
- system composite replacement: `NEW → SUPERSEDED` or `ACKNOWLEDGED → SUPERSEDED`;
- `RESOLVED` and `SUPERSEDED` are terminal;
- a recurring anomaly creates a new deterministic episode/window identity rather than reopening a terminal row.

When a COMPOSITE is persisted, the API atomically inserts it and marks referenced active individual alerts (`NEW` or `ACKNOWLEDGED`) `SUPERSEDED`. A resolved alert is not retroactively superseded.

Indexes:

- `(entity_id, detected_at DESC)`;
- `(counterparty_entity_id, detected_at DESC)`;
- `(status, detected_at DESC)`;
- `(alert_type, detected_at DESC)`.

### `users`

| Column | Type | Nullable |
| --- | --- | --- |
| `user_id` | UUID PK | No |
| `google_sub` | TEXT UNIQUE | No |
| `email` | TEXT | No |
| `last_login_at` | TIMESTAMPTZ | No |
| `created_at` | TIMESTAMPTZ | No |

### `user_workspaces`

| Column | Type | Nullable |
| --- | --- | --- |
| `user_id` | UUID PK/FK | No |
| `scope` | JSONB | No |
| `updated_at` | TIMESTAMPTZ | No |

Canonical scope contains geographic bounds plus `entity_types` and `alert_types` filters.

---

## Neo4j

### Node `Entity`

Properties: `id` (unique), `type`, optional `name`.

### Edge `PROXIMITY_EVENT`

One edge per proximity episode.

| Property | Description |
| --- | --- |
| `idempotency_key` | `{pair_key}:{episode_start_ms}` |
| `episode_start_ms` | Source event time for encounter start |
| `last_seen_ms` | Latest source event time confirming encounter |
| `min_distance_metres` | Closest observed distance |
| `lat`, `lon` | Midpoint at detection |
| `distance_at_detection` | Distance at episode start |

Correlation Worker writes with `MERGE` on `idempotency_key`.

### Edge `KNOWN_ASSOCIATE`

Represents a pre-existing expected relationship such as same fleet or scheduled pairing.

The **Correlation Worker** checks this edge before publishing `proximity.candidates`. Known-associate encounters may be recorded as evidence, but they do not become unscheduled-proximity candidates.

Properties include `established_at` and `relationship_type`. v1 seeds these manually; no runtime service creates them.

The Alert Evaluator does not read Neo4j in the current v1 contract.

---

## Redis

### `entity:live:{entity_id}` — hash

Fields: `lat`, `lon`, optional `altitude`, `entity_type`, `last_seen_ms`, `live_geo_cell`.

- Writer: Position Consumer.
- Readers: Alert Evaluator, Correlation Worker, API.
- TTL: 24h safety net, deliberately longer than signal-loss timing.
- Timestamp guard: stale/out-of-order telemetry cannot replace a newer `last_seen_ms` state.

### `geo-cell:{h3_cell_id}` — sorted set

Member=`entity_id`, score=`last_seen_ms`.

Position Consumer update sequence:

1. read previous `live_geo_cell` before replacing the live hash;
2. if the cell changed, `ZREM geo-cell:{old_cell} {entity_id}`;
3. `ZADD geo-cell:{new_cell} {last_seen_ms} {entity_id}`.

Correlation Worker reads the incoming entity cell plus computed k-ring using `ZRANGEBYSCORE` with a freshness lower bound. No TTL is required; stale members age out logically by score.

### `alert-state:{entity_id}` — hash

Fields:

- `dark_since_ms`;
- `signal_loss_alert_id`;
- `composite_issued` (`0|1`).

Writer/reader: Alert Evaluator only. No TTL. Position Consumer deletes it when the entity resumes, after writing `recent-loss`.

### `recent-loss:{entity_id}` — hash

Fields: `dark_since_ms`, `resumed_at_ms`, `signal_loss_alert_id`.

- Writer: Position Consumer on first accepted resume position.
- Reader/consumer: Alert Evaluator.
- TTL: `COMPOSITE_CORRELATION_WINDOW_MS`.

A qualifying composite consumes/deletes the key.

### `deviation-state:{entity_id}` — hash

Fields: `count`, `episode_start_ms`, `last_processed_ms`, `alert_emitted`.

Writer/reader: Alert Evaluator. Safety TTL `DEVIATION_STATE_TTL_MS`; explicit delete on `IN_RANGE`.

### `proximity-episode:{pair_key}` — hash

Fields: `episode_start_ms`, `last_seen_ms`, optional `candidate_published`.

Writer/reader: Correlation Worker. TTL `PROXIMITY_EPISODE_GAP_MS`.

- unscheduled pair: `candidate_published=0` before Kafka publish and `1` after success;
- known associate: field omitted because no candidate is ever published.

### `alert-evaluator:leader` — string

Value=`instance_id`.

Acquire: `SET NX PX`. Renewal/release must compare current ownership before `PEXPIRE` / `DEL`.

### `position-updates` — pub/sub

Publisher: Position Consumer. Subscribers: API instances.

### `alert-events` — pub/sub

Publisher: API. Subscribers: all API instances.

Canonical envelope:

```json
{
  "type": "ALERT_CREATED | ALERT_STATUS_CHANGED | ALERT_SUPERSEDED",
  "payload": {}
}
```

Redis pub/sub and WebSocket delivery are at-least-once from the client's perspective; duplicate lifecycle messages are allowed and must be safe.

---

## Kafka Event Schemas

### `adsb.raw` / `ais.raw`

Provider-fidelity records. Position Consumer owns parsing/normalization.

### `position.normalized`

| Field | Type |
| --- | --- |
| `entity_id` | string |
| `entity_type` | string |
| `timestamp_ms` | number |
| `lat` | number |
| `lon` | number |
| `altitude` | number \| null |
| `source` | string |
| `history_geo_cell` | string |
| `live_geo_cell` | string |

Published by Position Consumer; consumed by Correlation Worker and Deviation Detector.

### `deviation.candidates`

Published on every eligible synthetic-entity ping by the stateless Deviation Detector.

Fields: `entity_id`, `timestamp_ms`, `status` (`OUT_OF_RANGE|IN_RANGE`), `current_position`, optional `nearest_segment_index`, optional `deviation_metres`.

### `proximity.candidates`

Published once per **new unscheduled** proximity episode. Known associates are already filtered upstream.

Fields:

- `pair_key`;
- `entity_a_id` (lexicographically smaller);
- `entity_b_id`;
- `episode_start_ms`;
- `lat`, `lon` midpoint;
- `distance_at_detection`.

This event already contains everything required to trigger proximity/composite evaluation; the Alert Evaluator does not perform a second Neo4j known-associate check.

### `alerts`

Published by Alert Evaluator; consumed by API.

| Field | Type |
| --- | --- |
| `alert_id` | string |
| `entity_id` | string |
| `counterparty_entity_id` | string \| null |
| `entity_type` | string |
| `alert_type` | string |
| `priority` | string |
| `detected_at_ms` | number |
| `payload` | object |

Payload evidence:

- SIGNAL_LOSS: `dark_since_ms`, last known location;
- ROUTE_DEVIATION: route segment/deviation data and sustained count;
- UNSCHEDULED_PROXIMITY: `pair_key`, counterparty, location, distance, `episode_start_ms`;
- COMPOSITE: nested signal-loss + proximity evidence, correlation window, `supersedes_alert_ids`.

### `adsb.dlq` / `ais.dlq`

Fields: `raw_payload`, `rejection_reason`, `source_topic`, `source_offset`, `consumer_id`, operational `timestamp_ms`.

---

## Canonical Idempotency Identities

Do not use one universal key shape for every write. Use the identity of the logical fact being stored:

| Fact | Identity |
| --- | --- |
| Position history | `(entity_id, observed_at)` derived from source `timestamp_ms` |
| Redis live state | `entity_id` plus monotonic `last_seen_ms` guard |
| Proximity episode / Neo4j evidence | `{pair_key}:{episode_start_ms}` |
| Signal-loss alert | `{entity_id}:SIGNAL_LOSS:{dark_since_ms}` |
| Route-deviation alert | `{entity_id}:ROUTE_DEVIATION:{episode_start_ms}` |
| Unscheduled-proximity alert | `{pair_key}:UNSCHEDULED_PROXIMITY:{episode_start_ms}` |
| Composite alert | `{pair_key}:COMPOSITE:{dark_since_ms}` |

These identities provide deterministic replay behavior without claiming exactly-once transport.
