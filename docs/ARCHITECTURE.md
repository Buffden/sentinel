# Architecture

This document defines Sentinel's canonical service boundaries, event flow, persistence ownership, and cross-service contracts.

![Architecture Diagram](../diagrams/docs/architecture.svg)

---

## Core Flow

```text
External feeds
  → Ingestion Poller
  → adsb.raw / ais.raw
  → Position Consumer
      → TimescaleDB position_history
      → Redis entity:live:* + geo-cell:* + position-updates
      → position.normalized
          ├→ Deviation Detector → deviation.candidates
          └→ Correlation Worker → Neo4j evidence → proximity.candidates

Alert Evaluator
  ← Redis scheduled scan (signal loss)
  ← deviation.candidates
  ← proximity.candidates
  ← Redis anomaly episode state
  → alerts

API
  ← alerts
  → TimescaleDB alerts/users/workspaces
  ↔ Redis pub/sub + live state
  → Neo4j investigation reads
  → REST + WebSocket
  → Angular dashboard
```

Kafka and WebSocket transport are at-least-once where replay/reconnection can repeat logical events. Sentinel relies on deterministic identity plus idempotent durable writes for exactly-once **durable effects**, not exactly-once transport.

---

## Ingestion Poller

**Runtime:** Node.js

**Consumes/reads:** OpenSky and AISHub external feeds.

**Publishes:** `adsb.raw`, `ais.raw`.

Contract:

- forwards source telemetry without domain normalization;
- may split provider response envelopes into individual source records;
- performs no anomaly logic and no persistence writes;
- owns polling cadence and provider rate-limit compliance.

---

## Position Consumer

**Runtime:** Node.js

**Consumes:** `adsb.raw`, `ais.raw` (`position-consumer` group).

**Publishes:** `position.normalized`, `adsb.dlq`, `ais.dlq`, Redis `position-updates`.

**Writes:** TimescaleDB `position_history`; Redis `entity:live:{entity_id}`, `geo-cell:{h3_cell_id}`, and resume-related `recent-loss:{entity_id}` state.

Contract:

- parses/normalizes source telemetry;
- preserves source `timestamp_ms`;
- computes `history_geo_cell` and `live_geo_cell` independently;
- writes `position_history` idempotently;
- updates Redis live state only when `incoming.timestamp_ms >= stored.last_seen_ms`;
- updates `entity:live:*` and `geo-cell:*` consistently under the same freshness decision;
- when an accepted ping changes H3 cell, reads the old `live_geo_cell`, `ZREM`s the entity from the old sorted set, then `ZADD`s it to the current cell with score = `last_seen_ms`;
- malformed/unparseable source events go to DLQ; valid source records with no usable position are skipped with an observable metric/log rather than treated as parser failure;
- on entity resume after signal loss, writes `recent-loss:{entity_id}` before deleting `alert-state:{entity_id}`;
- does not evaluate anomaly rules or write Neo4j.

Historical backfill uses a separate consumer group/mode and suppresses live Redis/pub-sub/anomaly side effects unless explicitly rebuilding that state.

---

## Deviation Detector

**Runtime:** Node.js

**Consumes:** `position.normalized` (`deviation-detector` group).

**Reads:** `route_references`, `route_reference_points` in TimescaleDB.

**Publishes:** `deviation.candidates`.

Contract:

- synthetic entities only in v1;
- stateless per ping;
- computes minimum point-to-route-segment distance;
- publishes `OUT_OF_RANGE` or `IN_RANGE` for every eligible ping;
- does not own sustained-deviation episode state;
- does not emit alerts.

---

## Correlation Worker

**Runtime:** Node.js

**Consumes:** `position.normalized` (`correlation-worker` group).

**Reads:** Redis `geo-cell:*`, `entity:live:*`, `proximity-episode:*`; Neo4j `KNOWN_ASSOCIATE` relationships.

**Writes:** Redis `proximity-episode:*`; Neo4j `PROXIMITY_EVENT` evidence.

**Publishes:** `proximity.candidates` for unscheduled pairs only.

Contract:

- uses `LIVE_H3_RESOLUTION` to query nearby fresh entities from Redis sorted sets;
- computes the k-ring radius from `PROXIMITY_THRESHOLD_METRES` and H3 geometry rather than hardcoding it;
- performs exact distance checks after H3 candidate reduction;
- canonicalizes pairs as `min(a,b):max(a,b)`;
- checks `KNOWN_ASSOCIATE` before publishing `proximity.candidates`;
- known-associate proximity may be recorded as graph evidence but is never forwarded as an unscheduled candidate;
- creates one `proximity.candidates` event per unscheduled proximity episode;
- Neo4j write precedes Kafka publication; `candidate_published` enables safe retry after a publish failure;
- does not emit alerts or read signal-loss/composite state.

---

## Alert Evaluator

**Runtime:** Node.js

**Purpose:** Convert qualified anomaly facts plus live anomaly state into canonical alert events.

**Consumes:**

- `deviation.candidates`;
- `proximity.candidates`.

**Reads/writes Redis:**

- scans `entity:live:*` for signal loss;
- owns `alert-state:*`, `deviation-state:*` and leader lease state;
- reads/consumes `recent-loss:*` for composite correlation.

**Reads TimescaleDB:** `position_history` only when constructing last-known signal-loss payload data.

**Publishes:** `alerts`.

**Does not read Neo4j.** The Correlation Worker already filters `KNOWN_ASSOCIATE` pairs before `proximity.candidates` is emitted, and the candidate event carries the immutable episode data needed for the proximity/composite rule.

### Signal loss

A leader-only scheduled Redis scan detects `now - last_seen_ms > SIGNAL_LOSS_THRESHOLD_MS`. `alert-state:{entity_id}` prevents re-emitting the same dark episode.

### Route deviation

The evaluator consumes stateless `OUT_OF_RANGE` / `IN_RANGE` facts and owns `deviation-state:{entity_id}` including replay guard, sustained count, episode start, and `alert_emitted`.

### Proximity / composite

Every `proximity.candidates` message already represents an unscheduled pair.

When one arrives, the evaluator checks both entities for:

- active signal-loss state (`alert-state:*`), or
- recent-loss state (`recent-loss:*`).

If a qualifying loss episode exists within `COMPOSITE_CORRELATION_WINDOW_MS`, emit COMPOSITE with `supersedes_alert_ids`; otherwise emit UNSCHEDULED_PROXIMITY.

One signal-loss episode may produce at most one COMPOSITE.

### Leadership

Only the Redis lease holder joins/polls the `alert-evaluator` Kafka group and runs scheduled signal-loss evaluation. Lease renewal/release use ownership-safe compare-and-expire / compare-before-delete logic.

Leader election reduces concurrent writers; deterministic alert IDs remain the replay/race correctness backstop.

---

## API

**Runtime:** Node.js / Express

**Consumes:** `alerts` (`api` consumer group).

**Writes:** TimescaleDB `alerts`, `users`, `user_workspaces`.

**Reads:** Redis live entity state; TimescaleDB histories/alerts/workspaces; Neo4j relationship evidence for investigation.

**Pub/sub:** subscribes to `position-updates` and `alert-events`; publishes `alert-events` after durable alert/lifecycle writes.

**Auth:** Google OAuth ID-token verification + application JWT.

### Alert consume ordering

```text
1. durable TimescaleDB write / transaction
2. publish alert-events
3. commit Kafka offset
```

If the process crashes before offset commit, Kafka may replay the alert. The durable insert remains idempotent and Redis/WebSocket delivery may repeat. Clients deduplicate alert delivery by `alert_id`.

### Composite supersession

When a COMPOSITE alert arrives, the API transaction:

1. inserts the COMPOSITE idempotently;
2. marks referenced active individual alert(s) `SUPERSEDED` and sets `superseded_by`;
3. commits atomically;
4. publishes lifecycle events to `alert-events`.

`SUPERSEDED` is system-only and may replace an individual alert while it is `NEW` or `ACKNOWLEDGED`.

### Operator lifecycle

Operators may acknowledge and resolve alerts through the API. `RESOLVED` and `SUPERSEDED` are terminal states. A later recurrence creates a new deterministic alert identity for the new anomaly episode rather than reopening history.

---

## Dashboard

**Runtime:** Angular + Leaflet

The dashboard communicates only with the API. It renders live positions, alert feed, lifecycle state, workspace scope, and investigation data.

Repeated WebSocket alert delivery is harmless because the client deduplicates by `alert_id`.

---

## Kafka Topics

| Topic | Producer | Consumer(s) | Purpose |
|---|---|---|---|
| `adsb.raw` | Ingestion Poller | Position Consumer | Raw ADS-B source records |
| `ais.raw` | Ingestion Poller | Position Consumer | Raw AIS source records |
| `adsb.dlq` | Position Consumer | Manual inspection | Unparseable ADS-B records |
| `ais.dlq` | Position Consumer | Manual inspection | Unparseable AIS records |
| `position.normalized` | Position Consumer | Correlation Worker, Deviation Detector | Canonical position facts |
| `deviation.candidates` | Deviation Detector | Alert Evaluator | Stateless route classification facts |
| `proximity.candidates` | Correlation Worker | Alert Evaluator | One unscheduled proximity fact per episode |
| `alerts` | Alert Evaluator | API | Canonical anomaly alerts |

---

## Persistence Ownership

### TimescaleDB

| Object | Writer | Readers |
|---|---|---|
| `position_history` | Position Consumer | Alert Evaluator, API |
| `route_references` / `route_reference_points` | seed/load generator | Deviation Detector |
| `alerts` | API | API |
| `users` | API | API |
| `user_workspaces` | API | API |

TimescaleDB partitions `position_history` by `observed_at` only. `geo_cell` is an indexed query column, not a partition/sharding dimension.

### Neo4j

| Object | Writer | Readers |
|---|---|---|
| `Entity` | Correlation Worker | API |
| `PROXIMITY_EVENT` | Correlation Worker | API |
| `KNOWN_ASSOCIATE` | manual/future import | Correlation Worker, API |

### Redis

| Key/channel | Writer | Reader |
|---|---|---|
| `entity:live:{entity_id}` | Position Consumer | Alert Evaluator, Correlation Worker, API |
| `geo-cell:{h3_cell_id}` | Position Consumer | Correlation Worker |
| `proximity-episode:{pair_key}` | Correlation Worker | Correlation Worker |
| `alert-state:{entity_id}` | Alert Evaluator; deleted by Position Consumer on resume | Alert Evaluator |
| `recent-loss:{entity_id}` | Position Consumer | Alert Evaluator |
| `deviation-state:{entity_id}` | Alert Evaluator | Alert Evaluator |
| `alert-evaluator:leader` | Alert Evaluator | Alert Evaluator |
| `position-updates` | Position Consumer | API instances |
| `alert-events` | API | API instances |

---

## Canonical Deterministic Alert IDs

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

See ADR-007 and `DATA_MODEL.md` for the corresponding schema contracts.
