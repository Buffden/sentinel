# Architecture

This document defines Sentinel's service boundaries, component contracts, persistence ownership, Kafka topics, and delivery semantics. It is authoritative for who reads/writes which store and what each service is allowed to decide.

![Architecture Diagram](../diagrams/docs/architecture.svg)

---

## Architectural Principles

- Services are independently deployable and communicate through Kafka for event flow.
- Shared persistence is intentional where documented; private inter-service HTTP calls are not.
- Kafka processing is at-least-once.
- Durable side effects are made idempotent with deterministic identity and database constraints.
- Redis live state is monotonic by source event time; stale telemetry cannot overwrite newer state.
- Leader election prevents concurrent active Alert Evaluators, but deterministic alert identity remains the durable deduplication backstop.
- WebSocket delivery is at-least-once. Clients must tolerate and deduplicate replayed lifecycle events.

---

## Services

### Ingestion Poller

**Runtime:** Node.js (ADR-013)

**Concern:** Fetch raw ADS-B/AIS telemetry and publish it to Kafka without domain normalization or anomaly logic.

| Direction | Contract |
| --- | --- |
| Reads | OpenSky Network REST API, AISHub |
| Publishes | `adsb.raw`, `ais.raw` |
| Writes stores | None |

The poller may unwrap a provider response envelope and split it into per-entity records. Field coercion, canonical naming, validation, persistence, and DLQ handling belong to the Position Consumer.

---

### Position Consumer

**Runtime:** Node.js

**Concern:** Normalize raw telemetry, persist history, maintain current live state and live spatial index, and publish normalized position events.

| Direction | Contract |
| --- | --- |
| Consumes | `adsb.raw`, `ais.raw` — group `position-consumer` |
| Publishes Kafka | `position.normalized`, `adsb.dlq`, `ais.dlq` |
| Writes TimescaleDB | `position_history` |
| Writes Redis | `entity:live:{entity_id}`, `geo-cell:{live_geo_cell}`, `recent-loss:{entity_id}` |
| Deletes Redis | `alert-state:{entity_id}` after writing `recent-loss` on resume |
| Publishes Redis | `position-updates` |

**Contract:**

- `position_history` is idempotent on `(entity_id, observed_at)` where `observed_at` is deterministically derived from source `timestamp_ms`.
- Redis live state is updated only when the incoming source timestamp is not older than the stored `last_seen_ms`.
- Compute `history_geo_cell` at `HISTORY_H3_RESOLUTION` for historical queries and `live_geo_cell` at `LIVE_H3_RESOLUTION` for the Redis proximity index.
- When an entity changes live H3 cells: read the previous `live_geo_cell`, `ZREM` the entity from the old sorted set, then `ZADD` it to the current set with score=`last_seen_ms`.
- Malformed/unparseable records go to the source-specific DLQ. Valid source records with no position are skipped with observability, not treated as parse failures.
- Historical backfill suppresses ephemeral live side effects and uses a separate consumer group/mode.
- The Position Consumer does not evaluate anomalies and does not write Neo4j.

---

### Correlation Worker

**Runtime:** Node.js

**Concern:** Detect proximity episodes, maintain graph evidence, filter expected relationships, and publish unscheduled-proximity candidates.

| Direction | Contract |
| --- | --- |
| Consumes | `position.normalized` — group `correlation-worker` |
| Reads Redis | `geo-cell:*`, `entity:live:*`, `proximity-episode:{pair_key}` |
| Writes Redis | `proximity-episode:{pair_key}` |
| Reads Neo4j | `KNOWN_ASSOCIATE` for the specific candidate pair |
| Writes Neo4j | `Entity`, `PROXIMITY_EVENT` |
| Publishes | `proximity.candidates` |

**Contract:**

- Candidate search uses the incoming entity's live H3 cell plus a computed k-ring based on `PROXIMITY_THRESHOLD_METRES` and `LIVE_H3_RESOLUTION`.
- `geo-cell:*` members are sorted by `last_seen_ms`; `ZRANGEBYSCORE` filters stale members before exact distance calculation.
- Pair identity is canonical: `pair_key = min(a,b):max(a,b)`.
- One continuous encounter maps to one `proximity-episode:{pair_key}` and one Neo4j `PROXIMITY_EVENT` edge keyed by `{pair_key}:{episode_start_ms}`.
- On a new pair episode, check `KNOWN_ASSOCIATE` before publishing a candidate.
- Known associates may retain graph/episode evidence but never publish `proximity.candidates`.
- Unscheduled pair write order is: Neo4j `MERGE` → create episode with `candidate_published=0` → Kafka publish → set `candidate_published=1`.
- If Kafka publication fails, the next qualifying ping retries because `candidate_published` remains `0`.
- The Correlation Worker does not read alert/composite state and does not decide whether a candidate becomes UNSCHEDULED_PROXIMITY or COMPOSITE.

---

### Deviation Detector

**Runtime:** Node.js

**Concern:** Statelessly compare eligible positions with deterministic reference-route segments and publish deviation classifications.

| Direction | Contract |
| --- | --- |
| Consumes | `position.normalized` — group `deviation-detector` |
| Reads TimescaleDB | `route_references`, `route_reference_points` |
| Publishes | `deviation.candidates` |

**Contract:**

- v1 applies route deviation to synthetic entities with assigned reference routes only.
- For each eligible ping, calculate minimum point-to-segment distance to the reference route.
- Publish `OUT_OF_RANGE` or `IN_RANGE` on every eligible ping.
- The detector is stateless. Sustained-ping counting, episode state, replay guards, and alert emission belong to the Alert Evaluator.

---

### Alert Evaluator

**Runtime:** Node.js

**Concern:** Own anomaly-rule interpretation and publish deterministic alert events.

The Alert Evaluator remains the complete Alert Layer. Removing its direct Neo4j read does **not** remove the service; it removes a redundant dependency because known associates are already filtered by the Correlation Worker.

| Direction | Contract |
| --- | --- |
| Consumes | `deviation.candidates`, `proximity.candidates` — group `alert-evaluator` |
| Reads Redis | `entity:live:*`, `alert-state:*`, `recent-loss:*`, `deviation-state:*`, leader lease |
| Writes Redis | `alert-state:*`, `deviation-state:*`; consumes qualifying `recent-loss:*` |
| Reads TimescaleDB | `position_history` only for last-known signal-loss position payload |
| Publishes | `alerts` |
| Coordination | Redis lease `alert-evaluator:leader` |

**Contract:**

- Only the current lease holder joins/polls the `alert-evaluator` Kafka consumer group.
- Lease renewal and release are ownership-safe compare-and-expire / compare-and-delete operations.
- Signal loss is detected by a scheduled Redis scan because absence of telemetry does not generate a Kafka event.
- Route deviation state lives in `deviation-state:{entity_id}`. Replayed/out-of-order classifications cannot regress or double-increment an episode.
- `proximity.candidates` already means: exact proximity confirmed, new episode, and no `KNOWN_ASSOCIATE` relationship. The evaluator therefore does **not** query Neo4j again.
- When a proximity candidate arrives, inspect `alert-state` / `recent-loss` for both entities:
  - qualifying active/recent signal loss → `COMPOSITE`;
  - otherwise → `UNSCHEDULED_PROXIMITY`.
- A signal-loss episode can produce at most one composite correlation opportunity.
- The evaluator publishes deterministic logical alerts. Kafka may redeliver/replay them; duplicate durable rows are prevented downstream by deterministic `alert_id` and database constraints.

---

### API

**Runtime:** Node.js / Express (ADR-008)

**Concern:** Persist alerts, authenticate operators, expose REST, serve WebSockets, enforce workspace scope, and provide investigation reads.

| Direction | Contract |
| --- | --- |
| Consumes | `alerts` — group `api` |
| Reads Redis | live entity state; subscribes `position-updates`, `alert-events` |
| Publishes Redis | `alert-events` |
| Reads TimescaleDB | positions, alerts, users, workspaces |
| Writes TimescaleDB | alerts, users, workspaces |
| Reads Neo4j | relationship/proximity evidence for investigation |
| Auth | Google OAuth 2.0 ID-token verification + application JWT |

**Alert consume ordering:**

1. Persist the alert transactionally/idempotently in TimescaleDB.
2. Publish the resulting alert lifecycle event to Redis `alert-events`.
3. Commit the Kafka offset.

If the process crashes after the DB write but before offset commit, replay is safe. Redis/WebSocket events may be observed more than once; clients deduplicate.

**Composite supersession:** insertion of a COMPOSITE and supersession of every referenced active individual alert occur in one DB transaction. Active means `NEW` or `ACKNOWLEDGED`; `RESOLVED` is terminal and is not later superseded.

**Phase boundaries:** Phase 03 proves first-instance authenticated alert delivery. Workspace scoping arrives in Phase 07; multi-instance `alert-events` fan-out and full lifecycle transitions arrive in Phase 08. Final-state use-case diagrams may show the completed behavior.

---

### Dashboard

**Runtime:** Angular + Leaflet (ADR-009)

The dashboard communicates only with the API. It renders live positions, alert feed, workspace-scoped data, and investigation evidence. WebSocket clients must tolerate duplicate alert lifecycle events and converge by `alert_id` plus status/version semantics.

---

## Kafka Topics

| Topic | Producer | Consumer | Purpose |
| --- | --- | --- | --- |
| `adsb.raw` | Ingestion Poller | Position Consumer | Raw ADS-B records |
| `ais.raw` | Ingestion Poller | Position Consumer | Raw AIS records |
| `adsb.dlq` | Position Consumer | Manual inspection | Rejected ADS-B records |
| `ais.dlq` | Position Consumer | Manual inspection | Rejected AIS records |
| `position.normalized` | Position Consumer | Correlation Worker, Deviation Detector | Canonical position facts |
| `deviation.candidates` | Deviation Detector | Alert Evaluator | Per-ping route classification |
| `proximity.candidates` | Correlation Worker | Alert Evaluator | New unscheduled proximity episode |
| `alerts` | Alert Evaluator | API | Deterministic logical alerts |

Derived candidate topics have short retention because they are transient rule inputs, not the durable historical source of truth.

---

## Persistence Ownership

### TimescaleDB

| Object | Writer | Readers |
| --- | --- | --- |
| `position_history` | Position Consumer | Alert Evaluator, API |
| `route_references`, `route_reference_points` | Synthetic/manual seed | Deviation Detector |
| `alerts` | API | API |
| `users`, `user_workspaces` | API | API |

TimescaleDB partitions `position_history` by `observed_at` only. `geo_cell` is an indexed query column, not a spatial partition/shard dimension.

### Neo4j

| Object | Writer | Readers |
| --- | --- | --- |
| `Entity` | Correlation Worker | API |
| `PROXIMITY_EVENT` | Correlation Worker | API |
| `KNOWN_ASSOCIATE` | Manual/future import | Correlation Worker, API |

The Alert Evaluator does not read Neo4j in the current v1 contract.

### Redis

| Key / Channel | Writer | Reader | Purpose |
| --- | --- | --- | --- |
| `entity:live:{entity_id}` | Position Consumer | Alert Evaluator, Correlation Worker, API | Latest monotonic live state |
| `geo-cell:{h3_cell_id}` | Position Consumer | Correlation Worker | Live H3 sorted-set candidate index |
| `proximity-episode:{pair_key}` | Correlation Worker | Correlation Worker | Encounter episode/retry state |
| `alert-state:{entity_id}` | Alert Evaluator | Alert Evaluator | Active signal-loss/composite state |
| `recent-loss:{entity_id}` | Position Consumer | Alert Evaluator | Bounded post-resume correlation state |
| `deviation-state:{entity_id}` | Alert Evaluator | Alert Evaluator | Sustained deviation episode state |
| `alert-evaluator:leader` | Alert Evaluator | Alert Evaluator | Ownership-safe lease |
| `position-updates` | Position Consumer | API instances | Live position pub/sub |
| `alert-events` | API | API instances | Alert lifecycle fan-out |

---

## H3 Usage

Sentinel uses two configurable H3 access patterns:

- `HISTORY_H3_RESOLUTION`: `position_history.geo_cell`, an indexed query column inside TimescaleDB time chunks.
- `LIVE_H3_RESOLUTION`: Redis `geo-cell:*` sorted sets used to reduce live proximity candidates.

TimescaleDB partitions `position_history` by `observed_at` only. H3 cells are not TimescaleDB chunks or shards in this design.

---

## Canonical Data Flow

```text
External feeds
  → Ingestion Poller
  → adsb.raw / ais.raw
  → Position Consumer
      → TimescaleDB position_history
      → Redis entity:live + geo-cell sorted sets
      → Redis position-updates
      → position.normalized
          ├→ Deviation Detector
          │    → deviation.candidates
          └→ Correlation Worker
               → Redis live/H3 lookup
               → Neo4j relationship evidence + KNOWN_ASSOCIATE check
               → proximity.candidates (unscheduled pairs only)

Redis entity:live scan ─────────────────────────┐
deviation.candidates ───────────────────────────┤
proximity.candidates ───────────────────────────┤
Redis alert-state / recent-loss / deviation-state ─→ Alert Evaluator
                                                     → alerts
                                                     → API
                                                         → TimescaleDB alerts
                                                         → Redis alert-events
                                                         → REST/WebSocket
                                                         → Angular dashboard

API → Neo4j only for operator investigation/evidence reads.
```

---

## Delivery Guarantees

| Boundary | Guarantee |
| --- | --- |
| Kafka consumption | At-least-once |
| TimescaleDB position write | Idempotent / exactly-once effect |
| Neo4j proximity evidence | Idempotent `MERGE` by episode identity |
| Redis live state | Monotonic by source event time |
| Alert durable persistence | Idempotent / exactly-once effect by deterministic `alert_id` |
| WebSocket lifecycle delivery | At-least-once; client deduplication required |
| Alert Evaluator leadership | Single active evaluator under normal lease ownership; durable idempotency remains the correctness backstop |

---

## ADR Index

See `docs/adr/ADR-001` through `ADR-015`. In particular:

- ADR-005 — Alert Evaluator leader election
- ADR-006 — H3 geo-cell indexing strategy
- ADR-007 — deterministic idempotency identities
- ADR-010 — durable alert lifecycle store
- ADR-014 — hybrid Alert Evaluator input model
- ADR-015 — v1 deterministic reference routes
