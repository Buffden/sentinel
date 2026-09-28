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
| Reads | ADS-B providers (adsb.fi as the regional primary, OpenSky as the fallback, per ADR-020), AISHub; the coordinator also reads its own `{live-provider}` keys at lease acquisition |
| Publishes | `adsb.raw`, `ais.raw` |
| Writes Redis | `{live-provider}:lease`, `{live-provider}:authority`, `{live-provider}:coverage`, `{live-provider}:health:adsbfi`, `{live-provider}:health:opensky` (ingestion coordinator only) |
| Coordination | Redis lease `{live-provider}:lease` (ingestion coordinator only) |

Only one ADS-B provider is authoritative at a time. The production ADS-B entry point is the ingestion coordinator (`npm run coordinate`), which owns both provider adapters and automatic failover (ADR-022). The old lease-free standalone OpenSky and adsb.fi publishers have been removed.

The ingestion coordinator runs both the adsb.fi and OpenSky adapters, and only while it holds the Redis lease `{live-provider}:lease`. It renews the lease every 5 s, and each renewal also writes `heartbeat_ms` to `{live-provider}:authority`. A second coordinator waits as a follower and takes over once the leader's lease expires.

The coordinator also keeps the live-provider authority record and coverage timeline (ADR-022 sections 5 to 7). A failover candidate publishes first, then COMMIT changes authority and increments the epoch while keeping coverage closed. Voluntary OpenSky → adsb.fi failback uses the same publication-before-Redis ordering, but HANDOVER changes authority only from the exact expected OpenSky term after its coverage was closed for the attempt. CREDIT is a separate lease-checked write that opens or extends coverage only when the delivered cycle qualifies. A failed active cycle closes the open segment at the last credited success. A new lease holder closes any segment its predecessor left open before polling. The Alert Evaluator reads the timeline to measure signal-loss silence (see Alert Evaluator below).

The coordinator also keeps each provider's health (ADR-022 section 2) in `{live-provider}:health:adsbfi` and `{live-provider}:health:opensky`, moving through `HEALTHY`, `DEGRADED`, `UNAVAILABLE` and `RECOVERING`. Health is judged only from requests to the provider: each request's outcome is recorded right after the request and its validation, before anything is published, so a Kafka failure never counts against a provider. `DEGRADED` becomes `UNAVAILABLE` on a timer 60 s after entry, whether or not a request is finishing. The coordinator checks OpenSky on standby at ADR-022's rates (15 min when `HEALTHY`) without ever publishing its data, and honours an OpenSky `429` retry time as a pause. Health is restored at every lease acquisition, before any request.

Three things are kept apart: **provider health** describes the upstream provider, **coverage** describes successful authoritative delivery, and **authority** decides who may publish. When the authoritative provider becomes `UNAVAILABLE`, the coordinator relinquishes to `none` and selects an eligible provider. While OpenSky is authoritative, adsb.fi stays on standby; only after adsb.fi is `HEALTHY` and OpenSky has held authority for at least five minutes does the coordinator finish the current OpenSky cycle, close its coverage as `handover_attempt`, and try one adsb.fi handover cycle. A failed attempt leaves OpenSky authoritative and resumes it.

The lease is a duplicate-instance guard, not fencing. Kafka never checks it, so a coordinator paused past its lease can still complete a send after a successor has taken over (ADR-022 section 8). All production ADS-B publishing now goes through the coordinator; there is no lease-free standalone publisher.

The poller may unwrap a provider response envelope, split it into per-entity records, and drop records outside the monitored area or outside the canonical identity model. It wraps each `adsb.raw` record as `{ provider, payload }` (ADR-021) and keys it by lowercase ICAO24, so one aircraft's records share a partition whichever provider sent them. It may add documented context the record needs once split from its response, such as a response time. Field coercion, canonical naming, validation, persistence, and DLQ handling belong to the Position Consumer.

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
- Classify each `adsb.raw` record by provider before normalizing it, and normalize with that provider's mapping only (ADR-021). There is no default provider: a record that cannot be identified is archived with a null provider and sent to the DLQ.
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
| Reads Redis | `entity:live:*`, `alert-state:*`, `recent-loss:*`, `deviation-state:*`, `{live-provider}:authority`, `{live-provider}:coverage`, leader lease |
| Writes Redis | `alert-state:*`, `deviation-state:*`; consumes qualifying `recent-loss:*` |
| Reads TimescaleDB | None. The signal-loss scan takes `last_seen_ms`, `provider`, `on_ground`, callsign and last-known position from Redis `entity:live:{entity_id}` |
| Publishes | `alerts` |
| Coordination | Redis lease `alert-evaluator:leader` |

**Contract:**

- Only the current lease holder joins/polls the `alert-evaluator` Kafka consumer group.
- Lease renewal and release are ownership-safe compare-and-expire / compare-and-delete operations.
- Signal loss is detected by a scheduled Redis scan because absence of telemetry does not generate a Kafka event.
- Each scan first reads `{live-provider}:authority` and `{live-provider}:coverage` once, in one `MULTI`/`EXEC`, and judges every entity against that snapshot with one scan time (ADR-022 section 5). An entity's silence is its **observed silence**: the coverage of the provider in its `provider` field since `last_seen_ms`. `SIGNAL_LOSS` fires only when that reaches the threshold, so provider outages and pipeline downtime add nothing. A missing or uncovered provider, an uninitialized timeline, or an unreadable timeline means no signal-loss alerts; there is no wall-clock fallback. An unreadable timeline ends only that scan: proximity and composite run in the candidate consumer and are unaffected.
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

**Runtime:** Next.js (CSR) + Blueprint.js (ADR-016); MapLibre GL + deck.gl map engine (ADR-017); Dockview workspace layout (ADR-018). Supersedes ADR-009.

The dashboard is a registry-driven dockable workspace. It communicates only with the API. Widgets render live positions, alert feed, multi-tenant workspace-scoped data, and investigation evidence. The map is a Dockview widget running MapLibre GL with deck.gl layers; aviation is the first layer implementation. WebSocket clients must tolerate duplicate alert lifecycle events and converge by `alert_id` plus status/version semantics.

---

## Kafka Topics

| Topic | Producer | Consumer | Purpose |
| --- | --- | --- | --- |
| `adsb.raw` | Ingestion Poller | Position Consumer | Raw ADS-B records from any ADS-B provider, in a `{ provider, payload }` envelope |
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
| `position_history` | Position Consumer | API |
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
| `entity:live:{entity_id}` | Position Consumer | Alert Evaluator, Correlation Worker, API | Latest monotonic live state: `lat`, `lon`, `altitude_m`, `entity_type`, `last_seen_ms`, `live_geo_cell`, `speed_mps`, `course_deg`, `heading_deg`, `vertical_rate_mps`, `on_ground`, `navigation_status`, `callsign`, `entity_subtype`, `provider` |
| `geo-cell:{h3_cell_id}` | Position Consumer | Correlation Worker | Live H3 sorted-set candidate index |
| `proximity-episode:{pair_key}` | Correlation Worker | Correlation Worker | Encounter episode/retry state |
| `alert-state:{entity_id}` | Alert Evaluator | Alert Evaluator | Active signal-loss/composite state |
| `recent-loss:{entity_id}` | Position Consumer | Alert Evaluator | Bounded post-resume correlation state |
| `deviation-state:{entity_id}` | Alert Evaluator | Alert Evaluator | Sustained deviation episode state |
| `alert-evaluator:leader` | Alert Evaluator | Alert Evaluator | Ownership-safe lease |
| `{live-provider}:lease` | Ingestion coordinator | Ingestion coordinator | Duplicate-instance guard for the coordinator, not fencing |
| `{live-provider}:authority` | Ingestion coordinator | Alert Evaluator | Authoritative provider, epoch, open coverage segment, `timeline_version`, and the lease heartbeat |
| `{live-provider}:coverage` | Ingestion coordinator | Alert Evaluator | Closed provider coverage segments with positive length, scored by end time |
| `{live-provider}:health:adsbfi`, `{live-provider}:health:opensky` | Ingestion coordinator | Ingestion coordinator (restore at acquisition), operators | Provider health state and request evidence. Not used for signal loss |
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
                                                         → Next.js dashboard

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
| Ingestion coordinator leadership | Single polling coordinator under normal lease ownership. Not fencing: a coordinator paused past its lease can still complete an in-flight Kafka send |

---

## ADR Index

See `docs/adr/ADR-001` through `ADR-018`. In particular:

- ADR-005 — Alert Evaluator leader election
- ADR-006 — H3 geo-cell indexing strategy
- ADR-007 — deterministic idempotency identities
- ADR-010 — durable alert lifecycle store
- ADR-014 — hybrid Alert Evaluator input model
- ADR-015 — v1 deterministic reference routes
- ADR-016 — Next.js + Blueprint.js dashboard (supersedes ADR-009)
- ADR-017 — MapLibre GL + deck.gl map engine (supersedes react-leaflet portion of ADR-016)
- ADR-018 — Dockview dockable workspace (supersedes fixed shell in ADR-016)
