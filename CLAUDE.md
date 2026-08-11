# CLAUDE.md — Sentinel

Read this before implementation work. Sentinel is a real-time geospatial entity-tracking and rule-based anomaly-correlation portfolio project. The objective is interview-defensible distributed-systems engineering: correctness, explicit ownership, failure reasoning, replay safety, and hands-on understanding over feature count or visual polish.

---

## Fixed Stack

Do not change without an ADR.

| Concern | Technology |
| --- | --- |
| Broker | Kafka semantics; Redpanda locally, MSK on AWS |
| Position history | TimescaleDB |
| Entity graph | Neo4j |
| Live state / leases / pub-sub | Redis |
| Backend runtime | Node.js / TypeScript |
| API | Express + WebSocket |
| Dashboard | Angular + Leaflet |
| Auth | Google OAuth 2.0 + application JWT |
| Deployment | Docker Compose → AWS |

Do not introduce new infrastructure silently.

---

## Source-of-Truth Order

Before coding:

1. `README.md`
2. relevant `docs/adr/`
3. `docs/ARCHITECTURE.md`
4. `docs/DATA_MODEL.md`
5. relevant `docs/use-cases/`
6. `docs/IMPLEMENTATION_PLAYBOOK.md`
7. relevant `docs/implementation/phase-XX-*.md`
8. existing code/tests/configuration

`docs/implementation/` is permanent and intentional. Keep phase files aligned with accepted architecture changes.

---

## Canonical Service Ownership

### Position Consumer

Raw telemetry → normalized position history + monotonic Redis live state + Redis H3 sorted-set membership + `position.normalized`.

### Deviation Detector

Stateless reference-route geometry → `deviation.candidates`.

### Correlation Worker

Redis H3 candidate lookup → exact distance → Neo4j proximity evidence + `KNOWN_ASSOCIATE` check → `proximity.candidates` for unscheduled pairs only.

### Alert Evaluator

Remains the full Alert Layer. It owns final rule interpretation:

- signal loss from scheduled Redis scan;
- sustained route deviation from `deviation.candidates` + Redis episode state;
- proximity/composite choice from `proximity.candidates` + Redis `alert-state` / `recent-loss`.

The Alert Evaluator does **not** read Neo4j in the current v1 contract. Known-associate filtering happens upstream in the Correlation Worker.

### API

Consumes `alerts`, owns durable alert lifecycle state, auth/workspace enforcement, Redis pub/sub fan-out, REST/WebSocket serving, and Neo4j investigation reads.

---

## Delivery and Idempotency Rules

Never describe Sentinel as an exactly-once transport pipeline.

- Kafka: at-least-once processing.
- Position history: idempotent durable effect through `(entity_id, observed_at)`.
- Redis live state: monotonic by source event time; stale telemetry cannot overwrite newer state.
- Neo4j proximity evidence: idempotent `MERGE` by canonical pair episode identity.
- Alerts: deterministic type-specific `alert_id`; API DB persistence gives an idempotent exactly-once durable effect.
- WebSocket lifecycle delivery: at-least-once; clients tolerate duplicates.
- Leader election: prevents concurrent active Alert Evaluators; deterministic durable idempotency remains the correctness backstop.

Canonical pair key:

```text
min(entity_a_id, entity_b_id):max(entity_a_id, entity_b_id)
```

Canonical alert IDs are defined in `docs/DATA_MODEL.md`; do not invent alternatives.

---

## Event Time

Episode anchors, correlation windows, replay guards, and deterministic identities use source event time. Processing time is only for operational/audit timestamps where the data model explicitly permits it.

---

## Replay Modes

### Crash recovery

Resume from committed offsets and execute normal processing with idempotent protections.

### Historical backfill

Use a separate group/mode and suppress ephemeral live side effects unless they are the explicit rebuild target. Do not replay old telemetry through the live alert path or overwrite current Redis state.

---

## H3 Mental Model

- TimescaleDB partitions `position_history` by time only.
- `history_geo_cell` is an indexed query column inside time chunks.
- Redis `geo-cell:{live_geo_cell}` sorted sets are the live spatial candidate index.
- On live cell movement: remove from old sorted set, add to new sorted set.
- H3 reduces candidate sets; exact geographic distance decides proximity.

Do not describe H3 cells as TimescaleDB chunks/shards in this v1 schema.

---

## Alert Lifecycle

Operator path:

```text
NEW → ACKNOWLEDGED → RESOLVED
NEW → RESOLVED
```

System composite path:

```text
NEW → SUPERSEDED
ACKNOWLEDGED → SUPERSEDED
```

`RESOLVED` and `SUPERSEDED` are terminal. Recurrence creates a new alert; it does not reopen a resolved row.

---

## Implementation Style

This is a learning/pair-engineering repository. During implementation:

- explain the mental model before framework mechanics;
- work in small observable checkpoints;
- let the developer interact with Kafka, TimescaleDB, Redis, Neo4j, H3, and WebSockets directly;
- test failure boundaries, not just happy-path syntax;
- prefer explicit code over clever abstractions;
- comment why, not what;
- name constants with units;
- do not implement later phases opportunistically.

After meaningful checkpoints, explain the data flow, guarantee being demonstrated, trade-off, relevant failure, how to inspect it manually, and the next smallest checkpoint.

---

## Hard Constraints

- No ML anomaly detection in v1.
- No private inter-service HTTP coupling; Kafka/shared stores only where contracts explicitly allow it.
- No tech-stack change without ADR.
- No alternative auth providers or unnecessary account-management features.
- No elaborate RBAC beyond accepted scope.
- No speculative infrastructure or abstraction layers.
- No defense/intelligence-flavored naming or framing.
- Do not over-invest in dashboard polish.

If implementation evidence shows an accepted assumption is wrong, do not silently code around it. Surface the evidence, update the relevant architectural contract/ADR, then change code and docs together.
