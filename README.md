# Sentinel

A real-time geospatial entity-tracking and rule-based anomaly-correlation platform. Sentinel ingests ADS-B/AIS positional telemetry, maintains current and historical entity state, detects rule-based anomalies, correlates weak signals, and surfaces operator-visible alerts on a Next.js + Blueprint.js + MapLibre GL workspace.

Sentinel is built to production distributed-systems reasoning standards: streaming ingestion with replay safety, idempotent polyglot persistence, leader election, geospatial indexing, stateful multi-signal correlation, and observable failure behavior at every boundary.

---

## Architecture Overview

![Architecture](diagrams/docs/architecture.svg)

---

## Core Technical Decisions

| Decision | Choice | Why |
| --- | --- | --- |
| Streaming buffer | Kafka semantics; Redpanda local / MSK AWS | decoupling, buffering, replay |
| Position history | TimescaleDB | time-partitioned telemetry with indexed geo filtering |
| Entity graph | Neo4j | relationship/proximity evidence and graph investigation |
| Live state | Redis | current entity state, H3 live candidate index, leases, pub/sub |
| Alert coordination | Redis leader lease | one active Alert Evaluator under normal operation |
| Alert durability | Plain PostgreSQL table on TimescaleDB | transactional lifecycle and replay-safe dedup |
| API | Express + WebSocket | async event serving with lightweight REST/WS layer |
| Dashboard | Next.js + Blueprint.js + MapLibre GL + deck.gl + Dockview | registry-driven dockable workspace; WebGL map engine |

---

## Anomaly Detection

| Alert | Trigger | Notes |
| --- | --- | --- |
| `SIGNAL_LOSS` | Entity unseen beyond a configurable silence threshold | Detected by scheduled Redis scan, not a Kafka event |
| `ROUTE_DEVIATION` | Entity remains outside its assigned route corridor for sustained pings | Stateless per-ping geometry; episode state owned by Alert Evaluator |
| `UNSCHEDULED_PROXIMITY` | Exact proximity episode between a pair with no `KNOWN_ASSOCIATE` relationship | Known associates are filtered upstream in the Correlation Worker |
| `COMPOSITE` | Active or recent signal-loss episode correlated with an unscheduled proximity | Supersedes the individual alerts it references |

---

## Data Sources

- OpenSky Network — public ADS-B source subject to its terms/rate limits.
- AISHub — AIS aggregator; eligibility/access must be verified.
- Synthetic load generator — deterministic anomaly injection for demos and tests.

Third-party source terms and attribution requirements must be respected; see `NOTICE`.

---

## Service Pipeline

| Service | Role | Consumes | Publishes |
| --- | --- | --- | --- |
| Ingestion Poller | Fetch raw ADS-B/AIS telemetry from external feeds | OpenSky / AISHub REST | `adsb.raw`, `ais.raw` |
| Position Consumer | Normalize telemetry, persist history, maintain live Redis state and H3 spatial index | `adsb.raw`, `ais.raw` | `position.normalized` |
| Deviation Detector | Stateless per-ping geometry check against reference route corridors | `position.normalized` | `deviation.candidates` |
| Correlation Worker | Detect proximity episodes, write graph evidence, filter known associates | `position.normalized` | `proximity.candidates` |
| Alert Evaluator | Apply anomaly rules and emit deterministic alerts; owns signal-loss via scheduled Redis scan | `deviation.candidates`, `proximity.candidates` | `alerts` |
| API | Persist and lifecycle alerts, authenticate operators, serve REST and WebSocket | `alerts`, Redis pub/sub | REST + WebSocket |

---

## Architecture Decision Records

ADR-001 through ADR-015 live in `docs/adr/`.

| ADR | Decision | Choice | Key Reasoning |
| --- | --- | --- | --- |
| ADR-001 | Broker over direct HTTP ingestion | Redpanda (Kafka-compatible) | Decouples producers from consumers; enables replay, backpressure, and at-least-once delivery |
| ADR-002 | Position history store | TimescaleDB | Time-partitioned hypertable with indexed geo-cell column; automatic chunk retention |
| ADR-003 | Entity graph store | Neo4j | Native relationship traversal for proximity evidence and known-associate filtering |
| ADR-004 | Live state store | Redis | Sub-millisecond current state, H3 sorted-set spatial index, pub/sub, and lease primitives in one store |
| ADR-005 | Alert Evaluator coordination | Redis lease | Single active evaluator without distributed consensus overhead; durable idempotency remains the correctness backstop |
| ADR-006 | Geospatial candidate indexing | Uber H3 hex cells | Two resolutions: history column in TimescaleDB, live sorted sets in Redis; reduces proximity candidates before exact distance |
| ADR-007 | Idempotency strategy | Deterministic logical IDs | Replay safety without exactly-once transport; type-specific alert IDs and `(entity_id, observed_at)` position key |
| ADR-008 | API layer | Express + WebSocket | Lightweight async runtime; REST for queries, WebSocket for live alert and position fan-out |
| ADR-009 | Operator dashboard | Angular + Leaflet (superseded by ADR-016) | Functional map interface; WebSocket client tolerates duplicate lifecycle events |
| ADR-016 | Operator dashboard | Next.js + Blueprint.js | Blueprint.js Palantir-aesthetic components; stronger React geospatial ecosystem; no Angular code existed |
| ADR-017 | Map engine | MapLibre GL + deck.gl | WebGL-native rendering for world-scale data; registry-driven layer model; open license; supersedes react-leaflet |
| ADR-018 | Workspace layout | Dockview | Registry-driven dockable widget workspace; decouples domain concerns from layout |
| ADR-010 | Alert persistence | PostgreSQL table on TimescaleDB | Transactional lifecycle transitions; deterministic `alert_id` gives idempotent upsert on replay |
| ADR-011 | Operator authentication | Google OAuth 2.0 + application JWT | No password management; trusted identity; short-lived JWT for API session |
| ADR-012 | Multi-tenant data scoping | Server-side workspace filtering | Workspace membership enforced at API; no client-side trust |
| ADR-013 | Ingestion poller runtime | Node.js | Unified runtime across all services; no polyglot operational overhead |
| ADR-014 | Alert Evaluator input model | Hybrid: Kafka topics + scheduled Redis scan | Proximity and deviation arrive via Kafka; signal loss has no Kafka event so requires a periodic scan |
| ADR-015 | Reference-route model | Deterministic static seed data | Synthetic entities with assigned corridors enable reproducible anomaly injection for demos and tests |

---

## Getting Started

Implementation begins with Phase 01. Once the infrastructure scaffold exists:

```bash
docker compose up -d
```

Service-specific commands and verification steps are added as each phase becomes executable.

---

<br>

---

<div align="center">
  <sub>Copyright &copy; 2026 Harshwardhan Patil &nbsp;&middot;&nbsp; All rights reserved</sub><br>
  <sub>Not licensed for reuse, redistribution, or commercial use &nbsp;&middot;&nbsp; Available for review and evaluation purposes only</sub>
</div>
