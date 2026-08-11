# Sentinel

A real-time geospatial entity-tracking and rule-based anomaly-correlation platform. Sentinel ingests ADS-B/AIS positional telemetry, maintains current and historical entity state, detects rule-based anomalies, correlates weak signals, and surfaces operator-visible alerts on an Angular + Leaflet dashboard.

Sentinel is a portfolio/learning project designed to demonstrate distributed-systems reasoning: streaming ingestion, replay/idempotency, polyglot persistence, leader election, geospatial indexing, stateful correlation, and observable failure behavior.

---

## Architecture Overview

![Architecture](diagrams/docs/architecture.svg)

Canonical flow:

```text
OpenSky / AISHub / synthetic generator
  → Ingestion Poller
  → Kafka raw topics
  → Position Consumer
      → TimescaleDB position history
      → Redis live state + H3 sorted-set index
      → position.normalized
          ├→ Deviation Detector → deviation.candidates
          └→ Correlation Worker → Neo4j evidence + KNOWN_ASSOCIATE filter
                                  → proximity.candidates

Redis signal-loss scan ─────────────────────────┐
deviation.candidates ───────────────────────────┤
proximity.candidates ───────────────────────────┤
Redis anomaly episode state ────────────────────→ Alert Evaluator
                                                   → alerts Kafka
                                                   → API
                                                     → TimescaleDB alerts
                                                     → Redis alert-events
                                                     → REST/WebSocket
                                                     → Angular + Leaflet

API → Neo4j for operator investigation/evidence reads.
```

The Alert Evaluator remains the complete Alert Layer. Neo4j relationship filtering is performed upstream by the Correlation Worker before `proximity.candidates` is emitted.

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
| Dashboard | Angular + Leaflet | functional operator interface |

---

## Anomaly Types

| Alert | Detection |
| --- | --- |
| `SIGNAL_LOSS` | entity remains unseen beyond configurable threshold |
| `ROUTE_DEVIATION` | synthetic entity remains outside assigned route corridor for sustained pings |
| `UNSCHEDULED_PROXIMITY` | exact proximity episode between a pair with no `KNOWN_ASSOCIATE` relationship |
| `COMPOSITE` | qualifying signal-loss episode correlated with unscheduled proximity |

---

## Delivery Semantics

Sentinel does not claim exactly-once transport.

- Kafka processing is at-least-once.
- Durable TimescaleDB and Neo4j side effects are idempotent by deterministic logical identity.
- Redis live state is protected against source-time regression.
- Alert rows have type-specific deterministic IDs.
- WebSocket lifecycle delivery is at-least-once; clients tolerate duplicates.
- Leader election prevents concurrent active Alert Evaluators but is not a replacement for durable idempotency.

---

## H3 Usage

Sentinel uses two configurable H3 access patterns:

- `HISTORY_H3_RESOLUTION`: `position_history.geo_cell`, an indexed query column inside TimescaleDB time chunks;
- `LIVE_H3_RESOLUTION`: Redis `geo-cell:*` sorted sets used to reduce live proximity candidates.

TimescaleDB partitions `position_history` by `observed_at` only. H3 cells are not TimescaleDB chunks/shards in this design.

---

## Data Sources

- OpenSky Network — public ADS-B source subject to its terms/rate limits.
- AISHub — AIS aggregator; eligibility/access must be verified.
- Synthetic load generator — deterministic anomaly injection for demos and tests.

This is a non-commercial portfolio/learning project. Third-party source terms and attribution requirements must be respected; see `NOTICE`.

---

## Implementation Roadmap

1. Infrastructure + Canonical Schemas + Observability Skeleton
2. Live Position Pipeline
3. Signal Loss + Alert Delivery Foundation
4. Route Deviation
5. Correlation Worker + Unscheduled Proximity
6. Composite Correlation
7. Workspace + Operator Scope
8. Alert Lifecycle + Distributed Fan-Out
9. Entity Investigation
10. Production Hardening + Failure Lab

Permanent phase plans live in `docs/implementation/`. Implementation process is defined by `docs/IMPLEMENTATION_PLAYBOOK.md` and `docs/IMPLEMENTATION_WORKFLOW.md`.

---

## Architecture Decision Records

Accepted ADRs live in `docs/adr/`:

- ADR-001 Kafka over direct HTTP ingestion
- ADR-002 TimescaleDB for position history
- ADR-003 Neo4j entity graph
- ADR-004 Redis live state
- ADR-005 Alert Evaluator leader election
- ADR-006 H3 geo-cell indexing strategy
- ADR-007 deterministic idempotency identity
- ADR-008 Express API layer
- ADR-009 Angular dashboard
- ADR-010 durable alert lifecycle store
- ADR-011 Google OAuth operator auth
- ADR-012 workspace scope / server-side filtering
- ADR-013 Node.js ingestion poller
- ADR-014 hybrid Alert Evaluator input model
- ADR-015 v1 deterministic reference-route model

---

## Getting Started

Implementation begins with Phase 01. Once the infrastructure scaffold exists:

```bash
docker compose up -d
```

Service-specific commands and verification steps are added as each phase becomes executable.

---

## Future Work

Out of v1 scope:

- multi-region ingestion;
- ML-based ranking/scoring;
- multi-tenant organizational isolation;
- richer historical audit/replay UX.

---

## License

Copyright (c) 2026 Harshwardhan Patil. All rights reserved.

Available for personal, educational, and portfolio review only. See `LICENSE`.
