# Sentinel

> **Status: Architecture and planning phase — implementation in progress.**
> No services have been built yet. `docker compose up -d` will not work until Phase 01 is complete.
> See [`docs/implementation/`](docs/implementation/) for the phased build plan.

A real-time geospatial entity-tracking and anomaly-detection platform. Sentinel ingests live positional telemetry from aircraft (ADS-B) and vessels (AIS), correlates entities across time and space, and surfaces meaningful anomalies  - signal loss, route deviation, unexpected proximity between previously unrelated entities  - on a live map dashboard.

Built to exercise the full surface area of distributed systems design: high-throughput streaming ingestion, polyglot persistence chosen per access pattern, graph-based correlation, and symptom-driven alerting. Ingests real public telemetry (ADS-B, AIS); synthetic data drives controllable anomaly injection for demos.

---

## Problem Statement

Given a continuous, high-volume stream of positional pings from moving entities, detect and surface meaningful anomalies with low latency, without flooding operators with false positives, and without losing data during ingestion bursts or partial component failure.

---

## Architecture Overview

![Architecture](diagrams/docs/architecture.svg)

---

## Core Technical Decisions

| Decision | Choice | Why |
| --- | --- | --- |
| Ingestion buffer | Kafka (MSK on AWS, Redpanda locally) | absorbs bursty feeds; decouples producers from consumers |
| Position history | TimescaleDB | time-based partitioning on `observed_at`; `geo_cell` is a spatial index column, not a partition dimension |
| Entity graph | Neo4j | proximity queries are traversals, not table scans |
| Live state | Redis | highest-frequency read; cache, not source of truth |
| Alert coordination | Leader election | prevents duplicate emission under horizontal scale |
| Anomaly model | Composite correlation | single weak signal is not actionable; correlate across graph + time |

Full reasoning and rejected alternatives in each ADR → [`docs/adr/`](docs/adr/)

---

## Tech Stack

| Layer | Technology |
| --- | --- |
| Data ingestion | Node.js poller → Kafka (Redpanda locally, MSK on AWS) |
| Stream processing | Position Consumer (normalise + persist), Correlation Worker (proximity graph), Deviation Detector (reference route comparison) |
| Position store | TimescaleDB |
| Correlation graph | Neo4j |
| Live state cache | Redis |
| Alert evaluation | Leader-elected Alert Evaluator — hybrid inputs: scheduled Redis scan (signal loss) + `deviation.candidates` + `proximity.candidates` |
| API | Express (Node.js) - REST + WebSocket |
| Operator auth | Google OAuth 2.0 + JWT (identity required for per-user workspace) |
| Dashboard | Angular + Leaflet |
| Deployment | Docker Compose → AWS |
| CI/CD | GitHub Actions |

---

## Data Sources

- **Aircraft:** [OpenSky Network](https://opensky-network.org/) - free, public ADS-B feed, restricted to non-commercial and research use. See [OpenSky Terms of Use](https://opensky-network.org/about/terms-of-use).
- **Vessels:** [AISHub](https://www.aishub.net/) - AIS data aggregator. Access requires contributing AIS data or explicit approval. Verify eligibility before use.
- **Synthetic generator:** controllable anomaly injection for demos (real feeds do not reliably produce interesting events on demand)

This project is a non-commercial portfolio and learning exercise. All data source terms of service are respected - rate limits are honoured and attribution is included where required. See `NOTICE` for third-party attributions.

---

## Anomaly Types Detected

| Anomaly | Description |
| --- | --- |
| **Signal loss** | Entity goes dark beyond configurable threshold (AIS/ADS-B transponder off) |
| **Route deviation** | Current track diverges from the entity's assigned reference route by more than the corridor threshold (synthetic entities only in v1) |
| **Unscheduled proximity** | Two entities with no prior relationship converge at an unexpected location |
| **Composite** | Signal loss followed by proximity to a previously unrelated entity  - elevated to a single correlated alert |

---

## Architecture Decision Records

Significant design choices are documented in `/docs/adr/` with alternatives considered and explicitly rejected.

| ADR | Decision |
| --- | --- |
| ADR-001 | Kafka (MSK on AWS, Redpanda locally) over direct HTTP ingestion |
| ADR-002 | TimescaleDB over Cassandra for position history |
| ADR-003 | Neo4j for entity relationship graph |
| ADR-004 | Redis for live entity state |
| ADR-005 | Leader election strategy for alert evaluator |
| ADR-006 | Geo-cell sharding key design and hot-spot mitigation |
| ADR-007 | Idempotency key schema |
| ADR-008 | Express (Node.js) for the API layer |
| ADR-009 | Angular + Leaflet for the dashboard |
| ADR-010 | Alert lifecycle state in PostgreSQL table on TimescaleDB |
| ADR-011 | Google OAuth 2.0 for operator authentication |
| ADR-012 | Workspace scope and server-side alert filtering |
| ADR-013 | Node.js for the ingestion poller |
| ADR-014 | Hybrid input model for the Alert Evaluator (Deviation Detector + stream-based inputs) |
| ADR-015 | v1 reference route model for deviation detection |

---

## Getting Started

> Implementation has not started yet. These instructions will work once Phase 01 and Phase 02 are complete.

```bash
# Clone the repo
git clone https://github.com/<your-username>/sentinel.git
cd sentinel

# Start all services (requires Phase 01 complete)
docker compose up -d
```

> Service-specific startup instructions will be added to each service's README as phases are completed. See [`docs/implementation/`](docs/implementation/) for the build plan.

---

## Future Work

These are intentionally out of scope for v1 but are the natural next steps:

- **Multi-region ingestion** — regional collectors buffering and forwarding to a central store, with explicit consistency tiers (strongly consistent: entity identity and alert state; eventually consistent: historical position backfill)
- **ML-based anomaly scoring** — layering a scoring model on top of the rule-based detection to rank alert severity
- **Multi-tenant operator isolation** — scoped data access per organisation, not just per operator workspace
- **Historical alert replay and audit log** — browsing past anomalies in the dashboard

---

## License

Copyright (c) 2026 Harshwardhan Patil. All rights reserved.

This project is available for personal, educational, and portfolio review only.
Commercial use, redistribution, and use as a basis for competing products are prohibited.
See [LICENSE](LICENSE) for full terms.
