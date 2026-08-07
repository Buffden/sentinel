# Sentinel

A real-time geospatial entity-tracking and anomaly-detection platform. Sentinel ingests live positional telemetry from aircraft (ADS-B) and vessels (AIS), correlates entities across time and space, and surfaces meaningful anomalies  - signal loss, route deviation, unexpected proximity between previously unrelated entities  - on a live map dashboard.

Built to exercise the full surface area of distributed systems design: high-throughput streaming ingestion, polyglot persistence chosen per access pattern, graph-based correlation, and symptom-driven alerting  - all against real, unreliable, public telemetry rather than synthetic data.

---

## Problem Statement

Given a continuous, high-volume stream of positional pings from moving entities, detect and surface meaningful anomalies with low latency, without flooding operators with false positives, and without losing data during ingestion bursts or partial component failure.

---

## Architecture Overview

![Architecture](diagrams/docs/Sentinel%20Architecture.svg)

---

## Core Technical Decisions

| Decision | Choice | Why |
| --- | --- | --- |
| Ingestion buffer | Kafka (MSK on AWS, Redpanda locally) | absorbs bursty feeds; decouples producers from consumers |
| Position history | TimescaleDB | geo-cell + time-bucket sharding matches the query shape |
| Entity graph | Neo4j | proximity queries are traversals, not table scans |
| Live state | Redis | highest-frequency read; cache, not source of truth |
| Alert coordination | Leader election | prevents duplicate emission under horizontal scale |
| Anomaly model | Composite correlation | single weak signal is not actionable; correlate across graph + time |

Full reasoning and rejected alternatives in each ADR → [`docs/adr/`](docs/adr/)

---

## Tech Stack

| Layer | Technology |
| --- | --- |
| Data ingestion | Python/Node poller → Kafka (Redpanda locally, MSK on AWS) |
| Position store | TimescaleDB (geo-cell + time-bucket sharding) |
| Correlation graph | Neo4j |
| Live state cache | Redis |
| Alert evaluation | Leader-elected worker service |
| API | Express (Node.js) - REST + WebSocket |
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
| **Route deviation** | Current track diverges from established historical baseline by more than threshold distance |
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

---

## Getting Started

```bash
# Clone the repo
git clone https://github.com/<your-username>/sentinel.git
cd sentinel

# Start all services
docker compose up -d

# Start the ingestion poller (uses recorded data by default, no live API key needed)
cd ingestion && npm install && npm run start:replay

# Open the dashboard
open http://localhost:4200
```

> To use live ADS-B data, set `OPENSKY_USERNAME` and `OPENSKY_PASSWORD` in `.env`. See `.env.example`.

---

## License

Copyright (c) 2026 Harshwardhan Patil. All rights reserved.

This project is available for personal, educational, and portfolio review only.
Commercial use, redistribution, and use as a basis for competing products are prohibited.
See [LICENSE](LICENSE) for full terms.
