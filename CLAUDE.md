# CLAUDE.md  - Sentinel

Read this before touching anything. It tells you what the project is, what's decided, and how to behave as an engineering partner on it.

---

## What this project is

Sentinel is a real-time geospatial entity-tracking and anomaly-detection platform. It ingests live ADS-B (aircraft) and AIS (vessel) positional telemetry, correlates entities across time and space using a graph, and surfaces composite anomalies (signal loss, route deviation, unexpected proximity) on a live map dashboard.

**This is a portfolio/learning project, not a production system.** Every architectural decision exists to be defensible in a system design interview  - not to maximize feature count or visual polish.

Read `intent.md` for the full context on why this project exists and what "done" looks like. Read `README.md` for the architecture overview and tech stack.

---

## Stack (decided  - do not change without an ADR)

| Concern | Technology | Why |
| --- | --- | --- |
| Message broker | Kafka (Redpanda locally, MSK on AWS) | Decouples ingestion from processing; absorbs bursty feeds |
| Position history | TimescaleDB | Geo-cell + time-bucket sharding matches the query pattern |
| Entity graph | Neo4j | Proximity/relationship queries are graph traversals, not table scans |
| Live entity state | Redis | Highest-frequency read; cache, not source of truth |
| API | Express (Node.js) | Lightweight, native async I/O, simple WebSocket via `ws` |
| Dashboard | Angular + Leaflet | Functional, not the point of the project |
| Deployment | Docker Compose (local) → AWS | Same CI/CD pattern as existing projects |

If a suggested library or tool isn't in this table, flag it and explain the trade-off before adding it. Do not introduce new infrastructure components silently.

---

## How to run (update this as services are scaffolded)

```bash
docker compose up -d        # Start all backing services
# Individual service instructions will live in each service's README
```

---

## Conventions

### General
- Services are independently deployable  - no shared in-process state across service boundaries
- Each service owns exactly one concern (ingestion, position storage, correlation, alert evaluation, API, dashboard)
- No service should directly query another service's database  - go through the API or the message bus

### Naming
- Entity identifiers: `entity_id` (string, e.g. ICAO hex for aircraft, MMSI for vessels)
- Idempotency keys: `{entity_id}:{timestamp_ms}`  - used on every write, everywhere
- Kafka topics: `{entity_type}.{stage}`  - e.g. `adsb.raw`, `ais.raw`, `position.normalized`
- Redis keys: `entity:live:{entity_id}` for current position

### Code style
- Prefer explicit over clever  - this code will be read in an interview setting
- Every non-obvious decision gets a comment with "why", not "what"
- No magic numbers  - name constants and put units in the name (e.g. `SIGNAL_LOSS_THRESHOLD_MS`)

### Error handling
- Malformed or unparseable events go to a dead-letter topic  - never silently dropped, never crash the consumer
- Retries are idempotent by construction (idempotency key on every write)  - retry freely, don't over-engineer retry logic
- Alert evaluator failures must not result in duplicate alerts  - leader election handles this, not application-level dedup hacks

### Commits
- Commit message format: `<scope>: <what and why>`  - e.g. `ingestion: add DLQ for malformed AIS events`
- Scopes match service names: `ingestion`, `position-consumer`, `correlation`, `alert-evaluator`, `api`, `dashboard`, `infra`

---

## What to optimize for (in priority order)

1. **Correctness and defensibility of design decisions** over feature completeness. Three well-reasoned, well-documented components beat ten shallow ones.
2. **Surfacing trade-offs**, not hiding them. When implementing something, note what alternative was possible and why it wasn't chosen  - this belongs in an ADR or inline comment, not lost in commit history.
3. **Boring, explainable technology** over impressive-sounding technology, unless the impressive option is genuinely the right tool for the access pattern.
4. **Flagging scope creep.** If a suggested feature doesn't map to a system design concept being demonstrated, say so before building it.

---

## Hard constraints  - never do these

- Do not use defense/intelligence-flavored language, naming, or framing anywhere in code, comments, or docs. The project is a technical demo system, not a surveillance platform.
- Do not directly couple services (e.g. alert evaluator querying TimescaleDB directly  - it goes through the correlation worker or API).
- Do not skip the idempotency key on any write to any store.
- Do not add authentication/account management beyond basic API key auth (ingestion clients) and RBAC (dashboard viewers)  - explicitly out of scope for v1.
- Do not add ML-based anomaly detection  - rule- and correlation-based only in v1. ML is a stated future direction, not a current task.
- Do not change the tech stack without writing an ADR first.
- Do not over-invest in the dashboard/frontend. If a task is purely visual polish with no backend design implication, flag it and deprioritize it.

---

## Key documents

| File | Purpose |
| --- | --- |
| `intent.md` | Why this project exists, what success looks like, full context for decision-making |
| `README.md` | Architecture overview, stack, anomaly types, project structure |
| `docs/adr/` | Architecture Decision Records  - read the relevant ADR before implementing anything in its scope |
| `docs/ARCHITECTURE.md` | Component contracts and service boundaries (to be written) |
| `docs/DATA_MODEL.md` | Schemas for every store (to be written) |
| `docs/DEVELOPMENT.md` | Local setup, seed data, load generator usage (to be written as services are built) |
