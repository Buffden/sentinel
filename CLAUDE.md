# CLAUDE.md  - Sentinel

Read this before touching anything. It tells you what the project is, what's decided, and how to behave as an engineering partner on it.

---

## What this project is

Sentinel is a real-time geospatial entity-tracking and anomaly-detection platform. It ingests live ADS-B (aircraft) and AIS (vessel) positional telemetry, correlates entities across time and space using a graph, and surfaces composite anomalies (signal loss, route deviation, unexpected proximity) on a live map dashboard.

**This is a portfolio/learning project, not a production system.** Every architectural decision exists to be defensible in a system design interview  - not to maximize feature count or visual polish.

Read `insights/intent.md` for the full context on why this project exists and what "done" looks like. Read `README.md` for the architecture overview and tech stack.

---

## Stack (decided  - do not change without an ADR)

| Concern | Technology | Why |
| --- | --- | --- |
| Message broker | Kafka (Redpanda locally, MSK on AWS) | Decouples ingestion from processing; absorbs bursty feeds |
| Position history | TimescaleDB | Geo-cell + time-bucket sharding matches the query pattern |
| Entity graph | Neo4j | Proximity/relationship queries are graph traversals, not table scans |
| Live entity state | Redis | Highest-frequency read; cache, not source of truth |
| Ingestion poller | Node.js | Shares TypeScript types and Kafka client config with the API; one runtime across the backend — see ADR-013 |
| API | Express (Node.js) | Lightweight, native async I/O, simple WebSocket via `ws` |
| Dashboard | Angular + Leaflet | Functional, not the point of the project |
| Operator auth | Google OAuth 2.0 + JWT | Identity required for per-user workspace persistence; no new infrastructure - users table on TimescaleDB |
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
- No service should call another service's internal API or private endpoints directly - use Kafka for event flow between services. Reading from the shared persistence layers (TimescaleDB, Neo4j, Redis) is not inter-service coupling; it is the intended access pattern.

### Naming
- Entity identifiers: `entity_id` (string, e.g. ICAO hex for aircraft, MMSI for vessels)
- Idempotency keys: `{entity_id}:{timestamp_ms}`  - used on every write, everywhere
- Kafka topics: `{entity_type}.{stage}` for per-source topics (e.g. `adsb.raw`, `ais.raw`, `adsb.dlq`, `ais.dlq`); descriptive names for cross-source topics (e.g. `position.normalized`, `alerts`)
- Redis keys and channels:
  - `entity:live:{entity_id}` - current position hash, includes `last_seen_ms`; TTL = `SIGNAL_LOSS_THRESHOLD_MS` (drives dashboard ghost cleanup only, not alert detection)
  - `alert-state:{entity_id}` - in-loop alert suppression flag; **no TTL** — written by alert evaluator on first emission, deleted explicitly by position consumer when entity resumes broadcasting; distinct from the durable dedup in the `alerts` table (TimescaleDB)
  - `deviation-counter:{entity_id}` - consecutive out-of-baseline ping count for sustained deviation detection (US-04)
  - `alert-evaluator:leader` - leader election lease key, SET NX PX pattern
  - `position-updates` - Redis pub/sub channel; position consumer publishes every normalised position event here after writing to the hash; all API instances subscribe and fan out to scoped WebSocket connections

### Code style
- Prefer explicit over clever  - this code will be read in an interview setting
- Every non-obvious decision gets a comment with "why", not "what"
- No magic numbers  - name constants and put units in the name (e.g. `SIGNAL_LOSS_THRESHOLD_MS`)

### Error handling
- Malformed or unparseable events go to a dead-letter topic  - never silently dropped, never crash the consumer
- Retries are idempotent by construction (idempotency key on every write)  - retry freely, don't over-engineer retry logic
- Alert evaluator failures must not result in duplicate alerts  - leader election handles this, not application-level dedup hacks

### Service contracts
- Correlation worker Kafka consumer group: `correlation-worker`; consumes `position.normalized`; writes PROXIMITY_EVENT edges to Neo4j only — does not write to TimescaleDB, Redis, or Kafka

### Commits
- Commit message format: `<scope>: <what and why>`  - e.g. `ingestion: add DLQ for malformed AIS events`
- Scopes match service names: `ingestion`, `position-consumer`, `correlation`, `alert-evaluator`, `api`, `dashboard`, `infra`, `schema`

---

## What to optimize for (in priority order)

1. **Correctness and defensibility of design decisions** over feature completeness. Three well-reasoned, well-documented components beat ten shallow ones.
2. **Surfacing trade-offs**, not hiding them. When implementing something, note what alternative was possible and why it wasn't chosen  - this belongs in an ADR or inline comment, not lost in commit history.
3. **Boring, explainable technology** over impressive-sounding technology, unless the impressive option is genuinely the right tool for the access pattern.
4. **Flagging scope creep.** If a suggested feature doesn't map to a system design concept being demonstrated, say so before building it.

---

## Hard constraints  - never do these

- Do not use defense/intelligence-flavored language, naming, or framing anywhere in code, comments, or docs. The project is a technical demo system, not a surveillance platform.
- Do not directly couple services via inter-service HTTP calls (e.g. alert evaluator calling the position consumer's REST endpoints). Use Kafka for event flow or read from the shared persistence layers (TimescaleDB, Neo4j, Redis) directly - that is not coupling, that is the intended access pattern.
- Do not skip the idempotency key on any write to any store.
- Operator authentication uses Google OAuth 2.0 (ADR-011). Do not add other identity providers, username/password flows, or account management UI beyond what ADR-011 defines. Ingestion clients continue to use API key auth. Do not add RBAC beyond operator/viewer until it is justified by a real multi-role requirement.
- Do not add ML-based anomaly detection  - rule- and correlation-based only in v1. ML is a stated future direction, not a current task.
- Do not change the tech stack without writing an ADR first.
- Do not over-invest in the dashboard/frontend. If a task is purely visual polish with no backend design implication, flag it and deprioritize it.

---

## Key documents

| File | Purpose |
| --- | --- |
| `insights/intent.md` | Why this project exists, what success looks like, full context for decision-making (private, gitignored) |
| `README.md` | Architecture overview, stack, anomaly types, project structure |
| `docs/adr/` | Architecture Decision Records  - read the relevant ADR before implementing anything in its scope |
| `docs/ARCHITECTURE.md` | Component contracts and service boundaries (to be written) |
| `docs/DATA_MODEL.md` | Schemas for every store (to be written) |
| `docs/DEVELOPMENT.md` | Local setup, seed data, load generator usage (to be written as services are built) |
