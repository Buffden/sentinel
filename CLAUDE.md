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
| Position history | TimescaleDB | Time-based partitioning (`observed_at`); geo_cell is an index column for spatial filtering |
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
  - `entity:live:{entity_id}` - current position hash, includes `last_seen_ms`; **TTL = 24h** (safety-net only — prevents permanent ghost keys if an entity disappears before the alert evaluator detects it; dashboard cleanup is client-side via `last_seen_ms` comparison; signal loss detection is driven by `last_seen_ms`, not TTL expiry — the key must outlive `SIGNAL_LOSS_THRESHOLD_MS` or the evaluator can never scan it)
  - `geo-cell:{h3_cell_id}` - Redis **sorted set**; score = `last_seen_ms`; written by position consumer (`ZREM` old cell, `ZADD` new cell with score) using `LIVE_H3_RESOLUTION`; read by correlation worker via `ZRANGEBYSCORE` to get only fresh members in same-cell + computed k-ring cells; stale members age out via score filter — no explicit cleanup needed. **k-ring size** computed from `PROXIMITY_THRESHOLD_METRES` and cell edge length at `LIVE_H3_RESOLUTION` (not hardcoded k-ring(1)). Two separate resolution configs: `LIVE_H3_RESOLUTION` (for this Redis key) and `HISTORY_H3_RESOLUTION` (for TimescaleDB `geo_cell` column); validated by POC-03.
  - `proximity-episode:{pair_key}` - proximity episode state; hash with fields `episode_start_ms`, `last_seen_ms`, `candidate_published` (0/1 — tracks whether Kafka publish succeeded; enables retry recovery); TTL = `PROXIMITY_EPISODE_GAP_MS` (refreshed on each within-threshold ping); canonical pair = `min(a,b):max(a,b)`; written by correlation worker on first detection in a new episode; subsequent pings update `last_seen_ms` and refresh TTL instead of publishing a new candidate
  - `recent-loss:{entity_id}` - hash; TTL = `COMPOSITE_CORRELATION_WINDOW_MS`; written by position consumer when entity resumes after signal loss (before DEL of `alert-state`); fields: `dark_since_ms`, `resumed_at_ms`, `signal_loss_alert_id`; read by alert evaluator when proximity arrives to check for composite supersession opportunity
  - `deviation-state:{entity_id}` - hash; fields: `count`, `episode_start_ms`, `last_processed_ms`, `alert_emitted`; written by alert evaluator; safety TTL = `DEVIATION_STATE_TTL_MS` (default: 24h; decoupled from signal-loss timing so deviation state survives brief dark periods); DEL on `IN_RANGE` event; `last_processed_ms` is the replay guard — ignore events with `timestamp_ms <= last_processed_ms`
  - `alert-state:{entity_id}` - hash; fields: `dark_since_ms`, `signal_loss_alert_id`; **no TTL** — written by alert evaluator on first signal loss emission; deleted by position consumer after writing `recent-loss`; read by alert evaluator (suppression) and correlation worker (composite check for active dark entity)
  - `alert-evaluator:leader` - leader election lease key; SET NX PX (acquire); Lua compare-and-expire on heartbeat (`if GET == instance_id then PEXPIRE` — `SET XX PX` is NOT safe for renewal because it overwrites the value without checking the current holder); compare-before-DEL on release (`if GET == instance_id then DEL`)
  - `position-updates` - Redis pub/sub channel; position consumer publishes every normalised position event here after writing to the hash; all API instances subscribe and fan out to scoped WebSocket connections
  - `alert-events` - Redis pub/sub channel; typed envelope `{ type: ALERT_CREATED | ALERT_STATUS_CHANGED | ALERT_SUPERSEDED, payload: {...} }`; the API instance that acted on the event publishes here; all API instances subscribe and fan out to scope-matched WebSocket connections — solves the Kafka consumer group fan-out problem

### Code style
- Prefer explicit over clever  - this code will be read in an interview setting
- Every non-obvious decision gets a comment with "why", not "what"
- No magic numbers  - name constants and put units in the name (e.g. `SIGNAL_LOSS_THRESHOLD_MS`)

### Error handling
- Malformed or unparseable events go to a dead-letter topic  - never silently dropped, never crash the consumer
- Retries are idempotent by construction (idempotency key on every write)  - retry freely, don't over-engineer retry logic
- Alert evaluator failures must not result in duplicate alerts  - leader election handles this, not application-level dedup hacks

### Service contracts

- Correlation worker Kafka consumer group: `correlation-worker`; consumes `position.normalized`; scopes proximity candidates using H3 `ZRANGEBYSCORE` on `geo-cell:*` sorted sets at `LIVE_H3_RESOLUTION`; canonical pair = `min(a,b):max(a,b)` everywhere; checks `proximity-episode:{pair}` — if active episode exists, update `last_seen_ms` + refresh TTL + skip publish; if new episode: **write order: Neo4j MERGE first then Kafka publish**; writes `proximity-episode:{pair}` hash (TTL = PROXIMITY_EPISODE_GAP_MS); does not write to TimescaleDB
- Deviation detector Kafka consumer group: `deviation-detector`; consumes `position.normalized`; reads `route_reference_points` (via `route_references`) from TimescaleDB; **stateless** — emits `OUT_OF_RANGE` or `IN_RANGE` on every eligible ping (no transition tracking); skips entities with no reference route (real ADS-B/AIS); does not write to Redis, Neo4j, or the `alerts` topic; see ADR-015
- **Two replay modes:** (A) Crash recovery — consumer resumes from last committed offset; normal processing allowed. (B) Historical backfill — explicit separate consumer group or `--mode=backfill` flag; must disable ephemeral side effects (Redis live-state writes, `position-updates` pub/sub, deviation/proximity candidates, alerts) as appropriate for the store being rebuilt. Never use arbitrary full historical replay to re-warm Redis live state — prefer latest-row-per-entity from TimescaleDB or a bounded recent Kafka tail.

### Commits
- Commit message format: `<scope>: <what and why>`  - e.g. `ingestion: add DLQ for malformed AIS events`
- Scopes match service names: `ingestion`, `position-consumer`, `correlation`, `deviation-detector`, `alert-evaluator`, `api`, `dashboard`, `infra`, `schema`

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
| `docs/ARCHITECTURE.md` | Component contracts, service boundaries, Kafka topics, consumer groups, and persistence ownership |
| `docs/DATA_MODEL.md` | Schemas for all stores — TimescaleDB tables, Neo4j nodes/edges, Redis keys, Kafka event schemas |
| `docs/DEVELOPMENT.md` | Local setup, seed data, load generator usage (to be written as services are built) |
