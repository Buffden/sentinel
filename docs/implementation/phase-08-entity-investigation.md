# Phase 08 — Entity Investigation

## Goal

Build enough operator functionality to understand and investigate an incident.

Implement:

- `GET /entities` — live entity positions from Redis, workspace-scoped
- `GET /entities/:entity_id` — current state from Redis + recent alerts from TimescaleDB
- `GET /entities/:entity_id/history` — position timeline from `position_history`
- `GET /entities/:entity_id/graph` — Neo4j pivot: `PROXIMITY_EVENT` and `KNOWN_ASSOCIATE` edges
- `GET /alerts` — alert feed from TimescaleDB, filterable by status and type, workspace-scoped
- `GET /alerts/:alert_id` — full alert detail including payload and counterparty
- Dashboard: live map with entity markers, alert feed panel, investigation panel

Keep UI functional rather than polished. The architecture is what matters here.

---

## Learning Goals

For every query, ask and answer:

> Which datastore best matches this access pattern?

Typical answers for Sentinel:

- current entity state → Redis (lowest latency, highest frequency read)
- time-ordered position history → TimescaleDB (time-series partitioning, range queries)
- relationship traversal and graph context → Neo4j (traversals, not table scans)

Understanding why each query goes to the right store is the core learning goal of this phase.

---

## Suggested Checkpoints

1. `GET /entities` returns live positions from Redis with workspace scope applied server-side
2. `GET /entities/:entity_id/history` returns a time-range query from TimescaleDB; inspect the query plan with `EXPLAIN ANALYZE`
3. `GET /entities/:entity_id/graph` returns Neo4j proximity and associate edges; inspect the Cypher query
4. Dashboard renders entity markers on the Leaflet map; markers update via WebSocket
5. Alert feed panel displays live alerts; acknowledge/resolve actions work
6. Investigation panel links alert → entity → graph

---

## Exit Criteria

- an operator can open an alert, navigate to the entity, view its recent track, and see its proximity history in the graph
- all queries go to the correct datastore
- workspace scope is enforced server-side on every endpoint
- the dashboard is functional enough to demonstrate an end-to-end anomaly scenario using synthetic entities
