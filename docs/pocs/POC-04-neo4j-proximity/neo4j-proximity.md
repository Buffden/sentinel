# POC-04: Neo4j Proximity

**Branch:** `poc/neo4j-proximity`
**Status:** Not started

---

## Risk

Prior Neo4j experience exists (RingNet), but that project used static relationship queries. This project uses streaming writes with idempotency via `MERGE`, real-time proximity traversals, and multi-hop correlation queries for composite alerting - a different access pattern.

---

## Goal

Prove that the Neo4j data model, idempotent edge writes, and all core Cypher queries work correctly.

---

## Validate

- `MERGE` on proximity edges with the idempotency key (`entity_id:timestamp_ms`) correctly creates the edge on first write and is a no-op on duplicate writes (US-11)
- Core traversal query executes correctly: "Given entity X, find all entities connected by a PROXIMITY_EVENT edge within the last N hours that have no prior KNOWN_ASSOCIATE relationship with X" (US-05)
- Multi-hop correlation query executes correctly: "Given entity X that is dark, find all entities that came within proximity after X went dark and have no KNOWN_ASSOCIATE edge" (US-06)
- Performance is acceptable on a small synthetic graph (hundreds of entities, thousands of edges)

---

## Done When

- Duplicate write test passes: inserting the same proximity event 3 times results in exactly 1 edge
- Proximity traversal query returns correct results on a synthetic graph with known expected output
- Composite correlation query correctly identifies the signal loss + proximity pattern and returns no result when only one signal is present (US-06 single signal path)
- Query time is logged as a baseline for future comparison

---

## ADR Coverage

- [ADR-003 - Neo4j for Entity Relationship Graph](../../adr/ADR-003-neo4j-entity-graph.md)
- [ADR-007 - Idempotency Key Schema](../../adr/ADR-007-idempotency-key-schema.md)

## Use Case Coverage

- [US-05](../../use-cases/US-05-unscheduled-proximity/unscheduled-proximity.md) - unscheduled proximity
- [US-06](../../use-cases/US-06-composite-alert/composite-alert.md) - composite alert
- [US-11](../../use-cases/US-11-idempotent-writes/idempotent-writes.md) - idempotent writes (Neo4j)
