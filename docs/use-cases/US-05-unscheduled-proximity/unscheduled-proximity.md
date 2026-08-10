# US-05: Unscheduled Proximity Alert

**Actor:** Operator
**Status:** Defined

---

## Story

As an operator, I want to receive an alert when two entities with no prior relationship converge at an unexpected location so that I can identify unscheduled proximity events.

---

## Acceptance Criteria

- The system detects when two entities come within `PROXIMITY_THRESHOLD_METRES` of each other
- An alert is emitted only if the two entities have no prior recorded relationship (no existing KNOWN_ASSOCIATE edge in the graph)
- The alert includes both entity IDs, the time and location of convergence, and the distance at closest approach
- Routine proximity events between known-associated entities (e.g. regular flight paths crossing) do not produce alerts

---

## Flow Diagrams

### Proximity Detection

![Proximity Detection](../../../diagrams/docs/use-cases/US-05-unscheduled-proximity/proximity-detection.svg)

On each `position.normalized` event, the correlation worker reads the H3 k-ring(1) of the incoming entity's `geo_cell` (7 cells at resolution 5, ~1764 km² coverage) from `geo-cell:{h3_cell_id}` Redis sets to get candidate entity_ids, then fetches their positions from `entity:live:{entity_id}`. This scopes proximity comparisons to geographically relevant neighbours — not a full O(n²) pairwise scan. For each candidate within `PROXIMITY_THRESHOLD_METRES`, the worker queries Neo4j for a prior relationship, writes a PROXIMITY_EVENT edge, and — for unscheduled pairs only — publishes a `proximity.candidates` event to Kafka. The Alert Evaluator consumes `proximity.candidates`, emits the alert to the `alerts` topic, and the API writes it to the alerts table (status: NEW, idempotent) before pushing to scope-matched WebSocket connections.

### Graph Update

![Graph Update](../../../diagrams/docs/use-cases/US-05-unscheduled-proximity/graph-update.svg)

The correlation worker writes the proximity event as an edge in Neo4j using MERGE (idempotent under Kafka replay), then also publishes a `proximity.candidates` event to Kafka for unscheduled pairs.

---

## Architectural Justification

Justifies: [ADR-003 - Neo4j for Entity Relationship Graph](../../adr/ADR-003-neo4j-entity-graph.md)

"No prior relationship" is a graph absence query - checking whether an edge exists between two entity nodes. In a relational model this requires a self-join on a proximity events table, which becomes expensive as entity count grows. In Neo4j, it is a single Cypher traversal starting from an entity node, checking for the absence of a KNOWN_ASSOCIATE edge. The graph model also stores prior proximity events as PROXIMITY_EVENT edges with timestamp and location properties, making the relationship history queryable directly.
