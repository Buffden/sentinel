# ADR-003: Neo4j for Entity Relationship Graph

**Status:** Accepted
**Date:** 2026-08-05

---

## Context

The correlation worker needs to track relationships between entities  - specifically, which entities have been in proximity to each other, when, and where. The alert evaluator then queries these relationships to detect composite anomalies (e.g. signal loss followed by proximity to a previously unrelated entity).

The core query is: "Given entity X, find all entities that have been within distance D of X during the last N hours, and return any that have no prior relationship with X."

---

## Decision

Use Neo4j as the entity relationship graph store.

---

## Reasoning

**Proximity queries are graph traversals, not table scans.** The question "who has been near whom" is naturally expressed as: find all nodes connected to node X by a PROXIMITY_EVENT edge within a time window. In a relational model, this requires a self-join on a proximity events table  - expensive as the entity count grows. In Neo4j, it's a single Cypher traversal starting from the entity node.

**Relationship metadata is a first-class citizen.** Each PROXIMITY_EVENT edge can carry properties: timestamp, location, distance, duration. Querying "find all proximity events between entity A and entity B in the last 24 hours" is a direct edge property filter, not a join.

**Variable-depth traversal.** If future requirements include "find all entities that are two hops away from a flagged entity" (e.g. entities that were near entities that were near a flagged vessel), Neo4j handles this natively. Relational models require recursive CTEs or application-level BFS.

**Prior experience.** The existing RingNet project established Neo4j competency for graph-modeled fraud detection. This reuses that skill in a different access pattern (streaming proximity vs. static relationship queries).

---

## Alternatives Considered

### PostgreSQL with adjacency table (rejected)
- Possible for simple "who was near whom" queries with a self-join
- Variable-depth traversal requires recursive CTEs  - correct but slow and awkward
- Relationship metadata (edge properties) must be stored in extra columns, losing the natural graph model
- Does not scale gracefully as relationship density increases

### Amazon Neptune / ArangoDB (rejected)
- Neptune: managed graph database, but adds AWS dependency at a stage where local Docker Compose is the target environment
- ArangoDB: multi-model (document + graph), but the graph query language (AQL) is less mature than Cypher and the community/tooling is smaller

---

## Consequences

- Neo4j requires a separate Docker container and sufficient heap allocation (default JVM settings are too low for sustained load)
- The Cypher query language must be used  - no ORM abstraction; queries are written explicitly
- Neo4j's transaction model differs from RDBMS  - idempotency on edge creation must be handled with MERGE, not INSERT (addressed in ADR-007)
