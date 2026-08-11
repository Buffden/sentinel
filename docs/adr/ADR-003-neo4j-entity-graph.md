# ADR-003: Neo4j for Entity Relationship Graph

**Status:** Accepted
**Date:** 2026-08-05

---

## Context

Sentinel needs a durable graph of entity relationships and proximity evidence. The Correlation Worker must answer graph-oriented questions about a specific entity pair, especially whether a pre-existing `KNOWN_ASSOCIATE` relationship exists before treating proximity as anomalous.

The API also needs relationship/proximity evidence for operator investigation.

---

## Decision

Use Neo4j as the entity relationship graph store.

Neo4j is owned operationally by the Correlation Worker for runtime relationship/evidence writes and by the API for investigation reads. The Alert Evaluator does not read Neo4j in the current v1 contract.

---

## Graph Model

### `Entity`

One node per tracked entity.

### `PROXIMITY_EVENT`

One edge per continuous proximity episode, keyed by:

```text
{min(a,b)}:{max(a,b)}:{episode_start_ms}
```

Episode properties include start time, latest confirmation time, minimum distance, detection location, and distance at detection.

### `KNOWN_ASSOCIATE`

Represents a pre-existing expected relationship such as same fleet or scheduled pairing.

The Correlation Worker checks this relationship **before** publishing `proximity.candidates`. Known-associate proximity may remain as graph evidence, but it is not forwarded as an unscheduled-proximity candidate.

---

## Reasoning

- Relationship metadata is first-class and naturally modeled on edges.
- Pair-history and neighborhood investigation are direct Cypher traversals.
- `MERGE` on deterministic episode identity provides replay-safe edge creation.
- The graph remains useful for operator investigation without forcing the Alert Evaluator to depend on another database for final rule interpretation.

---

## Alternatives Considered

### PostgreSQL adjacency/proximity tables — rejected

Possible for simple pair lookups, but less natural for relationship-centric investigation and future bounded traversals.

### Neptune / ArangoDB — rejected

Would add either cloud coupling or another less-familiar graph stack without improving the v1 access pattern.

---

## Consequences

- Correlation Worker performs targeted `KNOWN_ASSOCIATE` reads and `PROXIMITY_EVENT`/Entity writes.
- API performs relationship/proximity evidence reads for investigation.
- Alert Evaluator consumes already-filtered `proximity.candidates`; it does not repeat the known-associate lookup.
- Pair canonicalization is mandatory everywhere graph episode identity is used.
- Neo4j failure can delay/block creation of new proximity facts, but should not independently disable signal-loss or route-deviation rules in the Alert Evaluator.
