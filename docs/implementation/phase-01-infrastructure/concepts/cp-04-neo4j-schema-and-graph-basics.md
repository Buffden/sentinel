# Neo4j Schema and Graph Basics

Concepts exercised in Checkpoint 4. This is not a full Neo4j or Cypher reference.

---

## Graph primitives

**Node** -- a data entity in the graph. In Sentinel there is one node per tracked entity.

**Label** -- a type tag on a node. Labels are analogous to a table name in relational terms, though a node can carry multiple labels. Sentinel uses one label: `Entity`.

**Property** -- a named value stored on a node or relationship. Properties are not typed at the schema level unless a constraint enforces it. Sentinel's canonical `Entity` properties are `id`, `type`, and the optional `name`.

**Relationship** -- a directed, named connection between two nodes. Unlike foreign keys, relationships are first-class objects in the graph and can carry their own properties. Direction is stored but traversal queries may ignore it.

**Relationship type** -- the name of a relationship kind. Sentinel defines two: `PROXIMITY_EVENT` and `KNOWN_ASSOCIATE`.

---

## Why `Entity.id` is unique

Each `Entity` node represents one real-world tracked object (aircraft or vessel) identified by its canonical `id` (ICAO hex, MMSI, or synthetic ID). There must be exactly one node per identity so that:

- relationship edges (`PROXIMITY_EVENT`, `KNOWN_ASSOCIATE`) connect to a single canonical node;
- `MERGE` in the Correlation Worker can find-or-create the right node efficiently and deterministically.

Without the uniqueness constraint, concurrent workers could create duplicate nodes for the same entity, producing a fragmented graph where relationship queries would miss evidence.

---

## Constraint vs index

**Index** -- a data structure that speeds up lookup by a property. It does not prevent duplicates.

**Uniqueness constraint** -- enforces that no two nodes with the same label can share the same property value. Neo4j automatically creates a backing index for every uniqueness constraint. Adding a separate index on the same property would be redundant.

```cypher
-- What Checkpoint 4 creates:
CREATE CONSTRAINT entity_id_unique IF NOT EXISTS
  FOR (e:Entity)
  REQUIRE e.id IS UNIQUE;

-- What Neo4j creates automatically as a side effect:
-- RANGE index on Entity.id named entity_id_unique
```

`IF NOT EXISTS` makes the statement idempotent: re-running the schema file succeeds without duplicating the constraint.

---

## SHOW CONSTRAINTS / SHOW INDEXES

After `make neo4j-schema`:

```
SHOW CONSTRAINTS;
-- entity_id_unique  UNIQUENESS  NODE  Entity  [id]

SHOW INDEXES;
-- entity_id_unique  RANGE   NODE  Entity  [id]  <-- backing index from constraint
-- index_343aff4e    LOOKUP  NODE  ...           <-- built-in token index
-- index_f7700477    LOOKUP  RELATIONSHIP  ...   <-- built-in token index
```

The two LOOKUP indexes are system-managed and always present. They are not created by the schema file.

---

## MATCH, CREATE, MERGE at a conceptual level

**MATCH** -- find existing nodes or relationships that satisfy a pattern. Returns nothing if no match.

**CREATE** -- unconditionally write a new node or relationship. Does not check for existing data (except uniqueness constraints, which will reject a duplicate).

**MERGE** -- find the pattern if it exists; create it if it does not. This is the idempotent write primitive. The Correlation Worker uses `MERGE` to write `Entity` nodes and `PROXIMITY_EVENT` edges safely under at-least-once Kafka delivery.

---

## Community Edition limit: relationship property uniqueness

Neo4j Community Edition supports uniqueness constraints on node properties only. It does NOT support uniqueness constraints on relationship properties.

This means:

**Entity identity:**
```
enforced by Neo4j uniqueness constraint
```

**PROXIMITY_EVENT episode identity (`{pair_key}:{episode_start_ms}`):**
```
enforced by deterministic application MERGE in the Correlation Worker (Phase 05)
-- not by a database schema constraint
```

The Correlation Worker writes PROXIMITY_EVENT edges with `MERGE` on the node pattern and relies on the deterministic `idempotency_key` value to avoid creating duplicate edges for the same episode. This is an application-level guarantee, not a database-level constraint. At-least-once Kafka redelivery is safe because the same `MERGE` on the same pattern produces the same result.

---

## Deferred

These Neo4j topics are intentionally not covered in this checkpoint:

- Cypher traversal patterns and variable-length paths
- graph algorithm libraries (APOC, GDS)
- application Neo4j driver (Phase 05)
- transaction retry logic in application code
- Correlation Worker `MERGE` write sequence and Redis interaction
- `KNOWN_ASSOCIATE` seeding and read pattern
- API investigation read queries
- Neo4j clustering and causal consistency
- production Neo4j sizing and tuning
- multi-tenant graph isolation
