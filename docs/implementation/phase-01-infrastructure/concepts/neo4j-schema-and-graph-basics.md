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

**Uniqueness constraint** -- enforces that no two nodes of the same label (or relationships of the same type) can share the same property value. Neo4j automatically creates a backing index for every uniqueness constraint. Adding a separate index on the same property would be redundant.

`IF NOT EXISTS` makes each statement idempotent: re-running the schema file succeeds without duplicating the constraint.

---

## Checkpoint 4 schema

See [`infra/neo4j/schema.cypher`](../../../../infra/neo4j/schema.cypher). Applied by `make neo4j-schema`.

Neo4j 5.7+ Community Edition supports uniqueness constraints on both node and relationship properties. Both constraints are database-enforced.

---

## SHOW CONSTRAINTS / SHOW INDEXES

After `make neo4j-schema`:

### Constraints

| Name | Type | Target | Label | Properties |
| --- | --- | --- | --- | --- |
| entity_id_unique | UNIQUENESS | NODE | Entity | [id] |
| proximity_event_idempotency_key_unique | RELATIONSHIP_UNIQUENESS | RELATIONSHIP | PROXIMITY_EVENT | [idempotency_key] |

### Indexes

| Name | Type | Target | Label | Properties | Notes |
| --- | --- | --- | --- | --- | --- |
| entity_id_unique | RANGE | NODE | Entity | [id] | backing index from constraint |
| proximity_event_idempotency_key_unique | RANGE | RELATIONSHIP | PROXIMITY_EVENT | [idempotency_key] | backing index from constraint |
| index_343aff4e | LOOKUP | NODE | — | — | system-managed token index |
| index_f7700477 | LOOKUP | RELATIONSHIP | — | — | system-managed token index |

The two LOOKUP indexes are system-managed and always present. They are not created by the schema file.

---

## MATCH, CREATE, MERGE at a conceptual level

**MATCH** -- find existing nodes or relationships that satisfy a pattern. Returns nothing if no match.

**CREATE** -- unconditionally write a new node or relationship. Uniqueness constraints will reject a duplicate property value; without a constraint, CREATE produces a duplicate.

**MERGE** -- find the pattern if it exists; create it if it does not. This is the idempotent write primitive. The Correlation Worker uses `MERGE` to write `Entity` nodes and `PROXIMITY_EVENT` edges safely under at-least-once Kafka delivery.

---

## Database constraint vs application MERGE: related but different

Both the uniqueness constraint on `PROXIMITY_EVENT.idempotency_key` and the Correlation Worker's use of `MERGE` are necessary. They solve different problems:

**The uniqueness constraint** prevents two concurrent writers from inserting duplicate edges at the storage level. Even if two Correlation Worker instances raced, the second write would fail with a constraint violation rather than silently creating a duplicate.

**MERGE** is the application's replay-safe find-or-create operation. When the Kafka offset is not committed and the same `position.normalized` event is redelivered, `MERGE` finds the already-created edge and updates its properties rather than attempting to create a new one. `CREATE` would hit the constraint and fail; `MERGE` succeeds.

Summary:

```text
Uniqueness constraint  ->  prevents duplicates at storage level, catches concurrent race conditions
MERGE                  ->  replay-safe find-or-create, correct under at-least-once redelivery
```

Both are required for correct behavior.

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
