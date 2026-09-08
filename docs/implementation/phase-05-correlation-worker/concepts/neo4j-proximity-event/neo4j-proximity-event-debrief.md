# Neo4j Proximity Event Debrief

---

## Setup

```bash
make up
make neo4j-schema   # applies Entity.id and PROXIMITY_EVENT.idempotency_key constraints
cd services/correlation-worker
pnpm install
```

Confirmed both constraints already existed from Phase 01:

```text
id, name, type, entityType, labelsOrTypes, properties
4, "entity_id_unique", "UNIQUENESS", "NODE", ["Entity"], ["id"]
6, "proximity_event_idempotency_key_unique", "RELATIONSHIP_UNIQUENESS", "RELATIONSHIP", ["PROXIMITY_EVENT"], ["idempotency_key"]
```

---

## Experiment 1: direct `cypher-shell` exploration, before writing service code

**Idempotent node creation** — `MERGE (a:Entity {id: ...})` run twice: `count(a)` stayed 1.

**Idempotent same-direction edge MERGE:**

```text
MERGE (a)-[r:PROXIMITY_EVENT {idempotency_key: K}]->(b) ON CREATE SET ... ON MATCH SET r.last_seen_ms = ...
```

Run twice: first call creates (`last_seen_ms = 1700000000000`), second call matches and updates (`last_seen_ms = 1700000009000`). `count(r)` stayed 1 both times.

**The critical failure mode** — same `idempotency_key`, reversed node direction:

```text
MERGE (b)-[r:PROXIMITY_EVENT {idempotency_key: K}]->(a)
```

```text
Relationship(0) already exists with type `PROXIMITY_EVENT` and property `idempotency_key` = '...'
```

`count(r)` after the failed attempt: still 1 — the failed write didn't corrupt anything, but it wasn't a silent no-op either. This is the concrete reason node order must be canonicalized in application code, not just an abstract concern.

**`KNOWN_ASSOCIATE` undirected read** — created `(a)-[:KNOWN_ASSOCIATE]->(b)`, then queried `(b)-[k:KNOWN_ASSOCIATE]-(a)` (opposite order, undirected pattern): found it. Direction only matters for the MERGE write path, not for existence reads.

| Check | Expected | Observed |
| --- | --- | --- |
| Same-direction MERGE run twice | 1 edge, `ON MATCH` fires | PASS |
| Reversed-direction MERGE, same key | Constraint violation, no corruption | PASS |
| Undirected `KNOWN_ASSOCIATE` read | Finds edge regardless of direction/order | PASS |

---

## Experiment 2: `mergeProximityEvent` against real Neo4j

Six integration tests:

```text
Test Files  1 passed (1)
     Tests  6 passed (6)
```

(Full suite: 21/21 across all checkpoints so far.)

| Test | Proves |
| --- | --- |
| creates one edge with the initial detection properties | Baseline creation works, all fields set correctly |
| refreshes `last_seen_ms` on a repeat call without duplicating | `ON MATCH` path works, one edge |
| produces the same edge regardless of which entity is passed first | The function's own canonicalization prevents the reversed-direction failure from Experiment 1 |
| tightens `min_distance_metres` but never lets it increase | The `CASE` comparison behaves correctly in both directions |
| creates a separate edge for a different `episode_start_ms` on the same pair | Episode identity includes time, not just the pair |
| sets `Entity.type` only on first creation | `ON CREATE SET` doesn't refire on match |

One implementation detail learned directly rather than assumed: this driver version returns small integer properties (`episode_start_ms`, `last_seen_ms`) as plain JS numbers, not wrapped `Integer` objects — the first test run failed on `.toNumber is not a function` until this was corrected, which is itself evidence this wasn't assumed but actually observed.

Cleanup verified: `MATCH (e:Entity) WHERE e.id STARTS WITH 'test-proximity-'` returned 0 after the suite ran.

---

## Engineering debrief

**Data flow:** given two entities and detection data (episode start, last seen, distance, location), `mergeProximityEvent` canonicalizes node order by comparing `entity.id` strings, builds `idempotency_key` from the canonical pair key plus `episode_start_ms`, then issues one `MERGE` that creates-or-matches both `Entity` nodes and the `PROXIMITY_EVENT` edge between them in a single write transaction.

**Trade-off:** `min_distance_metres` is tracked with an inline `CASE` comparison rather than a separate read-then-write — keeps the whole operation atomic in one Cypher statement instead of a read/compare/write cycle vulnerable to a race between concurrent writers.

**Failure behaviour:** the one real hazard here is direction, and it's now handled inside the function rather than left to callers — verified by a test that deliberately swaps entity argument order between two calls for the same episode and confirms exactly one edge results, not a crash.

## Manual inspection commands

```bash
# Inspect a proximity edge directly
docker exec sentinel-neo4j cypher-shell -u neo4j -p sentinel-dev \
  "MATCH (a:Entity)-[r:PROXIMITY_EVENT]->(b:Entity) RETURN a.id, b.id, r.idempotency_key, r.min_distance_metres, r.last_seen_ms"

# Run the proximity-event integration suite
cd services/correlation-worker && node_modules/.bin/vitest run src/proximityEvent.integration.test.ts
```

## Knowledge-check questions

1. What exact error does Neo4j throw when the same `idempotency_key` is MERGEd in the reversed direction, and why does that happen given the uniqueness constraint is only on the property?
2. Why doesn't `KNOWN_ASSOCIATE` existence-checking need the same canonicalization that `PROXIMITY_EVENT` writes do?
3. Why is `min_distance_metres` updated with a `CASE` comparison instead of always overwritten with the latest observation?
4. What does `mergeProximityEvent` deliberately not decide, and which future piece of the system decides it instead?

## Next

Redis `proximity-episode:{pair_key}` state with TTL-based gap detection — this is what decides whether a given ping is a continuation of the episode just written here, or the start of a new one, before calling `mergeProximityEvent` again.

---

## Key observations

| Concept | Observed |
| --- | --- |
| Same-direction MERGE, run twice | 1 edge, `ON MATCH` updates `last_seen_ms` |
| Reversed-direction MERGE, same key | Constraint violation; no corruption |
| Undirected `KNOWN_ASSOCIATE` read | Finds edge regardless of direction |
| `mergeProximityEvent` canonicalizes order itself | Confirmed — swapped-argument test produces 1 edge, not an error |
| `min_distance_metres` | Only ever decreases across repeat calls |
| Small integer properties from this driver version | Returned as plain JS numbers, not `Integer` objects — learned from a failing assertion, not assumed |
| Test cleanup | PASS — 0 leftover test nodes |
