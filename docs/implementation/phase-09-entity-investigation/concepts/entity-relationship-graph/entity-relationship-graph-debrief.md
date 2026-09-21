# Entity Relationship Graph — Checkpoint Debrief (CP4)

Evidence for CP4, checked on 2026-09-21. See [`entity-relationship-graph.md`](entity-relationship-graph.md) for the mental model this checkpoint implements.

---

## Automated checks

- `tsc --noEmit` (api): clean.
- Full API test suite: **137/137 passing** -- the prior 132 plus 5 new for `GET /entities/:entity_id/graph`, seeding real `Entity`/`PROXIMITY_EVENT`/`KNOWN_ASSOCIATE` data via the real driver rather than depending on other phases' dev-seeded graph:
  - empty edge list (not `404`) for an id with no graph data;
  - both edge types returned with their own correct shapes (`PROXIMITY_EVENT`'s `min_distance_metres` populated and `known_associate_type` null; `KNOWN_ASSOCIATE`'s `known_associate_type` populated and `episode_start_ms` null);
  - edges ordered by `last_seen_ms` descending;
  - `404` for an operator with no saved workspace, even though real graph data exists for the entity;
  - `200` with the graph for an operator when the entity is inside their saved scope.
- Confirmed no leftover test data after the run: `MATCH (e:Entity) WHERE e.id STARTS WITH 'test-graph-' RETURN count(e)` -- real `0`.

## A wrong assumption caught before it shipped

Before writing the route, `cypher-shell` was run directly against the real dev Neo4j to confirm the planned query. Its display rendered `episode_start_ms` as `1.789238876E12`, which read as evidence the JS driver would return a lossless `Integer` wrapper needing explicit `.toNumber()` conversion -- so the first version of the route included that conversion, with a comment claiming it was "confirmed directly against real seeded data."

That claim was not actually verified against the right thing. A follow-up one-off script (`node` script importing `neo4j-driver` directly, bypassing `cypher-shell` entirely) against the same real database showed `typeof ms === 'number'` and `neo4j.isInt(ms) === false` -- the scientific notation was `cypher-shell`'s own display choice for large integers, not evidence of the driver's actual return type. The unnecessary conversion function was removed before committing; the route now does plain property access. Caught here, before the debrief was written, rather than shipped and found later.

## Real manual verification (not just the test suite)

With the real dev stack running (server PID confirmed via `lsof -iTCP:3000` at both startup and teardown):

1. Queried the real dev-seeded graph directly: 7,262 real `PROXIMITY_EVENT` edges, 0 `KNOWN_ASSOCIATE` edges (none seeded locally), confirmed via `cypher-shell` before writing any code.
2. `curl`'d `GET /entities/a75634/graph` as a real demo session against that real data: got back real neighbor ids, types, and distances, timestamps as plain numbers (`1789238876000`, not an `{low, high}` Integer object).
3. `curl`'d a totally unknown id: real `200` with `edges: []`, not a `404`.
4. Inserted a real operator with no saved workspace via `psql`, minted a real JWT: `curl`'d the same real, graph-rich `a75634` entity -- real `404`, fail-closed even though the entity plainly has real relationship data.
5. Server process confirmed stopped via `lsof` afterward; no manually-inserted rows left behind.

## What this proves, and what it doesn't yet

Proves: the API can read Neo4j correctly (both edge types, correct ordering, correct empty-vs-404 semantics), the by-id authorization guarantee now holds across all three of Redis/TimescaleDB/Neo4j identically, and a specific wrong assumption about driver behavior was caught by verifying against the actual consumer rather than trusting a plausible-looking display artifact.

Does not prove on its own: any frontend investigation UI (a later checkpoint) or multi-hop graph traversal (deliberately out of this checkpoint's scope -- "graph pivot" in US-14 is the operator re-calling this same endpoint with a neighbor's id, not the server computing several hops in one call).
