# Entity Relationship Graph — Design and Learning Reference

Plain language first, then technical depth, then the code. Use this to understand, inspect, and defend CP4 (`GET /entities/:entity_id/graph`).

---

## What this checkpoint is, and deliberately isn't

CP4 adds the fourth and last of Phase 09's four entity access patterns, and the first time the API service reads Neo4j at all. Given an entity, it returns its 1-hop relationship neighborhood -- every `PROXIMITY_EVENT` episode and `KNOWN_ASSOCIATE` relationship on record. This completes the three-store investigation trilogy: Redis answers "where is it now" (CP1/CP2), TimescaleDB answers "where was it" (CP3), Neo4j answers "who did it interact with" (CP4).

It does not implement multi-hop traversal (US-14's "graph pivot" flow: an operator clicking a related entity to open *its own* investigation view is the mechanism for going further, not a single multi-hop query) and does not add any new write path -- Correlation Worker remains the only writer to this graph, per ADR-003.

---

## Concepts in plain language

### Why the API needed its own Neo4j driver instance, not the Correlation Worker's

Ownership: per ADR-003, the Correlation Worker performs runtime relationship/evidence writes, the API performs investigation reads. These are two different services, two different processes, so a new `neo4j.ts` (module-level driver, mirroring `db.ts`'s pool and `redis.ts`'s client) was needed. One difference in usage pattern is worth noting: the Correlation Worker opens one long-lived session for its entire process lifetime (it's a single Kafka consumer, one logical stream of work); the API opens a new session per request, since concurrent HTTP requests must not share one session's internal serialization.

### Why the first assumption about the driver's return types was wrong, and how that was caught

Before writing any route code, `cypher-shell` was used directly against the real dev Neo4j (per the project's "interact with unfamiliar infrastructure before wrapping it" rule) to confirm the query shape works. Its console display rendered `episode_start_ms` in scientific notation (`1.789238876E12`), which looked like evidence the JS driver would return a lossless `Integer` wrapper object rather than a plain number -- a real, specific, testable claim, not a vague guess. It was wrong: a direct one-off script against the real driver (bypassing `cypher-shell`'s own display formatting entirely) showed `typeof ms === 'number'` and `neo4j.isInt(ms) === false`. The scientific notation was `cypher-shell`'s own rendering choice for large integers, not evidence of the driver's actual return type. The code was written once, correctly, based on the verified fact rather than the plausible-looking first guess -- a concrete example of why "confirmed directly" has to mean confirmed against the actual consumer (the JS driver), not just the nearest tool that happens to show the data (`cypher-shell`).

### Why the by-id access check applies but can't filter individual neighbors

Same fail-closed rule CP2 and CP3 established, applied a third time: `GET /entities/:entity_id/graph` is reachable directly, so it re-checks `resolveOperatorEntityAccess` rather than trusting that the caller already went through the entity-list or entity-detail endpoints. What's different this time: Neo4j's `Entity` node stores no geography at all (`id`, `type`, optional `name` -- per ADR-003 and `DATA_MODEL.md`). Once the *primary* entity passes the scope check, the neighbors in the response are not separately filtered by the operator's bounds, because there is nothing geographic on them to filter against without an extra Redis/Postgres lookup per neighbor. This is the same "scope gates the entity, not sub-resources" trade-off CP3 made for history's individual points, restated here because this store has genuinely less data available to filter with, not because the reasoning changed.

### Why a dark or graph-empty entity returns a valid empty list, not a 404

An entity that exists (has live state, or alert history) but has never had a `PROXIMITY_EVENT` or `KNOWN_ASSOCIATE` edge is a completely normal, common case -- most tracked entities probably have no recorded relationships. `{ entity_id, edges: [] }` at `200` is correct; `404` stays reserved for "outside your scope" (operator) exactly as the other by-id endpoints already established. Demo sessions get no existence check at all, matching `GET /entities/:entity_id/history`'s already-documented design choice.

---

## Map to code

| Concept | Where it lives |
| --- | --- |
| Module-level Neo4j driver, one per process | `services/api/src/neo4j.ts` (`neo4jDriver`) |
| `GET /entities/:entity_id/graph` route: by-id access check, per-request session, Cypher query, response mapping | `services/api/src/routes/entities.ts` |
| Reused by-id authorization, now used by three endpoints | `services/api/src/shared/entityAccess.ts` (`resolveOperatorEntityAccess`) |
| Graph edge cap | `services/api/src/config.ts` (`ENTITY_GRAPH_MAX_EDGES`) |
| The write side this checkpoint reads, unchanged | `services/correlation-worker/src/proximityEvent.ts`, `services/correlation-worker/src/knownAssociate.ts` |
| Proof against real Neo4j, including both edge types and the scope-enforcement fix | `services/api/src/routes/entities.integration.test.ts` (`GET /entities/:entity_id/graph` describe block) |

---

## Retention questions

1. Why does the API open a new Neo4j session per request instead of reusing one long-lived session the way the Correlation Worker does?
2. What did the direct probe script prove that `cypher-shell`'s own output didn't, and why did that distinction matter here?
3. Why can't the graph response filter individual neighbors by the operator's saved geographic bounds, unlike (in spirit) what `GET /entities` does for its own list?
4. Why is an empty `edges` array a valid `200` rather than a `404`, and under what condition does this endpoint still return `404`?
5. What does ADR-003 say the Alert Evaluator's relationship to Neo4j is, and how does that differ from the API's?

---

## Completion checklist

- [ ] I can explain why this checkpoint needed its own Neo4j driver file instead of reusing the Correlation Worker's
- [ ] I can describe the wrong assumption about driver return types, how it was caught, and why the catch method mattered
- [ ] I can explain the by-id access check's trade-off for this store specifically, in my own words, not just recite CP3's
- [ ] I can distinguish the two ways this endpoint can respond to a nonexistent-or-empty entity vs. an out-of-scope one
- [ ] I have run a real Cypher query against the dev Neo4j myself and compared it to the endpoint's own output
