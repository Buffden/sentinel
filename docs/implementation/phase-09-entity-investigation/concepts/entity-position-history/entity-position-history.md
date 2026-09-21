# Entity Position History -- Design and Learning Reference

Plain language first, then technical depth, then the code. Use this to understand, inspect, and defend CP3 (`GET /entities/:entity_id/history`).

---

## What this checkpoint is, and deliberately isn't

CP3 adds the third of Phase 09's four access patterns: given an entity and a concrete time window, return its chronological position track from TimescaleDB's `position_history`. This is the map/timeline data for "where was this entity during the alert window" -- US-14's first acceptance criterion.

It reuses CP2's by-id authorization decision rather than re-deriving it. It does not add relationship evidence (CP4, Neo4j) or a UI (later checkpoint) -- this is the backend read only.

---

## Concepts in plain language

### Why the time window is required, not defaulted

An investigation always has a concrete window in mind -- almost always an alert's own `detected_at` range, sent by the frontend, not guessed by the server. Defaulting to "last N hours" would silently show the wrong slice of history for an old alert and give no signal that anything was wrong. `GET /entities/live`'s required `bbox` already established this pattern for a different query dimension (space instead of time); CP3 applies the same reasoning to time.

### Why the authorization check needed to be shared, not re-derived

`GET /entities/:entity_id/history` is reachable directly, without ever calling `GET /entities/:entity_id` first -- so CP2's fail-closed by-id rule (in scope via current live state, or via being the primary entity on an in-scope alert when dark) has to be enforced here independently. Re-deriving it inline a second time would risk the two copies drifting apart under a future change. `resolveOperatorEntityAccess` (new shared helper, `shared/entityAccess.ts`) is now the single place this decision is made; both CP2's handler and CP3's were updated to call it.

### Why a dark entity's history is still visible even though its live check fails

`getLiveEntity` returns `null` for a dark entity, so the "is the live state in scope" branch of the access check can't apply. The fallback -- is this entity the primary on at least one alert the scope permits -- is exactly what makes the single most important use case for this endpoint work at all: pulling up the position track that led to a `SIGNAL_LOSS`, at the moment the entity has no current live state by definition. Manually verified: while `manual-cp3-1` was live in Paris, a France-scoped operator got `200`; once its Redis hash was deleted with no alert on record for it, the same request became `404` -- proving the fallback isn't just theoretical, its absence is directly observable.

### Why the response doesn't filter each historical point by the scope's bounds

The scope's `geo_region.bounds` answers "can this operator investigate this entity at all," decided once via the shared access check. Filtering individual history points by the same bounds would fragment a flight path into disconnected segments whenever it happened to cross the box edge -- exactly the kind of investigation-defeating behavior US-14 is trying to avoid. Once an entity passes the access check, its full requested window is returned.

### Why the row cap is independent of the time window's width

A wide window over a fast-reporting entity could return an unbounded number of rows regardless of how the operator phrased the request. `ENTITY_HISTORY_MAX_POINTS` (config, default 1000) caps the response the same way `LIVE_ENTITIES_MAX` already caps `GET /entities`/`GET /entities/live` -- a named constant, not a hardcoded number, and independent of any other limit in the service.

---

## Map to code

| Concept | Where it lives |
| --- | --- |
| Shared by-id authorization, now used by two endpoints | `services/api/src/shared/entityAccess.ts` (`resolveOperatorEntityAccess`) -- CP2's handler was refactored to call this instead of its own inline copy |
| `GET /entities/:entity_id/history` route: required window validation, access check, time-range query | `services/api/src/routes/entities.ts` |
| Row cap | `services/api/src/config.ts` (`ENTITY_HISTORY_MAX_POINTS`) |
| Existing index making the query a range scan, not a table scan | `infra/migrations/002_position_history.sql` (`position_history_entity_time_idx` on `(entity_id, observed_at DESC)`) |
| Proof against real Postgres, including the dark-entity fallback | `services/api/src/routes/entities.integration.test.ts` (`GET /entities/:entity_id/history` describe block) |

---

## Retention questions

1. Why does this endpoint require `from_ms`/`to_ms` instead of defaulting to a recent window?
2. Why was CP2's own scope-check code refactored as part of this checkpoint, instead of leaving it alone and writing similar code for history?
3. Walk through why an entity that fails its live-state scope check can still return history, and under exactly what condition.
4. Why doesn't the response filter individual history points by the operator's saved bounds, only the entity as a whole?
5. What did `EXPLAIN ANALYZE` actually show for this query, and what does "Chunks excluded during startup" mean for a hypertable?

---

## Completion checklist

- [ ] I can explain why the time window has no default, unlike some other filters in this service
- [ ] I can trace the shared access-check function and name both endpoints that now call it
- [ ] I can explain the dark-entity fallback and reproduce the live-vs-dark scope-check difference myself against a real seeded entity
- [ ] I have run `EXPLAIN ANALYZE` on the real query myself and can read what it says about index usage and chunk exclusion
- [ ] I can explain why the row cap exists independently of the time window's width
