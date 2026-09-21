# Entity Position History — Checkpoint Debrief (CP3)

Evidence for CP3, checked on 2026-09-21. See [`entity-position-history.md`](entity-position-history.md) for the mental model this checkpoint implements.

---

## Automated checks

- `tsc --noEmit` (api): clean.
- Full API test suite: **131/131 passing** — the prior 124 (CP2's handler unchanged in behavior after being refactored onto the new shared `resolveOperatorEntityAccess`) plus 7 new for `GET /entities/:entity_id/history`:
  - 400 for missing `from_ms`/`to_ms`;
  - 400 for `from_ms` greater than `to_ms`;
  - points inside the window returned ascending, points outside excluded;
  - response capped at `ENTITY_HISTORY_MAX_POINTS` regardless of how many rows exist in the window;
  - 404 for an operator with no saved workspace;
  - 404 for an operator when the entity's live position is outside their saved bounds;
  - 200 for a dark entity (no Redis state) when it's the primary on an in-scope alert.

## Real manual verification (not just the test suite)

With the real dev stack running (server PID confirmed via `lsof -iTCP:3000` at both startup and teardown, learned the hard way in CP2):

1. Seeded 500 real `position_history` rows for one entity via `psql`, then ran the exact query shape the route executes through `EXPLAIN ANALYZE`. Real output: `Index Scan Backward using ..._position_history_entity_time_idx`, plus `Chunks excluded during startup: 3` from TimescaleDB's `ChunkAppend` — confirms the time-range query is a real index range scan with chunk pruning, not a table scan, satisfying US-14's own acceptance criterion.
2. Seeded three real position rows for `manual-cp3-1` (Paris) spanning a 10-minute range and a real live Redis hash. `curl`'d a 5-minute window as a real demo session: got back exactly the 2 in-range points, ascending, the 10-minute-out point excluded.
3. `curl`'d with no `from_ms`/`to_ms`: real `400`.
4. Inserted a real France-scoped operator via `psql`, minted a real JWT: `curl`'d the same entity's history while it was still live in Redis — real `200` with both points.
5. Deleted the entity's Redis hash (simulating it going dark) with no alert on record for it at all: same request, same operator, same entity, real `404` — confirms the fallback path's absence is directly observable, not just asserted by the test suite.
6. All manually-inserted rows/hashes deleted afterward; server process confirmed stopped via `lsof`.

## What this proves, and what it doesn't yet

Proves: the time-windowed query is a real index range scan with chunk exclusion, the required-window validation and row cap work, and the shared by-id authorization check (introduced this checkpoint, also used by CP2) correctly gates access to history independently of whether the entity-detail endpoint was ever called.

Does not prove on its own: relationship evidence (CP4, Neo4j) — a separate datastore and a separate checkpoint, per the phase's own learning goal of matching each access pattern to the store that actually answers it.
