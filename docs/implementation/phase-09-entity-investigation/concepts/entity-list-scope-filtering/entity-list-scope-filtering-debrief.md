# Entity List Scope Filtering — Checkpoint Debrief (CP1)

Evidence for CP1, checked on 2026-09-21. See [`entity-list-scope-filtering.md`](entity-list-scope-filtering.md) for the mental model this checkpoint implements.

---

## Automated checks

- `tsc --noEmit` (api): clean.
- Full API test suite: **116/116 passing** — the pre-existing 109 (including `GET /entities/live`'s full suite, unmodified, now running against the refactored `scanLiveEntities`) plus 7 new for `GET /entities`:
  - 401 with no auth cookie;
  - empty list for an operator with no saved workspace (fail-closed);
  - only entities inside an operator's saved bounds and `entity_types` (Paris in, New York excluded, aircraft-only scope);
  - an entity excluded when its type isn't in the saved `entity_types`;
  - demo session filtered by an ad-hoc `bbox`, unrestricted by entity type;
  - demo session with no `bbox` returns the fully unfiltered list;
  - malformed demo `bbox` rejected with 400.

## Real manual verification (not just the test suite)

With the real dev stack running (`make up`, `make migrate`, real API process with real `.env` secrets, real Postgres/Redis) — one unrelated container from another tool (`smart-anytool-agent-postgres-1`) was squatting on Sentinel's TimescaleDB port (5433) and had to be stopped first:

1. Seeded two real `entity:live:*` Redis hashes directly via `redis-cli`: one over Paris (`lat 45, lon 2`), one over New York (`lat 40.7, lon -74`), both `entity_type=aircraft`.
2. Inserted a real `users` + `user_workspaces` row via `psql`, scope = France bounds, `entity_types: ["aircraft"]`, and minted a real JWT for that user.
3. `curl`'d the real running server as that operator: got back exactly the Paris entity, not New York.
4. `curl`'d with no cookie at all: real `401`.
5. Inserted a second real user with **no** `user_workspaces` row, minted their JWT, called the same endpoint: real `[]` — confirmed fail-closed, not fail-open.
6. Minted a real demo JWT: `bbox=40,-5,50,10` returned only Paris; no `bbox` at all returned both Paris and New York, confirming the unfiltered fallback.
7. All manually-inserted rows/hashes deleted afterward; server process stopped.

## What this proves, and what it doesn't yet

Proves: workspace-scope filtering for the entity list works correctly against real Postgres and Redis state — fail-closed with no workspace, bounds+entity_type filtering with one, and the demo bbox/unfiltered fallback — under both automated test and a live manual run against the actual running service. Also proves the `scanLiveEntities` extraction is behavior-preserving: `GET /entities/live`'s own pre-existing test suite required no changes and still passes.

Does not prove on its own: entity detail (CP2), history (CP3), or graph (CP4) — those are separate checkpoints with their own datastores and their own manual verification.
