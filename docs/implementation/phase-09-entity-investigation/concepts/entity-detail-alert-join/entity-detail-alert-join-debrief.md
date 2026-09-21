# Entity Detail + Alert Join — Checkpoint Debrief (CP2)

Evidence for CP2, checked on 2026-09-21. See [`entity-detail-alert-join.md`](entity-detail-alert-join.md) for the mental model this checkpoint implements.

---

## Automated checks

- `tsc --noEmit` (api): clean.
- Full API test suite: **124/124 passing** — the prior 116 plus 8 new for `GET /entities/:entity_id`:
  - 404 for an id with no live state and no alerts;
  - live state plus alerts where the entity is primary or counterparty, joined correctly;
  - `entity: null` with populated alerts for a dark entity with no Redis state (not a 404);
  - 404 for an operator with no saved workspace, even when the entity exists;
  - 404 for an operator when the entity's live position is outside their saved bounds;
  - 200 with the entity and its in-scope alerts for an operator when it's inside their saved scope;
  - two route-mount-order regression tests, mounting `entitiesRouter` and `entitiesLiveRouter` together exactly as `index.ts` does, proving `/entities/live` isn't swallowed by the new `/:entity_id` route.

## A real bug this checkpoint's own design would have reintroduced, caught before it shipped

Adding `GET /:entity_id` to `entitiesRouter` created a genuine routing collision: with the mount order `/entities` before `/entities/live` (as CP1 left it), a request to `/entities/live` would match `GET /:entity_id` with `entity_id="live"`, and `entitiesLiveRouter` would never run. Caught by re-reading the route table before running anything, not by a failing test — fixed by mounting `/entities/live` before `/entities` in `index.ts`, with a comment explaining why the order matters. The two mount-order tests above exist so this can't silently regress if a future checkpoint reorders the mounts again.

## Real manual verification (not just the test suite)

With the real dev stack running (real API process, real Postgres/Redis) — this time the server's PID was confirmed via `lsof -iTCP:3000` both at startup and at teardown, after CP1's cleanup left a server process running unnoticed for over an hour and caused two unrelated Kafka consumer-group test failures (`alertSink.integration.test.ts` saw duplicate deliveries from the second live consumer in the `api` group) until it was found and killed:

1. Seeded a real live entity (`manual-cp2-1`, Paris) via `redis-cli`, a `SIGNAL_LOSS` alert with `manual-cp2-1` as `entity_id`, and an `UNSCHEDULED_PROXIMITY` alert with it as `counterparty_entity_id`, via `psql`.
2. `curl`'d as a real demo session: got back the live entity plus both alerts (own and as counterparty) correctly joined.
3. Deleted the Redis hash, left the alert rows: `curl`'d the counterparty entity's own id — got `entity: null` with its alert history intact, `200` not `404`.
4. `curl`'d a totally made-up id: real `404`.
5. Inserted a real France-scoped operator (`entity_types: ["aircraft"]`, `alert_types: ["SIGNAL_LOSS"]`) and a real operator with no saved workspace, both via `psql`, minted real JWTs for each.
6. France-scoped operator on the Paris entity: `200` (in scope).
7. Same operator, alerts with an empty test payload (no embedded position): both alerts correctly excluded from the returned list — confirmed this is `matchesScope`'s existing "no position, no inclusion" rule working as designed, not a bug, by fixing the test payloads to carry real positions and re-running: the `SIGNAL_LOSS` alert (in `alert_types`) then appeared, the `UNSCHEDULED_PROXIMITY` one (not in `alert_types`) stayed excluded.
8. Operator with no saved workspace on the same Paris entity: real `404`, fail-closed.
9. All manually-inserted rows/hashes deleted afterward; server process confirmed stopped via `lsof`, not just by pattern-matching `pkill`.

## What this proves, and what it doesn't yet

Proves: the live-state/alert join, the dark-entity `200` vs. truly-missing `404` distinction, and the by-id scope-enforcement fix (including the counterparty-exclusion and per-alert `matchesScope` filtering) all work correctly against real infrastructure, not mocks — plus the route-mount-order fix is now regression-tested, not just fixed once.

Does not prove on its own: position history (CP3) or graph evidence (CP4) — separate datastores, separate checkpoints.
