# Entity List Scope Filtering — Design and Learning Reference

Plain language first, then technical depth, then the code. Use this to understand, inspect, and defend CP1 (`GET /entities`).

---

## What this checkpoint is, and deliberately isn't

CP1 adds a new, separate `GET /entities` endpoint that returns the operator's currently-visible live entities, filtered by their saved workspace scope — the same "no saved workspace, no data" contract ADR-012 already established for `GET /alerts`. It does not touch `GET /entities/live?bbox=...`, which stays exactly what it always was: an unscoped, viewport-driven query the map widget uses to render whatever the operator is currently looking at, regardless of their saved scope.

This checkpoint does not add entity detail, history, or graph endpoints — those are CP2–CP4.

---

## Concepts in plain language

### Why this isn't just `/entities/live` with a filter bolted on

`/entities/live` answers "what's in this map viewport right now" — a UI concern, not an authorization concern. `/entities` answers "what is this operator allowed to see" — the same question `GET /alerts` already answers, just for the entity list instead of the alert list. Conflating them would mean the map's viewport query silently inherits workspace-scope semantics it was never designed around (and that operators reasonably expect to pan past — the workspace scope, unlike the map viewport, is not something they adjust every few seconds).

### Why `matchesScope` (from `alertScopeFilter.ts`) couldn't be reused directly

`matchesScope` extracts a position from an *alert's payload*, keyed by `alert_type` — `SIGNAL_LOSS` reads `payload.last_known_lat/lon`, `UNSCHEDULED_PROXIMITY` reads flat `payload.lat/lon`, `COMPOSITE` reads a nested `payload.proximity.lat/lon`. A live entity has none of that: its position is already sitting directly on the Redis hash as `lat`/`lon`, and there is no `alert_type`-equivalent dimension to check for a plain entity (no `alert_types` filter applies to an entity list). Duplicating the position-extraction *and* the bounds math would have been wrong; duplicating only the bounds math would have left two independently-maintained copies of the same four-line rectangle check. `withinBounds` was pulled out into `regions.ts` (the existing home of `GeoBounds`) as the one shared definition; `alertScopeFilter.ts` was updated to import it instead of keeping its own private copy, and `entityScopeFilter.ts` is a new, entity-shaped predicate that uses the same shared bounds check but never touches alert-specific concepts.

### Why the Redis scan itself was extracted, not copied a second time

`GET /entities/live` already had a full `SCAN entity:live:*` → parse → filter → cap loop. `GET /entities` needs the identical scan and parse step, with a different inclusion predicate (workspace scope vs. viewport bbox) and a different mandatory-ness of the bbox/scope input. Rather than hand-copy the loop, `scanLiveEntities(predicate)` in `shared/liveEntities.ts` now owns the scan/parse/cap mechanics once, and both routes supply their own predicate. `GET /entities/live`'s behavior is unchanged — same fields, same staleness cutoff, same cap — verified by its existing test suite passing unmodified against the refactored implementation.

### Why "no saved workspace" returns `[]` instead of an unfiltered list

An operator's workspace scope is how Sentinel enforces "you only see what you're supposed to see" at the server, per ADR-012. If a missing scope defaulted to unfiltered, a new operator would briefly see every tracked entity worldwide before setting up a scope — the exact alert-flood problem ADR-012 was written to prevent, just on the entity list instead of the alert list. Fail-closed (empty, not everything) is the same choice `GET /alerts` already made.

### Why demo sessions still get a bbox fallback, not a workspace lookup

A demo JWT (`role: 'demo'`) has no `users` row and can't hold a `user_workspaces` row — this is unchanged from ADR-012. `GET /entities` reuses the identical "ad-hoc `bbox` query param filters geography only, no `bbox` means fully unfiltered" rule `GET /alerts` already uses for demo, for the same reason: it's one shared predicate function, just fed a request parameter instead of a persisted row.

---

## Map to code

| Concept | Where it lives |
| --- | --- |
| Shared Redis scan/parse/cap mechanics, reused by both entity endpoints | `services/api/src/shared/liveEntities.ts` (`scanLiveEntities`, `LiveEntity`) |
| Shared bounds-comparison, reused by both scope filters | `services/api/src/shared/regions.ts` (`withinBounds`) |
| Entity-shaped scope predicate (bounds + `entity_types`, no `alert_types`) | `services/api/src/shared/entityScopeFilter.ts` (`matchesEntityScope`) |
| New `GET /entities` route: operator fail-closed lookup, demo bbox/unfiltered fallback | `services/api/src/routes/entities.ts` |
| Unchanged-behavior `GET /entities/live`, now built on the shared scan | `services/api/src/routes/entitiesLive.ts` |
| HTTP-level proof against real Postgres + Redis | `services/api/src/routes/entities.integration.test.ts` |

---

## Retention questions

1. Why does `GET /entities` exist as a separate route instead of adding scope filtering to `GET /entities/live`?
2. Why couldn't `entityScopeFilter.ts` just call `alertScopeFilter.ts`'s `matchesScope` with a fake alert-shaped object?
3. What's the one piece of logic that actually is shared between the alert and entity scope filters, and where does it live now?
4. What does an operator with no saved workspace see when they call `GET /entities`, and why is that the correct behavior rather than a bug?
5. If `GET /entities/live`'s tests still pass unmodified after this checkpoint, what does that prove about the refactor?

---

## Completion checklist

- [ ] I can explain why `/entities` and `/entities/live` are two different routes with two different authorization models
- [ ] I can explain why a live entity's position never needs "extraction" the way an alert's does
- [ ] I can point to the one function shared between alert and entity scope filtering, and the two that aren't
- [ ] I can explain the fail-closed rule for a missing workspace, and why it mirrors `GET /alerts`
- [ ] I have run the real dev server myself and confirmed the France/NYC scoping example against real Redis and Postgres state, not just the test suite
