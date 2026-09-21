# Entity Detail + Alert Join — Design and Learning Reference

Plain language first, then technical depth, then the code. Use this to understand, inspect, and defend CP2 (`GET /entities/:entity_id`).

---

## What this checkpoint is, and deliberately isn't

CP2 adds a by-id lookup that joins one entity's current Redis live state with its recent alert history from Postgres — the mechanism US-14 calls "resolve a raw entity_id into something investigable." It is the endpoint any UI element showing a bare `entity_id` (an alert counterparty, a composite child not locally loaded, a future graph-pivot node) can call to get a human-readable identity and evidence trail.

It does not add position history (CP3, TimescaleDB) or relationship evidence (CP4, Neo4j) — those are separate datastores answering separate questions, per the phase's own learning goal.

---

## Concepts in plain language

### Why the staleness cutoff from CP1's list scan doesn't apply here

`scanLiveEntities` (CP1) drops any entity whose `last_seen_ms` is older than `LIVE_ENTITY_STALE_AFTER_MS` — correct for a live list, where a stale entity is just clutter. A by-id lookup is different: an operator investigating a `SIGNAL_LOSS` alert is *specifically* looking at an entity whose staleness is the anomaly. Hiding its last known state because it's stale would defeat the endpoint's purpose. `getLiveEntity` (new in `shared/liveEntities.ts`) shares the same hash-parsing logic as the scan (`hashToLiveEntity`) but skips the cutoff entirely — it returns whatever Redis last recorded, however old, or `null` only if there's truly no position on the hash.

### Why a dark entity (no Redis state) is a valid `200`, not a `404`

If `entity: null` always meant "not found," an operator investigating exactly the case this endpoint exists for — an entity that's gone dark — would get an error instead of the alert history that explains why. `404` is reserved for "nothing at all exists for this id" (no live state *and* no alert history) or "this id is outside your scope." Everything else returns `200` with whatever combination of `entity`/`alerts` actually exists.

### Why this is the checkpoint that had to close the enumeration gap CP1 didn't have

CP1's `GET /entities` only ever returns what's already inside the operator's scope — there's no way to ask it about an entity outside that scope, because it never emits an id it wasn't already going to include. `GET /entities/:entity_id` is different: it accepts *any* string. Without a check, an operator could enumerate `entity_id`s (or just guess a real one from another system) and read live state and alert evidence their saved workspace was supposed to hide, defeating ADR-012 entirely through a side door CP1 never opened. The fix generalizes CP1's own fail-closed rule to a direct-lookup path: the entity is only visible if its live position/type passes `matchesEntityScope`, or — when it's gone dark and there's no live state to check — if it's the *primary* entity (not just a counterparty) on at least one alert that itself passes `matchesScope`. A `404`, not `403`, is returned either way, so the response itself never confirms an out-of-scope entity exists.

### Why a counterparty-only match doesn't establish scope on its own

If entity A (in scope) has an `UNSCHEDULED_PROXIMITY` alert against entity B (outside scope), B still shows up in that alert row as `counterparty_entity_id`. But treating that as "B is visible" would let an operator discover out-of-scope entities just by them being someone else's counterparty — a smaller version of the same enumeration problem. The in-scope check only counts an alert where the *primary* `entity_id` matches, never a counterparty match.

### Why the returned alert list itself is filtered through `matchesScope`, not just the entity

Passing the entity-level scope check doesn't mean every alert mentioning it is safe to return. A `COMPOSITE` alert an operator's scope excludes by `alert_types`, or one whose recorded position has since drifted outside the operator's bounds, must not leak just because the entity it's attached to happens to be in scope. Every alert row goes through the exact same `matchesScope` predicate `GET /alerts` already uses — one definition, same guarantee, applied at both access paths.

---

## Map to code

| Concept | Where it lives |
| --- | --- |
| Shared hash parse, reused by the list scan and the by-id lookup | `services/api/src/shared/liveEntities.ts` (`hashToLiveEntity`) |
| By-id lookup, no staleness cutoff | `services/api/src/shared/liveEntities.ts` (`getLiveEntity`) |
| `GET /entities/:entity_id` route: join, not-found handling, operator fail-closed scope check, demo unrestricted path | `services/api/src/routes/entities.ts` |
| Reused alert-shaped scope predicate (already existed from `GET /alerts`) | `services/api/src/shared/alertScopeFilter.ts` (`matchesScope`) |
| Existing indexes making the join a real index lookup, not a table scan | `infra/migrations/006_alerts.sql` (`alerts_entity_time_idx`, `alerts_counterparty_time_idx`) |
| Proof against real Postgres + Redis, including the enumeration-gap fix | `services/api/src/routes/entities.integration.test.ts` (`GET /entities/:entity_id` describe block) |
| Regression proof that `/entities/live` isn't shadowed by this checkpoint's new `/:entity_id` route | `services/api/src/routes/entities.integration.test.ts` (mount-order describe block), `services/api/src/index.ts`'s mount order |

---

## Retention questions

1. Why does `getLiveEntity` deliberately skip the staleness cutoff `scanLiveEntities` applies?
2. Walk through what `GET /entities/:entity_id` returns for an entity that's gone dark but has alert history, and why that's `200` not `404`.
3. Why isn't a counterparty-only alert match sufficient to establish that an entity is in an operator's scope?
4. Why does the returned alert list get filtered by `matchesScope` even after the entity itself has already passed its own scope check?
5. What would have broken silently if `/entities/live` had stayed mounted after `/entities` instead of before it, once `/:entity_id` existed?

---

## Completion checklist

- [ ] I can explain why a by-id lookup needed its own scope-enforcement design, even though CP1 already enforced scope for the list
- [ ] I can explain the difference between "entity not found" and "entity is dark but has history" and why both return different things
- [ ] I can trace why a counterparty match alone can't grant visibility into the counterparty entity
- [ ] I can explain the route-mount-order bug this checkpoint could have reintroduced, and how the regression test catches it
- [ ] I have run the real dev server myself and confirmed the France-scope alert_types filtering example against real Postgres state, not just the test suite
