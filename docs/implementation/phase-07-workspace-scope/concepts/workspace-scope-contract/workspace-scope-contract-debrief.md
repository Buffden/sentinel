# Workspace Scope Contract — Checkpoint Debrief (CP1)

Real evidence for CP1, checked on 2026-09-13 against the actual running local stack, not restated from the design doc. See [`workspace-scope-contract.md`](workspace-scope-contract.md) for the mental model this checkpoint implements.

---

## Automated tests

Unit tests for the pure validation/resolution functions (`resolveGeoRegion`, `isSubsetOfAllowed`), no database:

```
Test Files  2 passed (2)
     Tests  17 passed (17)
```

Integration tests against real Postgres (`workspace.integration.test.ts`) — no-workspace `404`, create-then-read round trip, upsert-not-duplicate on a second `PUT`, invalid-body rejection writes nothing, `entity_types` outside `["aircraft"]` rejected, demo role `403` on both endpoints without touching the database, region catalog served:

```
Test Files  1 passed (1)
     Tests  7 passed (7)
```

Full API suite after the change (confirms the `requireAuth` change to attach `userRole` didn't regress anything already shipped):

```
Test Files  7 passed (7)
     Tests  72 passed (72)
```

---

## Real end-to-end run through the actual browser

Brought up the full local stack for this: Redpanda, TimescaleDB, Redis, Neo4j, plus all six application services (ingestion-poller, position-consumer, correlation-worker, alert-evaluator, API, dashboard) — not just the router-level test harness. The ingestion-poller was pulling live OpenSky data the whole time (105 real aircraft in the seeded bbox), so this wasn't an idle stack.

From the actual dashboard at `localhost:4000`, logged in for real, in the Safari Web Inspector console:

```js
await fetch('/api/users/me/workspace', { method: 'POST' }).then(r => r.json())
// { error: "no_workspace" }  -- 404, first call, no saved workspace yet

await fetch('/api/users/me/workspace/regions').then(r => r.json())
// [{name: "Global", ...}, {name: "France", ...}, {name: "United Kingdom", ...},
//  {name: "Western Europe", ...}, {name: "United States", ...}]

await fetch('/api/users/me/workspace', {
  method: 'PUT',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({
    geo_region: { name: 'France' },
    entity_types: ['aircraft'],
    alert_types: ['SIGNAL_LOSS']
  })
}).then(r => r.json())
// { geo_region: { name: "France", bounds: {...} }, entity_types: ["aircraft"], alert_types: ["SIGNAL_LOSS"] }

await fetch('/api/users/me/workspace', { method: 'POST' }).then(r => r.json())
// identical object to the PUT response above
```

All four calls returned exactly what the flow diagram predicts, including the client sending only `{ name: 'France' }` with no `bounds` field and the server resolving the full bounding box from the catalog on its own.

## Real state, inspected directly in Postgres

```
psql "postgres://sentinel:sentinel-dev@localhost:5433/sentinel" -c "SELECT user_id, scope, updated_at FROM user_workspaces;"

               user_id                |                                      scope                                                                                        |          updated_at
--------------------------------------+------------------------------------------------------------------------------------------------------------------------------------+------------------------------
 ec2e7a28-c38e-4b95-8d69-b77b2feed018 | {"geo_region": {"name": "France", "bounds": {"max_lat": 51.1, "max_lon": 9.6, "min_lat": 41.3, "min_lon": -5.2}}, "alert_types": ["SIGNAL_LOSS"], "entity_types": ["aircraft"]} | 2026-09-13 17:36:50.42081+00
```

One row, exactly matching what the browser saw. `geo_region.bounds` is the real catalog value for France (`41.3, 51.1, -5.2, 9.6`), not the empty/placeholder object the client never even sent.

---

## What this proves, and what it doesn't yet

Proves: the full `POST`/`PUT /users/me/workspace` contract from the design doc is real, working, and observed through every layer (browser, dashboard proxy, API, Postgres) — not just asserted by a test harness.

Does **not** yet prove: that this scope does anything. `GET /alerts` and the WebSocket stream still ignore it entirely — that's CP2 and CP3. A saved scope today is observable, not yet operationally meaningful, exactly as the concept doc's flow diagram note says.

---

## Loose end noticed while testing

Starting `api`, `alert-evaluator`, and `correlation-worker` via `tsx src/<entry>.ts` does **not** auto-load their `.env` file — `tsx` v4 does not do this automatically in this setup, contrary to what I assumed when first writing the manual-test instructions. Each needs `npx tsx --env-file=.env src/<entry>.ts` (or an equivalent env loader) to pick up `JWT_SECRET`, `PROXIMITY_THRESHOLD_METRES`, etc. `position-consumer` and `ingestion-poller` have no `.env` of their own so this didn't bite them; `dashboard` uses Next.js's own `.env.local` auto-loading, unaffected. Not fixed here since it wasn't this checkpoint's job (`package.json` scripts weren't touched) — worth a one-line fix to each affected service's `start`/`evaluator`/`worker` script the next time one of those services is touched for its own reasons.
