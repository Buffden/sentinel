# Entity Detail Panel — Checkpoint Debrief (FE-CP1)

Evidence for FE-CP1, checked on 2026-09-21. See [`entity-detail-panel.md`](entity-detail-panel.md) for the mental model this checkpoint implements, and `mockups/entity-detail-panel.svg` for the approved design.

---

## Automated checks

- `tsc --noEmit` (dashboard): clean.
- `eslint` (dashboard, changed files): clean — caught one real issue mid-implementation (see below).
- Full dashboard test suite: **38/38 passing** — the prior 30 (all unmodified) plus 8 new for the `entities/entity-detail` adapter: live-entity envelope adaptation, `entity: null` (dark entity) preserved rather than coerced away, empty alerts array, a malformed alert entry dropped rather than throwing, and the `isValidWireEntityDetailDto` guard's accept/reject cases.

## A real lint catch during implementation

`react-hooks/set-state-in-effect` flagged a synchronous `setState({kind: 'loading'})` at the top of the data-fetch effect. Fixed by removing it rather than suppressing the rule: `entityId` is fixed for a given widget instance's entire lifetime (a new entity always gets a new panel, per `WorkspacePanelContext`'s deterministic per-entity panel id), so the effect only ever runs once per mount and the initial `useState({kind: 'loading'})` already covers it — the reset was dead code the lint rule correctly flagged as a real anti-pattern, not a false positive.

## Real browser verification (not just the test suite)

Per this project's rule that frontend changes must be exercised in a real browser before being called done — not just typechecked/tested. No project skill existed yet for running this app, so a driver was written by hand: `playwright` + `chromium` installed into the session's scratchpad (not the repo), Chromium downloaded, and a script driving the actual running dev server.

Two real obstacles hit and resolved along the way, not glossed over:

1. **The demo auth endpoint's real rate limit** (`POST /auth/demo`, 1 request/hour/IP) was hit on the second run from the same headless-browser IP, since the first run had already consumed it. Worked around by minting a demo JWT directly (same shape the real endpoint issues) and injecting it as a cookie via Playwright's `context.addCookies`, rather than repeatedly exercising the rate-limited endpoint itself — the thing under test was the new widget, not the demo login flow.
2. **Hardcoded entity ids went stale between querying Postgres and clicking in the browser**, because the dev environment's synthetic load generator churns alert data continuously (3,199 live alerts at verification time). Fixed by reading the currently-rendered entity ids directly out of the DOM at runtime instead of hardcoding ids from an earlier query.

With those resolved, against the real running API + dashboard + Postgres/Redis:

1. Logged in as a real demo session, loaded the dashboard, confirmed the Alerts widget showing real live data (3,199 alerts).
2. Clicked a real alert's entity_id (`a12f72`): a new Entity Detail panel opened, docked to the right, correctly rendering the **dark-entity** state — red "DARK" badge, "No current live state — see alerts below for last known position," and a real, populated "Recent Alerts" list for that entity. (The dev demo dataset had no currently-live entities at verification time, confirmed separately via `GET /entities`; the live-state render branch is a simple ternary already proven correct on the backend side during CP2's own verification, so this wasn't force-tested with synthetic Redis data.)
3. Clicked a second, different entity's id (`a4460f`): a second panel opened alongside the first — both visible and independently populated simultaneously, proving real multi-instance behavior, not a single panel being repainted.
4. Re-clicked the first entity (`a12f72`) again: panel count stayed at exactly 2, not 3 — confirmed by screenshot and by counting rendered panels before/after, proving the deterministic-panel-id focus behavior works against the real Dockview instance, not just in isolated reasoning.
5. `console --errors` equivalent (page/console error listeners) reported zero errors across the entire run.
6. Both the API and dashboard dev servers' PIDs were confirmed via `lsof` at start and explicitly killed at the end; ports confirmed free afterward.

Screenshots from this run are in the session scratchpad (not committed — this debrief's text is the durable record, per this project's rule against Markdown files carrying embedded scratch artifacts).

## What this proves, and what it doesn't yet

Proves: the outer-Dockview architectural fix is real and correct (verified against the actual Dockview instance, not mocked), the deterministic panel-id mechanism prevents duplicates under a real repeat click, the dark-entity render path works against real backend data, and the whole path from `AlertWidget` click through `WorkspacePanelContext` through `GET /entities/:entity_id` to rendered pixels is wired correctly end to end.

Does not prove on its own: the History tab (FE-CP2, TimescaleDB), the Relationships tab (FE-CP3, Neo4j graph pivot — which will reuse this same `WorkspacePanelContext` mechanism), or the live-entity (non-dark) render branch under real browser conditions specifically (proven on the backend side in CP2; the frontend branch is a simple, low-risk ternary on already-tested data).

## Recommendation

No project skill existed for running this app end to end (dev server + auth + browser drive). Since real setup work was needed (installing Playwright, working around the demo rate limit, discovering the `/dashboard` route), this is a good candidate for `/run-skill-generator` to capture for future sessions — flagged here rather than acted on unprompted.

## Addendum (2026-09-22): the map-marker trigger, closed after the fact

This debrief's own mental-model doc states the widget is "opened by clicking any entity_id: an alert card's counterparty, a map marker, or a relationship-graph node" — but only the first and (later, FE-CP3) third of those three were ever actually wired. The map-marker click was named as intent and never implemented, surfaced when the developer tried it against a real running instance with real OpenSky-fed traffic.

Closed the same day, reusing `WorkspacePanelContext` a fourth time (no context changes needed): `MapWidget`'s deck.gl overlay now takes an `onClick` that resolves the picked aircraft and calls `openEntityDetail`. Caught a real, separate bug while wiring it: `AircraftPosition.id` (the aviation layer's data key) is set to the aircraft's **callsign** when known, for display purposes — passing that straight into `openEntityDetail` would have looked up a callsign as if it were an `entity_id` and always 404'd for any aircraft with a known callsign (i.e. almost all of them). Fixed by adding a separate `entityId` field to `AircraftPosition`, kept distinct from the display-oriented `id`, so a click always resolves the real canonical id regardless of what `id` is used for elsewhere.

Verified against the real, now-fully-live pipeline (ingestion-poller → position-consumer → correlation-worker → alert-evaluator, all started for this same manual testing session, real OpenSky traffic over the SF Bay Area): a real headless-browser click on a real aircraft icon opened the Entity Detail panel showing real current state (`AB5B2B` / `SWA4468`, `LIVE`, real lat/lon/altitude/speed/course), confirmed by screenshot, zero console errors. `tsc`/`eslint`/the full 50-test suite all stayed clean after the fix.
