# Entity Detail: Single Always-Visible Panel — Checkpoint Debrief

Evidence for this change, checked on 2026-09-22. See [`entity-detail-single-panel.md`](entity-detail-single-panel.md) for the mental model.

---

## Automated checks

- `tsc --noEmit` (dashboard): clean.
- `eslint` (dashboard, full `src`): clean.
- Full dashboard test suite: **50/50 passing**, unmodified — no test depended on the Dockview-panel-opening mechanism directly (all existing tests target the adapters/models, which didn't change), confirming the swap was correctly isolated behind `WorkspacePanelContext`'s unchanged public API.

## Real browser verification (not just the test suite)

Against the real running pipeline (API, dashboard, and by this point the full ingestion → position-consumer → correlation-worker → alert-evaluator chain, all live against real OpenSky traffic over the SF Bay Area — the same session the multi-instance design was being manually tested in when the product decision to change it was made):

1. Loaded the dashboard fresh: the Entity Detail widget was already visible in its fixed slot, showing the empty-state placeholder ("Click an entity on the map, an alert card, or a relationship graph to inspect it here") — confirmed present by default, with no click required.
2. Confirmed the old `FlightInfoWidget` hardcoded sample data (`UAE204`) was completely gone from the page.
3. Clicked a real alert's entity: the panel updated to show that entity's real state.
4. Clicked a second, different entity: the panel updated **in place** to the second entity's real state (confirmed via screenshot: `AD27F3`/`JBU933`, `LIVE`, real lat/lon/altitude/speed/course, real recent alerts) — counted `RELATIONSHIPS` tab-label occurrences across the whole page before and after: stayed at exactly 1, confirming no second panel was created.
5. Zero console/page errors across the entire run.

## What this proves, and what it doesn't yet

Proves: the context's public API genuinely insulated every consumer from the underlying mechanism change (zero call-site edits needed), the keyed-remount fix correctly resets tab/fetch state on every new selection, and the always-visible empty state is real and correctly shown by default.

Does not change: any of FE-CP1–FE-CP3's own backend-facing proofs (the `GET /entities/:entity_id`, `/history`, `/graph` contracts, and their fail-closed scope guarantees) — this was purely a frontend presentation-layer change built entirely on top of already-verified backend behavior.

## Addendum (2026-09-22): live position updates, asked for once the single panel was visible in use

Once the panel was permanently visible and being watched during real testing, the natural next question came up: if the selected aircraft's position updates on the map, shouldn't the panel update too, without re-clicking? It didn't -- `EntityDetailBody` only ever did a one-shot `GET /entities/:entity_id` fetch on selection, then sat on that snapshot.

Fixed by adding `EntityDetailBody` as a third listener on `useLiveFeed`'s already-shared WebSocket connection (`MapWidget` and `AlertWidget` were the first two -- the hook's own header comment already documents this as supported), filtering incoming `position-updates` frames to the currently-selected `entityId` and merging them via the same `applyPositionUpdate` monotonic-merge function `MapWidget` already uses (stale/out-of-order frames discarded, not just newest-wins).

**Real, stated limit, not hidden:** this connection's server-side bbox filter is owned by `MapWidget` (via its own `subscribe(bbox)` calls tied to the map viewport) -- the widget only receives live updates for entities inside the *current* map view. A selected entity that scrolls out of view stops updating here until it's back on screen. This matches the actual scenario asked about ("the flight's position is updated on map") rather than claiming a stronger guarantee (e.g. "always live regardless of viewport") that isn't true of the current architecture.

Verified against the real live pipeline, not simulated: selected a real aircraft, read its displayed `LAT / LON`, waited 25 real seconds (more than two real OpenSky poll cycles at the poller's 10-second interval), read it again with no interaction in between. Real result: `37.6198, -122.3776` → `37.6183, -122.3797` -- a genuine, small, real movement, confirmed by screenshot. Zero console errors. `tsc`/`eslint`/the full 50-test suite stayed clean.
