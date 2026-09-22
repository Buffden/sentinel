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
