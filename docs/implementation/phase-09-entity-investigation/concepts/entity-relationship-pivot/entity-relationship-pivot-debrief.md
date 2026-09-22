# Entity Relationship Pivot — Checkpoint Debrief (FE-CP3)

Evidence for FE-CP3, checked on 2026-09-22. See [`entity-relationship-pivot.md`](entity-relationship-pivot.md) for the mental model this checkpoint implements.

---

## Automated checks

- `tsc --noEmit` (dashboard): clean.
- `eslint` (dashboard, full `src`): clean on the first pass -- no `set-state-in-effect`/`purity` issues this time, having applied FE-CP2's keyed-remount pattern from the start.
- Full dashboard test suite: **50/50 passing** -- the prior 43 (all unmodified) plus 7 new for the `entities/entity-graph` adapter: `PROXIMITY_EVENT` and `KNOWN_ASSOCIATE` edges adapted correctly, an unrecognized `edge_type` returning `null` rather than being fabricated, and the two `isValid*` guards' accept/reject cases.

## Real browser verification (not just the test suite)

Reused FE-CP1/FE-CP2's driver setup. Against the real running API + dashboard + Neo4j (confirmed via `cypher-shell` beforehand that `a12f72` has real `PROXIMITY_EVENT` edges in the dev graph -- no manual seeding needed this time, unlike FE-CP2's history chart):

1. Opened `a12f72`'s panel from a real alert card, switched to RELATIONSHIPS: the graph rendered with `a12f72` as the center node (blue outline) and 12 real neighbor nodes arranged around it, all connected by solid lines (this dev environment has 0 `KNOWN_ASSOCIATE` edges, consistent with CP4's own backend debrief -- so no dashed edges were expected or seen). The legend and a "+11 more relationships not shown" caption both rendered, confirming the 12-node cap and truncation count are correct against this entity's real 23-edge degree.
2. Clicked a neighbor node (`a23a28`): a **second, independent** Entity Detail panel opened, showing its own real state -- `DARK` badge, "No current live state," and its own real "Recent Alerts" list, all fetched fresh for that entity. The original `a12f72` panel remained open on its own Relationships tab, unaffected.
3. Counted `OVERVIEW` tab-label occurrences across the page before and after the pivot: 1 → 2, confirming a genuinely new panel was created, not the existing one repainted.
4. Zero console/page errors across the entire run.
5. No manual database seeding was needed or performed this time -- the real dev Neo4j graph already had sufficient data; nothing to clean up afterward. Both server PIDs confirmed via `lsof` and explicitly killed; ports confirmed free.

## What this proves, and what it doesn't yet

Proves: the graph renders correctly against real Neo4j data including the edge-type styling and the high-degree-node truncation case, and the pivot mechanism opens a genuinely independent second panel with its own correctly-fetched state -- the exact multi-instance behavior FE-CP1's architectural fix was built to support, now exercised a second, different way (from inside a widget, not from `AlertWidget`).

Does not prove on its own: a `KNOWN_ASSOCIATE` edge's dashed-line rendering (none exist in this dev environment to test against; the styling logic itself is simple and covered by the adapter's own unit tests) or multi-hop traversal (out of scope by design, per CP4's backend and US-14's own "pivot by re-opening" model).

## Phase status

This completes Phase 09's frontend against the approved mockup (`entity-detail-panel.svg`): all three tabs (Overview, History, Relationships) are real and wired to real backend data. The map-overlay position track (noted as its own follow-on in FE-CP2's debrief) remains the one explicitly deferred piece from the original mockup.
