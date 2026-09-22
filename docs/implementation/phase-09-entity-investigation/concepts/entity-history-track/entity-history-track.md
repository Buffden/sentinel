# Entity History Track — Design and Learning Reference

Plain language first, then technical depth, then the code. Use this to understand, inspect, and defend FE-CP2 (the History tab of the Entity Detail widget).

---

## What this checkpoint is, and deliberately isn't

FE-CP2 wires up the Entity Detail widget's History tab: a window selector plus an altitude-over-time chart, backed by `GET /entities/:entity_id/history` (Phase 09 CP3). It reuses FE-CP1's panel/context infrastructure directly rather than building anything new at that layer.

It deliberately does **not** build the map-overlay track. The approved mockup itself says the map overlay is "the real deliverable," this in-panel chart "a compact summary" — but they're materially different concerns (an SVG chart inside a widget vs. a new deck.gl layer in the Map widget's own layer registry), so this checkpoint ships the smaller, self-contained half and leaves the map overlay as an explicit, separately-scoped follow-on rather than a half-finished attempt at both.

---

## Concepts in plain language

### Why the window needed a new piece of context, not just a prop

`GET /entities/:entity_id/history` requires an explicit `from_ms`/`to_ms` — there is no server-side default (CP3's own design: an investigation always has a concrete window in mind, usually an alert's own `detected_at`). The approved mockup states this directly: the window "defaults to the opening alert's own `detected_at` window when opened from an alert card, else a manual range." Making that real meant extending `WorkspacePanelContext.openEntityDetail` with an optional `anchorMs`, threaded from `AlertWidget`'s three click sites (the card header, the `COUNTERPARTY` row, the `ENTITY ID` row) through to the new panel's initial params. This is the second real use of FE-CP1's context, not a new mechanism -- and a third (the Relationships tab's graph-pivot) is still to come in FE-CP3.

### Why altitude, specifically, and why a hand-rolled chart

Discussed and decided before implementation: the History sparkline in the mockup was originally undefined -- a generic line with no stated Y-axis. Altitude-over-time is the most investigatively useful choice for an aircraft (shows climb/cruise/descent shape; a deviation from that shape right before a `SIGNAL_LOSS` is exactly what an operator would scan for). `GET /entities/:entity_id/history` already returns `altitude_m` per point, so no backend change was needed. No charting library exists in this codebase yet (`deck.gl`/`maplibre-gl` are map-specific); a hand-rolled SVG `<polyline>` was chosen over adding a dependency for one sparkline, matching this project's "explicit code over clever abstractions" guardrail.

### Why the fetch logic is a separate, `key`-remounted child component

The window preset is real, mutable state that changes during this tab's lifetime (unlike `EntityDetailWidget`'s `entityId`, which is fixed per panel instance). A synchronous `setState({kind: 'loading'})` at the top of the fetch effect -- the natural first attempt -- is flagged by `react-hooks/set-state-in-effect` as an anti-pattern, and rightly so: it causes an extra render on every dependency change. The fix applied here: `HistoryPoints` (the component that actually owns the fetch and its loading/ready/error state) is mounted fresh, keyed by `${entityId}-${fromMs}-${toMs}`, whenever the window changes. A fresh mount's own initial `useState({kind: 'loading'})` already covers "show loading for this fetch" -- there's nothing left to reset mid-lifetime. `Date.now()` itself was also flagged (`react-hooks/purity` -- calling an impure function during render), fixed by capturing "now" once via a lazy `useState` initializer rather than reading it on every render.

### Why "no position history in this window" is a normal result, not a fallback

Confirmed directly against the real dev database: `position_history` was completely empty at verification time (a 48-hour TimescaleDB retention window with no active backfill in this dev environment). The empty-window message is the actual, common state an operator will see for a real, current investigation -- not a rare edge case. The chart-rendering path itself was verified separately by manually seeding real rows (see debrief).

---

## Map to code

| Concept | Where it lives |
| --- | --- |
| Domain model + adapter + API client for history points | `services/dashboard/src/entities/entity-history/{model,adapter,api}.ts` |
| `anchorMs` threaded through the panel-open context | `services/dashboard/src/features/workspace/WorkspacePanelContext.tsx` (`OpenEntityDetailOptions`) |
| Alert-card click sites now passing `alert.detectedAtMs` as the anchor | `services/dashboard/src/widgets/alert-widget/AlertWidget.tsx` |
| Window preset UI, keyed remount fix, hand-rolled altitude chart | `services/dashboard/src/widgets/entity-detail-widget/HistoryTab.tsx` |
| Tab switching (Overview/History now both real; Relationships still inert) | `services/dashboard/src/widgets/entity-detail-widget/EntityDetailWidget.tsx` |
| Proof against real wire shapes | `services/dashboard/src/entities/entity-history/adapter.test.ts` |

---

## Retention questions

1. Why does the window need an `anchorMs` at all, instead of the History tab always defaulting to "now"?
2. Why is altitude the chosen Y-axis, and where did that decision get made before any code was written?
3. Walk through why `HistoryPoints` is a separate, `key`-remounted component instead of `HistoryTab` handling its own fetch directly.
4. Why was `Date.now()` flagged, and what does the fix actually change about when "now" gets computed?
5. What's still explicitly out of scope for this checkpoint, and why wasn't it just built anyway since the backend already supports it?

---

## Completion checklist

- [ ] I can explain why `anchorMs` had to be threaded through `WorkspacePanelContext`, not just computed locally inside `HistoryTab`
- [ ] I can explain the `react-hooks/set-state-in-effect` fix in my own words, not just recite "move it to a keyed child"
- [ ] I can explain why `Date.now()` couldn't be called directly during render
- [ ] I can distinguish "no position history in this window" (empty, real, normal) from "entity not found" (404, scope) in the actual rendered UI
- [ ] I have opened the real dashboard myself, seeded real `position_history` rows for an entity, and watched the chart render against real data, not just the empty-window state
