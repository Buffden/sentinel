# Entity Detail Panel — Design and Learning Reference

Plain language first, then technical depth, then the code. Use this to understand, inspect, and defend FE-CP1 (the Overview tab of the Entity Detail widget).

> **Superseded (2026-09-22):** the multi-instance Dockview design described below (multiple simultaneous entity panels, opened via `api.addPanel`) was replaced the same day by an explicit product decision: one single, always-visible Entity Detail widget, permanently occupying the layout slot `FlightInfoWidget` used to hold, updated in place on every new selection. See [`entity-detail-single-panel/`](../entity-detail-single-panel/) for the current design and why it changed. This file remains as the accurate record of FE-CP1's original architecture, the real gap it discovered (no multi-instance widget support existed yet), and the reasoning at the time — historically true, not currently true.

---

## What this checkpoint is, and deliberately isn't

FE-CP1 is Phase 09's first frontend checkpoint: a dockable widget that shows one entity's current state (Redis, via `GET /entities/:entity_id`) plus its recent alerts, opened by clicking any bare `entity_id` in the dashboard — starting with `AlertWidget`'s own entity/counterparty references. Multiple entities can be investigated side by side, one panel instance per entity.

It implements only the Overview tab, per the approved mockup (`mockups/entity-detail-panel.svg`). The tab bar itself shows HISTORY and RELATIONSHIPS as inert placeholders — not a broken feature, just not built yet, matching the mockup's own "later checkpoint" annotations for those two (FE-CP2/TimescaleDB, FE-CP3/Neo4j).

---

## Concepts in plain language

### The architectural discovery this checkpoint surfaced before any code was written

The approved mockup assumed "one instance per opened entity" — multiple independent, coexisting panels. The codebase didn't actually support that yet. Two layout systems exist stacked on top of each other: an **outer Dockview instance** (`Workspace.tsx`, genuine multi-instance/dock/tab support via `api.addPanel`) hosting exactly two panels (`map`, `widgets`), and an **inner custom grid** (`WidgetPanel.tsx`'s `PanelGrid`) inside the `widgets` pane, where `AlertWidget`/`FlightInfoWidget`/`RouteStatusWidget` live — hardcoded by string literal, one instance each, no programmatic open-with-params API. `CLAUDE.md`'s "registry-driven, multi-instance, dockable" widget model describes the *outer* Dockview's actual capability, which nothing had used for a second real widget type yet.

This was surfaced to the developer before writing any code (per `CLAUDE.md`'s "when a design assumption fails, stop... present alternatives" rule), not silently worked around. The chosen fix: register Entity Detail as a new top-level Dockview panel type (a sibling to `map`/`widgets`), reusing Dockview's *already-existing* multi-instance capability rather than extending the inner custom grid or building a new library integration.

### Why this needed the codebase's first React context

Opening a new top-level Dockview panel requires the outer `DockviewApi`, which `Workspace.tsx` holds. `AlertWidget` — where the click actually happens — is nested several components deep inside the inner grid, with no prop path to that API. `WorkspacePanelContext` (`features/workspace/WorkspacePanelContext.tsx`) is the first context in this codebase, justified by two real consumers within this same feature: `AlertWidget`'s entity_id click-through (this checkpoint) and the Relationships tab's graph-pivot (FE-CP3, later) — the identical "click an entity_id, open its detail panel" mechanism reused a second time, not a speculative abstraction built ahead of need.

### Why re-clicking the same entity can't create a duplicate panel

`WorkspacePanelContext.openEntityDetail` derives a deterministic panel id (`entity-detail-${entityId}`) and checks `api.getPanel(id)` before deciding whether to focus the existing panel (`.api.setActive()`) or create a new one. There is exactly one code path into opening this widget type, and it always goes through this check — a repeat click can only ever focus, never duplicate. Verified directly in a real browser, not just reasoned about: see the debrief.

### Why the wire types weren't re-invented

`GET /entities/:entity_id` returns `{ entity: <same shape as GET /entities/live>, alerts: [...] }`. The `entity` field is byte-identical to what `entities/tracked-entity/adapter.ts` (`WireEntityDto`, `wireToTrackedEntity`) already parses for the map's own live feed, and each `alerts` element is byte-identical to what `entities/alert/adapter.ts` (`WireAlertDto`, `wireToAlert`) already parses for `AlertWidget`. `entities/entity-detail/` only adds the envelope (`EntityDetail`, `wireToEntityDetail`, `isValidWireEntityDetailDto`) — it does not redefine either nested shape.

### Why a dark entity is a distinct, deliberately-rendered state, not an edge case bolted on

`entity: null` (no current Redis hash) is a first-class branch in the widget: a red "DARK" badge instead of green "LIVE", and "No current live state — see alerts below for last known position" instead of position rows. This is not defensive/error-path code — it's the primary scenario a `SIGNAL_LOSS` investigation exists for, and it was the actual state observed in the real browser verification below (the current dev demo dataset had no currently-live entities at all).

---

## Map to code

| Concept | Where it lives |
| --- | --- |
| Envelope domain model + adapter, reusing existing entity/alert shapes | `services/dashboard/src/entities/entity-detail/{model,adapter,api}.ts` |
| First React context: deterministic-id panel open/focus | `services/dashboard/src/features/workspace/WorkspacePanelContext.tsx` |
| New top-level Dockview panel type, registered alongside `map`/`widgets` | `services/dashboard/src/workspace/Workspace.tsx` |
| The widget itself (Overview tab; History/Relationships inert) | `services/dashboard/src/widgets/entity-detail-widget/EntityDetailWidget.tsx` |
| First real consumer: clickable entity_id/counterparty in alert cards | `services/dashboard/src/widgets/alert-widget/AlertWidget.tsx` (`DetailRow`'s new `onClick`, the card header) |
| Proof against real wire shapes | `services/dashboard/src/entities/entity-detail/adapter.test.ts` |

---

## Retention questions

1. What two-layer layout system did this checkpoint discover, and why couldn't Entity Detail just be a fourth entry in the existing `WidgetPanel.tsx` grid?
2. Why is `WorkspacePanelContext` justified now rather than being a speculative abstraction?
3. Walk through exactly what prevents a duplicate panel when the same entity is clicked twice.
4. Why don't `entities/entity-detail/model.ts`/`adapter.ts` redefine the entity or alert shapes themselves?
5. What does the widget show for a dark entity, and why is that the important case rather than an edge case?

---

## Completion checklist

- [ ] I can explain the outer-Dockview-vs-inner-grid distinction and why it mattered for this checkpoint specifically
- [ ] I can trace `openEntityDetail` end to end: click → context → `getPanel`/`addPanel` → widget mount → fetch
- [ ] I can explain why this is the first context in the codebase and name its two real consumers (one built, one planned)
- [ ] I can explain what makes a repeat click idempotent at the panel level
- [ ] I have opened the real dashboard myself, clicked two different entities, and confirmed both panels coexist and re-clicking one doesn't duplicate it
