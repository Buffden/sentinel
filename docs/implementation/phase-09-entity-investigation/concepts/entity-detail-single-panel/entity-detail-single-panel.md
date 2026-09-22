# Entity Detail: Single Always-Visible Panel — Design and Learning Reference

Plain language first, then technical depth, then the code. Use this to understand, inspect, and defend the pivot away from FE-CP1's multi-instance Dockview design, made 2026-09-22 while manually testing the completed frontend against a real live pipeline.

---

## What this change is, and why it happened

FE-CP1 through FE-CP3 built a genuine multi-instance capability: clicking an entity opened a *new* Dockview panel, so an operator could investigate several entities side by side. That design was explicitly confirmed and built with real engineering behind it (a new top-level Dockview panel type, a deterministic-id focus-not-duplicate mechanism).

An explicit product decision reversed this, given after seeing the multi-panel behavior working against real data: the Entity Detail widget should instead be **one single, permanently visible panel**, occupying the fixed layout slot `FlightInfoWidget` (a hardcoded placeholder widget) used to hold, updated in place every time a different entity is selected — not a side-by-side multi-panel investigation view. This is a real requirements change discovered through use, not a bug fix; both the old and new choice were legitimate design points, presented to the developer as an explicit fork (single-panel vs. default-panel-plus-multi-instance) before implementation, per this project's rule that architectural discoveries get surfaced and decided, not silently picked.

---

## Concepts in plain language

### Why `WorkspacePanelContext` shrank instead of being replaced

The context's job was always "let any component change what the Entity Detail widget shows, without prop-drilling." Only *how* it fulfilled that job changed: previously it called Dockview's `api.addPanel`/`getPanel`/`setActive`; now it just holds one piece of shared state (`selectedEntity: { entityId, anchorMs } | null`) via `useState`. Every existing call site — `AlertWidget`'s three click spots, `RelationshipsTab`'s graph-pivot, `MapWidget`'s map-marker click — needed **zero changes**, because they never touched Dockview directly; they only ever called `openEntityDetail(entityId, options)`. This is the real payoff of having built the context as a proper abstraction boundary in FE-CP1 rather than passing a Dockview `api` reference around directly: the entire underlying mechanism could be swapped without touching a single consumer.

### Why the widget needed a keyed inner component, not just a prop change

The widget used to be created fresh per entity (one Dockview panel instance per `entity_id`), so its internal state (`activeTab`, the overview fetch) never needed to reset mid-lifetime — a new entity always meant a new mount. Now it's one persistent component whose selected entity changes underneath it without unmounting. Without a fix, clicking a new entity would leave the old entity's tab selection and (briefly) its stale fetched data on screen while the new fetch was in flight. Fixed the same way `HistoryTab`/`RelationshipsTab` already fixed the identical problem: extract the actual content into `EntityDetailBody`, mounted fresh via `key={selectedEntity.entityId}` in the outer `EntityDetailWidget`. A key change forces a clean remount, which resets `activeTab` back to Overview and the fetch state back to `loading` for free — no synchronous `setState` inside an effect, so no `react-hooks/set-state-in-effect` violation either.

### Why the empty state matters as much as the populated one

"Always visible" means it's on screen before anything is ever clicked, and its empty state ("Click an entity on the map, an alert card, or a relationship graph to inspect it here") is not a fallback error path — it's the actual state every session starts in. It reuses the same `WidgetHeader` pattern as every other widget, with a static "ENTITY DETAIL" title when nothing is selected (rather than an entity id, which doesn't exist yet).

### What was removed as a consequence

`FlightInfoWidget.tsx` (the original hardcoded `SAMPLE_FLIGHT` placeholder widget) is deleted outright, not deprecated in place — it served no purpose once genuinely replaced, per this project's guardrail against keeping unused code as a compatibility hedge. `Workspace.tsx` no longer registers an `'entity-detail-widget'` Dockview component type or holds a `DockviewApi` reference for `WorkspacePanelProvider` at all, since nothing about entity selection touches Dockview anymore.

---

## Map to code

| Concept | Where it lives |
| --- | --- |
| Shared selection state (`selectedEntity`), no more Dockview coupling | `services/dashboard/src/features/workspace/WorkspacePanelContext.tsx` |
| Keyed remount fix (`EntityDetailBody`), empty-state placeholder | `services/dashboard/src/widgets/entity-detail-widget/EntityDetailWidget.tsx` |
| Fixed layout slot (replaces the old `FlightInfoWidget` entry) | `services/dashboard/src/workspace/WidgetPanel.tsx` (`DEFAULT_ACTIVE`, `AVAILABLE_WIDGETS` in `AddWidgetModal.tsx`) |
| Simplified outer workspace (no entity-detail Dockview registration) | `services/dashboard/src/workspace/Workspace.tsx` |
| Deleted, no longer needed | `services/dashboard/src/widgets/flight-info-widget/` (removed) |

---

## Retention questions

1. Why did none of `AlertWidget`, `MapWidget`, or `RelationshipsTab` need any code changes when the underlying panel mechanism changed completely?
2. Why does `EntityDetailWidget` need a `key`-based remount for its body, when it didn't need one before this change?
3. What's the actual first thing a fresh session sees in this widget's slot, and why is that not an error state?
4. Why was `FlightInfoWidget.tsx` deleted rather than left in place, unused?
5. What real, working capability was given up by this change, and was it accidental or a deliberate tradeoff?

---

## Completion checklist

- [ ] I can explain why `WorkspacePanelContext`'s public API (`openEntityDetail`) didn't need to change even though its implementation completely did
- [ ] I can trace why a selection change without the `key` fix would show stale data momentarily
- [ ] I can explain the empty-state's role as a first-class state, not a fallback
- [ ] I have opened the real dashboard myself, confirmed the panel is visible with nothing selected, then confirmed clicking two different entities updates the same panel rather than opening a second one
