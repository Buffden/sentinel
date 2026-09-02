# CP7g: Aviation Filters — Concepts and Implementation Overview

Covers the floating `FilterPanel`, why it isn't the generic layer-toggle overlay this doc's
own `README.md` originally described, the `applyPositionUpdate` merge fix the status filter
required, and the two drag-bounds bugs found (and fixed) after implementation.

---

## What was built

A floating, draggable, collapsible filter island rendered inside the Map widget's own canvas
area (`features/entity-filtering/FilterPanel.tsx`), not a separate Dockview panel and not an
overlay layered outside the widget. It carries two controls:

- **Callsign search** — case-insensitive substring match. Non-matching aircraft are **dimmed**
  (rendered at low alpha), not removed — the operator can still see where they are.
- **Airborne / Grounded status** — a hard filter on `onGround`. Non-matching aircraft are
  excluded from the deck.gl layer's data entirely.

Plus a clear button (resets both to defaults) and a live "N of M aircraft shown" count.
Collapsing the panel via its header chevron leaves only the header bar (drag handle, title,
expand chevron) visible.

Entity subtype checkboxes and the altitude range slider from the original phase-03 spec were
deliberately deferred — not requested for this pass.

---

## Why a floating panel, not the layer-toggle overlay

This doc's own architecture section (see `README.md` in this directory) describes a generic,
searchable/collapsible layer-toggle overlay as the intended home for per-layer controls,
mirroring CLAUDE.md's permanent Map architecture rule. CP7g does not build that.

Aviation is still the only map layer that exists. Building a generic multi-layer enable/disable
overlay now, with nothing else to toggle, would be exactly the speculative platform primitive
the project's extensibility rule warns against — build shared primitives only when a real
second consumer proves the need. The floating `FilterPanel` is scoped to aviation's own filters
only; the layer-toggle overlay is still the right home for cross-domain layer enable/disable
once a second layer (vessels, weather, etc.) actually exists.

---

## The `on_ground` merge bug the status filter exposed

`applyPositionUpdate` (`entities/tracked-entity/model.ts`) used to fully overwrite an entity on
every WebSocket tick. The WS `position-updates` frame never carries `on_ground` or
`entity_subtype` — those fields only arrive via REST hydration. Together, this meant a
REST-hydrated entity's real ground status was silently wiped to `null` the instant its first
live position update arrived, which would have made the status filter flicker between correct
and broken for every actively-transmitting aircraft.

Fixed by merging instead of overwriting: `TrackedEntityUpdate` is a partial update type where a
source that doesn't carry a field simply omits the key (not `null`), and `applyPositionUpdate`
spreads `{ ...existing, ...incoming }` so an absent key preserves whatever value REST hydration
(or an earlier frame) already established.

**Residual gap:** an entity discovered only via WebSocket — never REST-hydrated — still has no
`on_ground` until the next hydration or reconnect, since the WS payload itself doesn't carry it.
Closing that fully needs a small `position-consumer` change (add `on_ground` to
`publishPositionUpdate`'s payload); not attempted here since this pass was frontend-only.

---

## Two drag-bounds bugs, found via manual browser testing

Both from the same root cause: the panel's position was never bounds-checked against its
container.

**1. Dragging off-screen.** The drag handler had no bounds check at all — the panel could be
dragged fully outside the map widget and vanish, still present in the DOM but off-screen with
no way back short of a page reload. Fixed by measuring the container's and panel's dimensions
once at drag start and clamping the position on every `mousemove`.

**2. Expand-after-collapse overflow.** Collapsing/expanding changes the panel's height, but
position wasn't re-clamped against the new size. Repro: collapse the panel (small), drag it
near the bottom edge of the map widget, then expand — the larger expanded body pushed past the
widget's boundary.

Fixed by re-measuring and clamping inside the collapse-toggle's click handler (`toggleCollapsed`
in `FilterPanel.tsx`), using `flushSync` from `react-dom` to force the `collapsed` state's DOM
update to commit before measuring the panel's ref: toggle the state inside `flushSync`, then
measure the container and panel bounds, then clamp the stored position against them.

This runs inside the click handler, not a `useLayoutEffect`. Calling `setState` synchronously
inside an effect trips this repo's `react-hooks/set-state-in-effect` lint rule as a hard error,
even behind a runtime bailout guard that returns the previous state unchanged when nothing
actually needs to move — the linter can't see that the update sometimes no-ops, so it flags the
pattern regardless of the guard.

**Lesson for any future floating/draggable UI in this codebase:** bounds-check position on
every dimension change — drag *and* content-size change — not just drag.

---

## Manual inspection

- Open the dashboard, log in via the demo button, drag the "AVIATION FILTERS" panel by its
  header — it stays inside the map widget on every edge.
- Collapse it, drag the small header near the bottom-right corner, expand it — the full body
  stays visible, doesn't overflow into the alert panel or below the map.
- Type a callsign that doesn't match any tracked aircraft — matching aircraft stay full
  brightness, others dim rather than disappear.
- Switch the status dropdown to "Grounded" — the "N of M shown" count drops to just grounded
  aircraft; switch back to "All" and it returns to the full count.
