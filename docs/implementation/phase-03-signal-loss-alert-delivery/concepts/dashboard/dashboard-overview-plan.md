# Dashboard Overview Plan

UI reference: [worldmonitor](https://github.com/koala73/worldmonitor), AGPL-3.0-only.
Study for layout and visual patterns only. Do not copy source code.

---

## Visual target

The dashboard is a dark, data-dense operational workspace. The map dominates the surface.
Panels are dense and data-first with no decorative chrome. Status colors follow the
established semantic palette: green for live/healthy, amber for warning, red for critical/lost.

The worldmonitor reference confirms this direction: map as the primary surface, right-side
panels for feeds and alerts, minimal top navigation.

---

## Shell structure

Two fixed regions:

- A top navigation bar at a fixed height, outside Dockview, persisting across all workspace states.
- A Dockview workspace filling the remaining viewport height.

```text
┌──────────────────────────────────────────┐
│  TopNav: logo | nav | conn | session     │
├──────────────────────────────────────────┤
│                          │               │
│   Map widget             │  Alert panel  │
│   (MapLibre GL)          │               │
│                          │               │
└──────────────────────────────────────────┘
```

The map occupies roughly 70% of the workspace width. The alert panel is docked to the right.
Operators may resize and rearrange panels; the default layout is a starting state, not a fixed
constraint. Layout persistence is deferred to a later phase.

---

## Build order

Three layers, built in sequence. Do not jump ahead.

### Layer 1: Shell

Top nav bar and Dockview workspace with placeholder panels. No real data yet.
Exit proof: panels are visible, resizable, and dockable.

### Layer 2: Map widget

MapLibre GL initialised inside the Dockview map panel. The panel container provides explicit
height so MapLibre sizes correctly without workarounds. A layer-toggle overlay is rendered
inside the map widget. The aviation deck.gl layer (aircraft markers) is the first real layer
implementation.
Exit proof: map tiles render, navigation controls work, overlay toggles a layer.

### Layer 3: Alert panel

Signal-loss alert list docked to the right. Scrollable, styled with shared design tokens.
Static data first, live feed wired in a later checkpoint.
Exit proof: alert cards render with correct status colors and layout holds on resize.

---

## Checkpoint order

| Step | Deliverable | Exit proof |
| --- | --- | --- |
| Map renders | Full-screen MapLibre GL with tiles | Tiles visible, no console errors |
| Shell | Dockview workspace, top nav | Panels resize and dock |
| Map in shell | MapLibre inside Dockview panel | Correct height, tiles render |
| Design tokens | CSS custom properties for color, spacing, typography | No hardcoded hex values in components |
| Alert panel (static) | Right panel with placeholder alert cards | Layout holds on resize |
| Live data | WebSocket and REST hydration | Per CP7e onwards |

---

## Design token categories

Define all values as CSS custom properties before building panels. Components reference tokens,
not hardcoded values.

- Background: app, panel, elevated surface
- Borders: default, subtle, focus
- Text: primary, secondary, muted
- Status: live green, warning amber, critical red, informational blue
- Spacing scale: 4px base unit
- Fixed dimensions: topnav height, panel min-width, border radius

---

## Key constraints from worldmonitor reference

- Map is the dominant surface, not a sidebar or secondary view.
- Right-panel pattern (alerts, feeds) matches the Sentinel alert panel target exactly.
- No glassmorphism, decorative gradients, or consumer-app styling.
- Panel headers are compact and consistent.

---

## What is out of scope for this phase

These exist as reserved slots in the shell. Their implementations wait for their respective phases.

- Route deviation alert cards (Phase 04)
- Proximity alert cards (Phase 05)
- Alert lifecycle controls: acknowledge, resolve (Phase 08)
- Investigation and entity detail panel (Phase 09)
- Historical timeline playback (future)
- Analytics and settings (future)
