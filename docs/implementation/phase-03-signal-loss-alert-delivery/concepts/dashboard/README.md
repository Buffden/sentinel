# Dashboard — Concepts and Architecture

UI reference: [worldmonitor](https://github.com/koala73/worldmonitor), AGPL-3.0-only.
Study for layout and visual patterns only. Do not copy source code.

---

## Contents

| Document | What it covers |
| --- | --- |
| This file | Visual target, shell structure, layer responsibilities, state ownership, checkpoint order |
| [`map-widget.md`](map-widget.md) | MapLibre GL + deck.gl integration, worker URL fix, layer boundary, header controls |
| [`filters.md`](filters.md) | CP7g floating FilterPanel, the layer-toggle-overlay deviation, the applyPositionUpdate merge fix, two drag-bounds bugs |
| [`shared-websocket.md`](shared-websocket.md) | CP7i live alert feed, the shared/reference-counted WebSocket singleton, REST vs WS alert wire-shape differences |
| [`frontend-testing.md`](frontend-testing.md) | CP7j — why Vitest, the tsconfig-alias gotcha, position staleness + alert dedup proven with tests |
| [`network-boundary-adapter.puml`](network-boundary-adapter.puml) | Wire JSON → adapter → domain model → React sequence |
| [`ws-separation-flow.puml`](ws-separation-flow.puml) | WebSocket lifecycle: connect, subscribe, reconnect, demo expiry, cleanup |

---

## Visual target

The dashboard is a dark, data-dense operational workspace. The map dominates the surface.
Panels are dense and data-first with no decorative chrome. Status colors follow the semantic
palette: green for live/healthy, amber for warning, red for critical/lost.

The worldmonitor reference confirms this direction: map as the primary surface, right-side
panels for feeds and alerts, minimal top navigation. No glassmorphism, decorative gradients,
or consumer-app styling. Panel headers are compact and consistent.

Reference image (long-term visual target): [`docs/ui/sentinel-dashboard-reference.png`](../../../../ui/sentinel-dashboard-reference.png)

---

## Shell structure

Two fixed regions:

- A top navigation bar at a fixed height, outside the workspace, persisting across all states.
- A resizable workspace filling the remaining viewport height.

```text
┌──────────────────────────────────────────────────────┐
│  TopNav: logo | nav | connection | session           │
├──────────────────────────────────────────────────────┤
│                               │                      │
│   Map widget                  │  Alert panel         │
│   (MapLibre GL + deck.gl)     │  (signal-loss list)  │
│                               │                      │
│   GLOBAL MAP  [2D][3D] [swap] │                      │
│                               │                      │
└──────────────────────────────────────────────────────┘
```

The map occupies roughly 65–70% of the workspace width. The alert panel is docked to the
right. Operators may resize panels; the default layout is a starting state, not a fixed
constraint. Layout persistence is deferred to a later phase.

---

## Build order

Three layers, built in sequence. Do not jump ahead.

**Layer 1: Shell.** Top nav bar and workspace with placeholder panels. No real data yet.
Exit proof: panels are visible and resizable.

**Layer 2: Map widget.** MapLibre GL initialised inside the map panel. The panel container
provides explicit height so MapLibre sizes correctly. A layer-toggle overlay is rendered
inside the map widget. The aviation deck.gl layer (aircraft markers) is the first real layer
implementation. Exit proof: map tiles render, projection toggle works, aircraft dots appear.

> **CP7g deviation:** the generic, searchable/collapsible layer-enable overlay described above
> was never built, and CP7g's aviation filters (callsign search, airborne/grounded status) live
> in their own floating `FilterPanel` instead, not that overlay. Reason: aviation is still the
> only map layer — building a generic multi-layer toggle overlay now, with nothing else to
> toggle, would be exactly the speculative platform primitive this project's extensibility rule
> warns against (build shared primitives only when a real second consumer proves the need). The
> layer-toggle overlay is still the right home for cross-domain layer enable/disable once a
> second layer (vessels, weather, etc.) actually exists — build it then, against that real need,
> and fold aviation's filters in if it makes sense at that point. Implementation details, the
> `applyPositionUpdate` merge fix, and two drag-bounds bugs: [`filters.md`](filters.md).

**Layer 3: Alert panel.** Signal-loss alert list docked to the right. Scrollable, styled with
shared design tokens. Static data first, live feed wired in a later checkpoint. Exit proof:
alert cards render with correct status colors and layout holds on resize.

---

## Directory structure

Feature-Sliced Design (lightweight). Dependency direction — never reversed:

```text
app → widgets → features → entities → shared
```

```text
services/dashboard/
  public/
    maplibre-gl-worker.mjs        worker served as static asset (Turbopack fix)
    maplibre-gl-shared.mjs        shared module imported by the worker

  src/
    app/                          routing and composition only
      layout.tsx
      page.tsx                    composes TopNav, Workspace, Footer

    shell/                        layout primitives
      Footer.tsx
      panel-grid/
        PanelGrid.tsx             CSS grid container for widgets
        ResizablePanel.tsx        per-widget drag handles (row and column span)
        AddWidgetCard.tsx         dashed-border toggle card
        AddWidgetModal.tsx        widget visibility modal

    workspace/                    Dockview workspace and widget panel
      Workspace.tsx               Dockview container: map panel left, widget panel right
      WidgetPanel.tsx             PanelGrid + ResizablePanel widget grid

    widgets/
      top-nav/
      map-widget/                 MapLibre GL + deck.gl + header controls
        MapWidget.tsx
        mapStyle.ts
        types.ts                  MapLayerDefinition interface
        layers/
          aviationLayer.ts
      alert-widget/
      flight-info-widget/
      route-status-widget/

    shared/
      ui/
        WidgetHeader.tsx
      styles/
        tokens.css                CSS custom properties

  docs/adr/
    ADR-017-maplibre-deck-gl-map-engine.md
    ADR-018-dockview-workspace.md
    ADR-019-map-rendering-architecture.md
```

---

## Layer responsibilities

**`app`** — routing and composition only. The operations page wires widgets together and owns
top-level layout state (which panel is on which side, split percentage). No WebSocket parsing,
API calls, or filtering logic here.

**`widgets`** — widget implementations (TopNavigation, MapWidget, AlertWidget). Each is an
independently rendered panel. Not generic — `MapWidget` is a Sentinel widget, not a reusable
grid primitive. Aviation filter controls live inside the aviation layer configuration within
the MapWidget's layer overlay, not as a separate sidebar.

**`features`** — user-facing behavior: authentication, demo-session, entity-filtering,
live-feed, connection-status. Distinction from entities: `AircraftMarker` is an entity UI;
"filter aircraft by altitude" is a feature.

**`entities`** — domain business objects: `TrackedEntity`, `Alert`. These carry the frontend
domain model, not raw API JSON.

**`shared`** — zero Sentinel business knowledge. Panel, StatusBadge, HTTP client, WebSocket
wrapper, design tokens, date formatter. If `shared/` grows `SignalLossAlert` or `Aircraft`,
those are in the wrong layer.

---

## Network boundary and adapter strategy

Do not let React components consume raw backend JSON directly. Every API response passes
through an adapter function that converts the wire DTO to the frontend domain model before
reaching React.

Both `last_seen_ms` (REST) and `timestamp_ms` (WebSocket) map to a single `eventTimeMs`
field in the frontend model. React never knows which field name the backend used.

See [`network-boundary-adapter.puml`](network-boundary-adapter.puml) for the full sequence:
wire JSON → validator → adapter → domain model → React, including the malformed-frame discard path.

---

## Frontend domain models

Two core domain types for CP7:

**TrackedEntity** — represents one live aircraft or ground vehicle. Fields: id, callsign, lat,
lon, altitudeM, speedMps, courseDeg, eventTimeMs, entityType, entitySubtype, onGround. All
optional fields are nullable.

**Alert** — represents one open alert. Fields: id, alertType, entityId, entityType, status,
priority, detectedAtMs, payload. Payload shape is alert-type-specific; SIGNAL_LOSS carries
darkSinceMs and last-known position fields.

These are frontend types — not database rows and not wire DTOs.

---

## Distributed-system invariants the frontend must enforce

**Position monotonicity.** When a WebSocket position update arrives for an entity already in
state, accept it only if its `eventTimeMs` is strictly newer than the stored value. Discard
stale frames silently. Never regress a marker to an older position. Implement as a pure
reducer in `entities/tracked-entity/model/` and unit-test it heavily.

**Alert deduplication.** If the same `alert_id` arrives twice over WebSocket, the panel must
show exactly one entry. Use a map keyed by alert_id rather than appending to an array.
Write-by-key is idempotent.

These two invariants are the most important frontend tests in CP7.

---

## State ownership

No Redux in CP7. State lives as close to its consumer as possible.

| State | Owner |
| --- | --- |
| Layout (swapped, mapPct) | `page.tsx` |
| Live entities | operations page via live-feed hook |
| Alerts | operations page via live-feed hook |
| Filter state | operations page, passed to MapWidget layer overlay |
| Map projection (2D/3D) | `MapWidget` (local) |
| Auth / session | AuthGuard; cookie managed by server |
| Socket lifecycle | shared websocketClient |

Redux Toolkit / Zustand becomes justified only when selected-entity state must coordinate
between map, detail panel, timeline, and investigation (Phase 09+). Do not add it now.

---

## WebSocket separation

Never open a WebSocket inside a component. The layers are:

```text
MapWidget / AlertWidget
  → useLiveFeed()          (features/live-feed)
    → websocketClient.ts   (shared/realtime)
      → WebSocket
```

`websocketClient.ts` knows: connect, disconnect, send, message, close, error. It does not
know what an aircraft is. `features/live-feed` knows: position-updates, alert-events,
subscribe bbox. It does not know how to render a marker. One WebSocket per page — two
connections would double position-update delivery.

See [`ws-separation-flow.puml`](ws-separation-flow.puml) for the full sequence including
subscribe, reconnect, demo expiry, and cleanup.

---

## Design tokens

All color, spacing, typography, and dimension values are CSS custom properties in
`shared/styles/tokens.css`. Components reference tokens, not hardcoded values.

| Category | Tokens |
| --- | --- |
| Background | app, panel, elevated surface |
| Borders | default, subtle, focus |
| Text | primary, secondary, muted |
| Status | live green, warning amber, critical red, informational blue |
| Spacing | 4px base unit scale |
| Dimensions | topnav height, panel min-width, border radius |

CSS Modules for component-specific styles. Global CSS variables for tokens. No Tailwind.
No hex values repeated across multiple component files.

---

## Quality gates

Run `typecheck`, `lint`, `test`, and `build` before declaring any checkpoint done.

Most important CP7 tests:

- Stale position (older `eventTimeMs`) is ignored
- Newer position is accepted and marker moves
- Duplicate `alert_id` over WebSocket produces one panel entry
- Bbox serialized correctly in subscribe message
- REST hydration seeds map and alert panel on mount
- 401 response redirects to login
- WS disconnect shows connection status
- Reconnect re-seeds from REST
- Demo expiry (close 4401) shows banner

---

## Out of scope for this phase

These exist as reserved slots in the shell. Implementations wait for their respective phases.

- Route deviation alert cards (Phase 04)
- Proximity alert cards (Phase 05)
- Alert lifecycle controls: acknowledge, resolve (Phase 08)
- Investigation and entity detail panel (Phase 09)
- Historical timeline playback (future)
- Analytics and settings (future)

---

## Checkpoint order

| Step | Deliverable | Exit proof |
| --- | --- | --- |
| CP7a | Demo API additions | CLI verification only |
| CP7b | Next.js bootstrap + quality tooling | lint / typecheck / build pass |
| CP7c | Tokens + workspace shell + map widget (MapLibre GL + deck.gl) | Tiles render, aircraft dots visible, panels resizable |
| CP7d | Authentication + login page | 401 redirects; cookie auth works |
| CP7e | REST map hydration | real Redis entities on map |
| CP7f | Live position WebSocket | aircraft moves without page refresh |
| CP7g | Filters — floating `FilterPanel` inside the Map widget; see [`filters.md`](filters.md) | filters do not corrupt underlying entity state |
| CP7h | REST alert hydration | existing SIGNAL_LOSS alerts appear |
| CP7i | Live alert WebSocket feed | new alert appears without page refresh |
| CP7j | Dedupe + stale guards | duplicate / stale simulation passes |
| CP7k | Reconnect reconciliation | disconnect → reconnect → hydrated |
| CP7l | Demo expiry UX | WS close 4401 shows banner; redirect works |
| CP7m | Full E2E demonstration | aircraft → dark → alert → alert panel |
