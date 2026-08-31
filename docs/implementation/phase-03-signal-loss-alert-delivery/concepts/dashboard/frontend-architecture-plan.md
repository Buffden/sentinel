# CP7 Frontend Architecture Plan

Reference image (long-term visual target): [`docs/ui/sentinel-dashboard-reference.png`](../../../../../ui/sentinel-dashboard-reference.png)

This document defines the architecture, layering rules, state ownership, and checkpoint order
for the Sentinel dashboard before any code is written. Implement only Phase 03 / CP7 behavior.
The shell must survive all 10 phases without redesign.

---

## 1. Separate the final UI shell from current CP7 scope

The reference image contains more than Phase 03. Only the following belong to CP7:

- Live operations map
- Basic entity filtering
- Signal-loss alert panel
- Login / demo session

Out of scope for CP7 (slots reserved in shell, implementations deferred):

- Route deviation alerts (Phase 04)
- Proximity alerts (Phase 05)
- Alert lifecycle — acknowledge / resolve (Phase 08)
- Investigation / entity detail panel (Phase 09)
- Analytics, settings (future)
- Historical timeline (future)

Before implementing CP7c (the shell), a low-fidelity SVG mockup must be created,
saved alongside this document, and approved by the developer. Implementation must
match the approved mockup per the Workspace Visual Language rule in CLAUDE.md.

Target CP7 visual reference (Dockview default — two-panel):

![Sentinel dashboard reference](../../../../../ui/sentinel-dashboard-reference.png)

The CP7 layout is: TopNavigation fixed at the top, MapWidget occupying the majority of the workspace, AlertWidget docked to the right. The LAYERS overlay is rendered inside the Map widget. Aviation filter controls (callsign, type, altitude, status) live inside the aviation layer configuration within that overlay — not as a separate fixed FilterRail sidebar.

---

## 2. Directory structure

Feature-Sliced Design (lightweight):

```text
services/dashboard/
├── public/
├── src/
│   ├── app/                        routing and composition only
│   │   ├── layout.tsx
│   │   ├── page.tsx
│   │   ├── login/
│   │   ├── operations/
│   │   └── providers.tsx
│   │
│   ├── workspace/                  Dockview setup and default layout
│   │   ├── registry.ts
│   │   └── default-layout.ts
│   │
│   ├── widgets/                    Dockview widget implementations
│   │   ├── top-navigation/
│   │   ├── map-widget/             Map widget: MapLibre GL + deck.gl + layer overlay
│   │   └── alert-widget/           Alert widget: signal-loss alert list
│   │
│   ├── features/                   user-facing behavior
│   │   ├── authentication/
│   │   ├── demo-session/
│   │   ├── entity-filtering/
│   │   ├── live-feed/
│   │   └── connection-status/
│   │
│   ├── entities/                   domain business objects
│   │   ├── tracked-entity/
│   │   └── alert/
│   │
│   └── shared/                     zero Sentinel business knowledge
│       ├── realtime/
│       ├── config/
│       ├── ui/
│       └── styles/
│
├── package.json
├── tsconfig.json
└── next.config.ts
```

---

## 3. Layer responsibilities and dependency rules

Dependency direction — never reversed:

```text
app → widgets → features → entities → shared
```

**`app`** — routing and composition only. The operations page wires widgets together and owns top-level state. No WebSocket parsing, API calls, or filtering logic here.

**`widgets`** — Dockview widget implementations (TopNavigation, MapWidget, AlertWidget). Each is an independently rendered Dockview panel. Not generic — `MapWidget` is a Sentinel widget, not a reusable grid primitive. The FilterRail does not exist as a fixed sidebar; aviation entity filter controls live inside the aviation layer configuration within the MapWidget's layer overlay.

**`features`** — user-facing behavior: authentication, demo-session, entity-filtering, live-feed, connection-status. Distinction from entities: `AircraftMarker` is an entity UI; "filter aircraft by altitude" is a feature.

**`entities`** — domain business objects: `TrackedEntity`, `Alert`. Later: `Investigation`, `Workspace`, `Route`. These carry the frontend domain model, not raw API JSON.

**`shared`** — zero Sentinel business knowledge. Panel, StatusBadge, HTTP client, WebSocket wrapper, date formatter, env config. If `shared/` grows `SignalLossAlert` or `Aircraft`, those are in the wrong layer.

---

## 4. Network boundary and adapter strategy

Do not let React components consume raw backend JSON directly. Every API response passes through an adapter function that converts the wire DTO to the frontend domain model before reaching React.

Both `last_seen_ms` (REST) and `timestamp_ms` (WebSocket) map to a single `eventTimeMs` field in the frontend model. React never knows which field name the backend used.

See [`network-boundary-adapter.puml`](network-boundary-adapter.puml) for the full sequence: wire JSON → validator → adapter → domain model → React, including the malformed-frame discard path.

---

## 5. Frontend domain models

Two core domain types for CP7:

**TrackedEntity** — represents one live aircraft or ground vehicle. Fields: id, callsign, lat, lon, altitudeM, speedMps, courseDeg, eventTimeMs, entityType, entitySubtype, onGround. All optional fields are nullable.

**Alert** — represents one open alert. Fields: id, alertType, entityId, entityType, status, priority, detectedAtMs, payload. Payload shape is alert-type-specific; SIGNAL_LOSS carries darkSinceMs and last-known position fields.

These are frontend types — not database rows and not wire DTOs.

---

## 6. Distributed-system invariants the frontend must enforce

### Position monotonicity

When a WebSocket position update arrives for an entity already in state, accept it only if its `eventTimeMs` is strictly newer than the stored value. Discard stale frames silently. Never regress a marker to an older position.

Implement as a pure reducer in `entities/tracked-entity/model/` and unit-test it heavily.

### Alert deduplication

If the same `alert_id` arrives twice over WebSocket, the panel must show exactly one entry. Use a map keyed by alert_id rather than appending to an array — write-by-key is idempotent.

These two invariants are the most important frontend tests in CP7.

---

## 7. State ownership (no Redux in CP7)

| State | Owner |
| --- | --- |
| Live entities | operations page via live-feed hook |
| Alerts | operations page via live-feed hook |
| Filter state | operations page, passed to MapWidget layer overlay |
| Auth / session | AuthGuard; cookie managed by server |
| Socket lifecycle | shared websocketClient |

Redux Toolkit / Zustand becomes justified only when selected-entity state must coordinate between map, detail panel, timeline, and investigation (Phase 09+). Do not add it now.

---

## 8. WebSocket separation

Never open a WebSocket inside a component. The layers are: MapWidget/AlertWidget → `useLiveFeed()` (features/live-feed) → `websocketClient.ts` (shared/realtime) → WebSocket. See [`ws-separation-flow.puml`](ws-separation-flow.puml) for the full sequence including subscribe, reconnect, demo expiry, and cleanup.

`websocketClient.ts` knows: connect, disconnect, send, message, close, error. It does not know what an aircraft is.

`features/live-feed` knows: position-updates, alert-events, subscribe bbox. It does not know how to render a marker.

One WebSocket per page. Two connections would double position-update delivery.

---

## 9. Design tokens

Define all color, spacing, typography, and dimension values as CSS custom properties in `shared/styles/tokens.css` before building panels. Components reference tokens, not hardcoded values.

Token categories: background (app, panel, elevated), borders, text (primary, secondary, muted), accent, status colors (live green, warning amber, critical red), spacing scale, and fixed dimensions (topbar height, rail width, panel width, border radius).

CSS Modules for component-specific styles. Global CSS variables for tokens. No Tailwind. No hex values repeated across multiple component files.

---

## 10. Shell layout

The shell has two levels: the application shell and the Dockview workspace.

The application shell is a CSS Grid with two rows: a fixed-height TopNavigation bar and a flexible Dockview workspace that fills the remaining viewport height. TopNavigation lives outside Dockview and persists regardless of workspace layout.

The Dockview workspace hosts independently resizable, dockable widget panels. The CP7 default layout is: MapWidget occupying approximately 70% of the width, AlertWidget docked to the right. Operators may resize panels; layout persistence is deferred to a later phase.

MapLibre GL requires its container to have an explicit height. Set the MapWidget Dockview panel container to `height: 100%`; MapLibre sizes correctly and does not require the overflow workarounds that Leaflet needed.

---

## 11. Component tree for CP7

![Sentinel dashboard reference](../../../../../ui/sentinel-dashboard-reference.png)

`SentinelOperationsPage` renders `AppShell`, which holds `TopNavigation` (outside Dockview) and `DockviewWorkspace`. TopNavigation contains: SentinelLogo, OperationsNavigation, ConnectionIndicator, SessionMenu.

DockviewWorkspace hosts two panels for CP7: `MapWidget` and `AlertWidget`. MapWidget contains `MapLibreMap` with an `AviationDeckLayer` (deck.gl layer for aircraft positions) and a `LayerOverlay` with `AviationLayerControls` (CallsignSearch, GroundStateFilter, EntitySubtypeFilter, AltitudeFilter). AlertWidget renders `SignalLossAlertCard` instances.

Do not build yet: SelectedAircraftPanel, PositionHistory, InvestigationPanel,
TimelinePlayback, Analytics, Settings, RouteDeviationAlertCard, ProximityAlertCard.
Their slots exist in the shell; their code waits for their phase.

---

## 12. Avoid `useEffect` soup

A single page component with many `useEffect` hooks is the most common way this deteriorates. Expose named hooks that each own one concern: authentication, live entities, alerts, viewport subscription, connection status.

Implement important algorithms (position reducer, alert deduplication) as plain TypeScript functions testable without React.

React = composition + rendering. Business logic = plain functions.

---

## 13. Quality gates — mandatory per checkpoint

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

## 14. Runtime validation

TypeScript interfaces do not protect the browser from malformed runtime JSON. Consider schema validation (e.g. Zod) at the network boundary: parse incoming WebSocket frames and REST responses against a schema before passing them to adapters. Discard and log frames that fail validation rather than letting malformed data reach React state.

This is an implementation choice — weigh bundle size, parse overhead, and safety before adding it as a dependency.

---

## 15. CP7 frontend checkpoint order

| Step | Deliverable | Exit proof |
| --- | --- | --- |
| CP7a | Demo API additions | CLI verification only (done) |
| CP7b | Next.js bootstrap + quality tooling | lint / typecheck / build pass |
| CP7c | Tokens + Dockview workspace with default aviation layout | TopNavigation + MapWidget + AlertWidget visible; panels resizable; approved SVG mockup must exist before this step begins |
| CP7d | Authentication + login page | 401 redirects; cookie auth works |
| CP7e | REST map hydration | real Redis entities on map |
| CP7f | Live position WebSocket | aircraft moves without page refresh |
| CP7g | Filters | filters do not corrupt underlying entity state |
| CP7h | REST alert hydration | existing SIGNAL_LOSS alerts appear |
| CP7i | Live alert WebSocket feed | new alert appears without page refresh |
| CP7j | Dedupe + stale guards | duplicate / stale simulation passes |
| CP7k | Reconnect reconciliation | disconnect → reconnect → hydrated |
| CP7l | Demo expiry UX | WS close 4401 shows banner; redirect works |
| CP7m | Full E2E demonstration | aircraft → dark → alert → alert panel |
