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

Target CP7 layout:

```text
┌──────────────────────────────────────────────────────────────┐
│ SENTINEL       Live Operations              ● LIVE    USER  │
├──────────┬────────────────────────────────────┬──────────────┤
│          │                                    │              │
│ FILTERS  │                                    │   ALERTS     │
│          │                                    │              │
│ callsign │                                    │ SIGNAL LOSS  │
│ type     │               MAP                  │              │
│ altitude │                                    │ SIGNAL LOSS  │
│ status   │                                    │              │
│          │                                    │              │
│          │                                    │              │
├──────────┴────────────────────────────────────┴──────────────┤
│ Connection status / entity count                             │
└──────────────────────────────────────────────────────────────┘
```

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
│   ├── widgets/                    large screen regions
│   │   ├── app-shell/
│   │   ├── top-navigation/
│   │   ├── filter-rail/
│   │   ├── map-workspace/
│   │   └── alert-panel/
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

**`widgets`** — large screen regions (TopNavigation, FilterRail, MapWorkspace, AlertPanel). Compose features and entity components. Not generic — `MapWorkspace` is a Sentinel widget, not a reusable grid primitive.

**`features`** — user-facing behavior: authentication, demo-session, entity-filtering, live-feed, connection-status. Distinction from entities: `AircraftMarker` is an entity UI; "filter aircraft by altitude" is a feature.

**`entities`** — domain business objects: `TrackedEntity`, `Alert`. Later: `Investigation`, `Workspace`, `Route`. These carry the frontend domain model, not raw API JSON.

**`shared`** — zero Sentinel business knowledge. Panel, StatusBadge, HTTP client, WebSocket wrapper, date formatter, env config. If `shared/` grows `SignalLossAlert` or `Aircraft`, those are in the wrong layer.

---

## 4. Network boundary and adapter strategy

Do not let React components consume raw backend JSON directly. Every API response passes through an adapter function that converts the wire DTO to the frontend domain model before reaching React.

Both `last_seen_ms` (REST) and `timestamp_ms` (WebSocket) map to a single `eventTimeMs` field in the frontend model. React never knows which field name the backend used.

```text
REST / WebSocket
      ↓
Runtime validation (optional Zod — see §14)
      ↓
DTO (matches wire format)
      ↓
Adapter function
      ↓
Frontend domain model
      ↓
React
```

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
| Filter state | operations page, passed down to FilterRail |
| Auth / session | AuthGuard; cookie managed by server |
| Socket lifecycle | shared websocketClient |

Redux Toolkit / Zustand becomes justified only when selected-entity state must coordinate between map, detail panel, timeline, and investigation (Phase 09+). Do not add it now.

---

## 8. WebSocket separation

Never open a WebSocket inside a component. The layers are:

```text
MapWorkspace / AlertPanel
        ↓
useLiveFeed()                  (features/live-feed)
        ↓
websocketClient.ts             (shared/realtime)
        ↓
WebSocket
```

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

The shell uses a CSS Grid with two rows: a fixed-height top navigation bar and a flexible content row that fills the remaining viewport height. The content row is a three-column grid: fixed-width filter rail, flexible map workspace, fixed-width alert panel.

The map column must use `minmax(0, 1fr)` — without the zero minimum, Leaflet overflow becomes unmanageable. MapWorkspace can later split into map + EventTimeline without changing the outer shell.

---

## 11. Component tree for CP7

```text
SentinelOperationsPage
└── AppShell
    ├── TopNavigation
    │   ├── SentinelLogo
    │   ├── OperationsNavigation
    │   ├── ConnectionIndicator
    │   └── SessionMenu
    ├── FilterRail
    │   ├── CallsignSearch
    │   ├── GroundStateFilter
    │   ├── EntitySubtypeFilter
    │   └── AltitudeFilter
    ├── MapWorkspace
    │   ├── SentinelMap
    │   └── AircraftMarker × N
    │       └── AircraftTooltip
    └── AlertPanel
        └── SignalLossAlertCard × N
```

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
| CP7c | Tokens + static dashboard shell | visually matches reference PNG |
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
