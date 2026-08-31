# Map Widget: Concepts and Implementation Overview

Covers the MapLibre GL and deck.gl integration, the worker URL problem and its fix,
the layer boundary, and the header controls added to the map panel.

---

## What was built

A fully functional map widget rendering a CARTO Dark Matter basemap with five sample aircraft
positions as deck.gl scatter dots. The widget lives in the left panel of a resizable split
layout. The header contains the title "GLOBAL MAP" on the left and three action controls on
the right: a 2D/3D segmented toggle, a panel-swap icon button, and a fullscreen icon button.
The map canvas fills the remaining height below the header, with a custom attribution line
pinned to the bottom-right corner.

---

## Rendering architecture (ADR-017 + ADR-019)

Two independent rendering systems are composed inside the same DOM container.

MapLibre GL owns the geographic environment: it fetches vector tiles, rasterises terrain,
labels, and coastlines using its own WebGL context, and manages the camera (pan, zoom, pitch,
bearing, projection).

deck.gl owns the Sentinel data visualization: it renders a second canvas positioned on top of
the MapLibre canvas using `interleaved: false`. Aircraft markers, future vessel tracks, and
alert geometry all live on the deck.gl canvas. The two canvases never share a WebGL context,
which avoids pipeline conflicts in Turbopack's dev mode.

`MapboxOverlay` from `@deck.gl/mapbox` bridges the two systems. It implements MapLibre's
`IControl` interface so it can be attached via `map.addControl()`. After that initial
attachment, layer updates go through `overlay.setProps({ layers })` — the map and overlay
are never recreated for a data change.

The `MapLayerDefinition` interface (ADR-019) defines the boundary between the platform and
domain layers. Each definition carries a stable id, a human-readable label, an
`enabledByDefault` flag, and a `createLayer(data, filters)` factory that returns a deck.gl
Layer. Data flows in from outside; the definition itself carries no data and no fetch logic.
This separation means live telemetry can be wired in without touching the MapLibre or deck.gl
lifecycle.

The aviation layer is the first implementation of this interface.

---

## Map lifecycle and React Strict Mode safety

MapLibre GL is not a React component. It takes a DOM element, creates internal WebGL state,
and must be explicitly destroyed. This conflicts with React's ownership model.

The `useEffect` in `MapWidget` manages the lifecycle explicitly:

- A `mapRef` guards against creating a second live instance. If the ref is already set, the
  effect returns immediately. This handles genuine concurrent mount attempts (e.g. two widgets
  of the same type rendered at once) without breaking React Strict Mode's intentional
  setup → cleanup → setup double-invoke.
- A `destroyed` boolean flag is set in the cleanup function before `map.remove()` is called.
  The `map.on('load')` callback checks `destroyed` before attaching the deck.gl overlay.
  Without this flag, a cleanup that fires before the style finishes loading would set
  `mapRef.current = null` while the load callback still has a reference to the map and
  tries to call `map.addControl()` on a destroyed instance.
- A `ResizeObserver` on the map container calls `map.resize()` whenever the container
  dimensions change. This handles SplitLayout divider drags and panel swaps without
  reinitialising the map.

Cleanup order matters: `resizeObserver.disconnect()` first, then `map.remove()`, which
internally removes all attached controls including the deck.gl overlay.

---

## The Turbopack worker URL problem

MapLibre GL v6 separates tile parsing into a Web Worker for performance. The worker runs
tile decoding off the main thread so the UI stays responsive during pan and zoom.

When MapLibre creates the worker, it derives the worker's URL from `import.meta.url` — the
URL of the maplibre-gl bundle itself. It replaces the filename with `maplibre-gl-worker.mjs`
and creates a module worker at that address. When Turbopack bundles maplibre-gl, the bundle
URL is something like `/_next/static/chunks/1nh9_maplibre-gl..._.js`, so the computed worker
URL becomes `/_next/static/chunks/maplibre-gl-worker.mjs`. That path does not exist as a
Turbopack chunk because Turbopack cannot statically analyse MapLibre's dynamic URL
construction pattern and therefore does not emit a worker bundle for it. When the browser
requests the URL it gets a Next.js HTML 404 page. The browser then rejects it with:
`Failed to load module script: text/html MIME type`.

The fix: copy the pre-built worker file and its shared module from the maplibre-gl package
into `public/` so they are served as static assets at `/maplibre-gl-worker.mjs` and
`/maplibre-gl-shared.mjs`, then call `setWorkerUrl('/maplibre-gl-worker.mjs')` once at
module load time before any Map is instantiated. `setWorkerUrl` is a MapLibre GL v6 named
export that overrides the computed URL. The worker imports the shared module via a relative
path, so both files must be present at the same path level in `public/`.

This is a Turbopack-specific workaround. A production Webpack build does not have this
problem because Webpack's worker bundling follows static `new URL(...)` patterns.

---

## Tile provider selection

| Provider | Style URL | API key | Notes |
| --- | --- | --- | --- |
| CARTO Dark Matter | `https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json` | None | Primary. High-contrast dark theme suited to operational use. Required attribution: © CARTO, © OpenStreetMap |
| OpenFreeMap dark | `https://tiles.openfreemap.org/styles/dark` | None | Fallback. Lower contrast, suitable as backup. OSM data (ODbL) |

The primary style is overridable via the `NEXT_PUBLIC_MAP_STYLE` environment variable. The
fallback fires automatically if the primary style emits a load error. Both providers require
attribution; the built-in `attributionControl` is disabled and a custom attribution overlay
is rendered in React to shorten the text to "© CARTO, © OpenStreetMap".

---

## Aviation layer

The first real `MapLayerDefinition` implementation. Uses deck.gl's `ScatterplotLayer` to
render aircraft positions as blue filled circles with a lighter blue stroke.

Each dot is 12,000 metres radius so it remains visible at global zoom (zoom 2). The layer
is entirely stateless: `createLayer(data, filters)` receives the position array and returns
a fresh deck.gl layer object. Updating positions in a future checkpoint means calling
`overlay.setProps` with a new layer instance — no map or overlay recreation required.

Sample positions are hardcoded for this checkpoint (five commercial flights over Europe and
North America). Live WebSocket data replaces this array in a later checkpoint without any
change to the layer definition or the MapWidget lifecycle.

---

## Header controls

Three controls are added to the `WidgetHeader` `actions` slot:

**2D / 3D toggle:** A segmented button pair. Clicking 3D calls `map.setProjection` with
`type: 'globe'`, which switches MapLibre from Mercator flat projection to a spherical globe.
Clicking 2D restores mercator. The active button is highlighted with the informational blue
design token. The `is3D` state lives in `MapWidget` because it is purely a map concern and
nothing outside the widget needs to know about it.

**Layout swap:** Clicking this button calls `onToggleLayout`, a prop passed from `page.tsx`.
`page.tsx` tracks `swapped` (which panel is on which side) and `mapPct` (the map's percentage
of the total width, always expressed as the map's share regardless of which side it is on).
When swapped, the left panel receives `100 - mapPct` so the workspace gets its historic width
and the map gets its historic width. Dragging the divider while swapped inverts the delta back
to `mapPct` so the next swap restores the correct widths for both panels.

**Fullscreen:** Uses the browser Fullscreen API on `outerRef`, which covers the entire widget
including the header. Pressing Escape exits fullscreen via the browser's native behaviour.

---

## File structure

The map widget spans the following files in the dashboard service:

- `public/maplibre-gl-worker.mjs` — tile-parsing worker, static copy served at known URL
- `public/maplibre-gl-shared.mjs` — shared utilities imported by the worker
- `src/app/page.tsx` — client component that owns `swapped` and `mapPct` state
- `src/shell/SplitLayout.tsx` — controlled split; accepts `leftPct` and `onLeftPctChange`
- `src/widgets/map-widget/MapWidget.tsx` — lifecycle, controls, layout
- `src/widgets/map-widget/mapStyle.ts` — provider URLs and env var override
- `src/widgets/map-widget/types.ts` — `MapLayerDefinition` interface
- `src/widgets/map-widget/layers/aviationLayer.ts` — first `MapLayerDefinition` implementation

---

## Key decisions

| Decision | Choice | Reason |
| --- | --- | --- |
| deck.gl interleave mode | `interleaved: false` | Separate canvas avoids WebGL context conflicts with Turbopack dev mode |
| Worker delivery | Static file in `public/` | Turbopack cannot bundle MapLibre's dynamic worker URL; static serving is the simplest correct fix |
| Projection API | `map.setProjection` with globe or mercator type | MapLibre GL v6 native; no additional packages needed |
| Attribution | Custom React overlay | Shortens legal text without removing required attribution |
| Width tracking | `mapPct` in page.tsx (map's share, always) | Ensures toggling layout preserves both panels' widths without coordinate transformation on every drag event |
| Canvas context | `antialias: true`, `powerPreference: 'high-performance'` | MSAA sharpens vector tile edges; GPU hint improves tile render throughput |
