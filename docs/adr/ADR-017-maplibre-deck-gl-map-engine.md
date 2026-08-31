# ADR-017: Replace react-leaflet with MapLibre GL + deck.gl as the Map Engine

**Status:** Accepted
**Date:** 2026-08-31
**Supersedes:** Map engine portion of ADR-016 (Next.js + Blueprint.js remain accepted)

---

## Context

ADR-016 chose react-leaflet as a direct open-source equivalent to ngx-leaflet from ADR-009. react-leaflet has not been used in production code yet; the dashboard has not been built.

Sentinel is evolving toward a general-purpose real-time geospatial workspace (see Product North Star in CLAUDE.md). The map must support:

- High-performance rendering of large point clouds, track trails, and polygon overlays at interactive framerates
- A composable, registry-driven layer model so domain layers (aviation, maritime, weather, infrastructure) can be added independently without modifying the map component
- Tile-level customisation for a dark operational aesthetic

react-leaflet is a DOM-based SVG/Canvas renderer. At world-scale data volumes, rendering performance degrades and there is no first-class WebGL layer model. deck.gl integration over Leaflet requires workarounds and does not compose cleanly.

MapLibre GL JS is the community-maintained open-source fork of Mapbox GL JS following the Mapbox re-licensing. It is WebGL-native, actively maintained, and has no usage-based pricing. deck.gl is the WebGL visualization framework purpose-built for large-scale geospatial rendering; it integrates with MapLibre via the deck.gl/maplibre interleaving API, allowing deck.gl layers to be composited correctly with MapLibre vector tile layers.

The cost of switching is zero: no map code exists yet.

---

## Decision

Replace react-leaflet with **MapLibre GL JS + deck.gl** as the Sentinel map rendering engine.

React integration: use `react-map-gl` (MapLibre flavour) or a thin custom hook wrapping `maplibre-gl` directly. deck.gl layers are registered against the MapLibre instance using the deck.gl/maplibre interleaving API.

react-leaflet is removed from the accepted stack.

---

## Reasoning

**WebGL-native rendering is required for world-scale data.** Rendering hundreds of moving entities, historical tracks, H3 cell overlays, and domain-specific polygon layers simultaneously at 60fps is not feasible with SVG/Canvas Leaflet at interactive framerates. MapLibre GL + deck.gl handle large point counts via GPU instancing.

**The layer registry model fits the platform direction.** deck.gl's layer API is composable and data-driven. Each domain registers its own deck.gl layer definitions. The Map widget renders them without prior knowledge of domain specifics. This is the correct abstraction boundary for a multi-domain workspace.

**MapLibre GL is open and production-quality.** It is the direct API-compatible successor to Mapbox GL JS with no proprietary dependency. Dark vector tile styles (from MapTiler, Stadia, or self-hosted) produce the operational aesthetic Sentinel targets.

**Portfolio signal.** MapLibre GL + deck.gl is the stack that Palantir, Esri, Uber, and companies building operational geospatial platforms use. It is a stronger signal than react-leaflet for this class of product.

---

## Alternatives Considered

### Keep react-leaflet

- Adequate for the aviation MVP with a small number of entities
- Becomes a bottleneck when maritime, weather, or infrastructure layers are added
- No first-class WebGL layer model; deck.gl integration over Leaflet requires unsupported workarounds
- Rejected: cost of switching is zero now; cost later is high

### Mapbox GL JS

- Proprietary license with usage-based pricing above a free tier
- MapLibre GL is the open-source equivalent with the same API; no reason to choose Mapbox

### Google Maps Platform

- Proprietary and pricing-dependent
- Not suitable for a self-hosted operational platform

---

## Consequences

- react-leaflet is removed from the accepted stack. `CLAUDE.md` Fixed Stack updated: Dashboard row loses react-leaflet; Map engine row added as MapLibre GL + deck.gl.
- ARCHITECTURE.md must be updated to reflect the map engine change when the dashboard phase begins.
- Aviation is the first map layer implementation. Do not build generic multi-domain layer abstractions before the aviation layer exists and is proven working.
- The map is a workspace widget (ADR-018), not the application shell. MapLibre initialises inside the Map widget component, not at the application root.
- Tile provider is an implementation choice, not an architectural one. Select at dashboard implementation time.
