# ADR-019: Map Rendering Architecture — Separation of Base Map and Data Rendering

**Status:** Accepted
**Date:** 2026-08-31
**Depends on:** ADR-017 (MapLibre GL + deck.gl), ADR-018 (Dockable Workspace)

---

## Context

Sentinel is intended to support multiple geospatial domains over time.

Different domains may require different visual representations of the world and different kinds of data to be rendered on top of it. For example, aviation, maritime, weather, infrastructure, disasters, and other future domains may not share the same basemap appearance or rendering requirements.

If the Map widget directly contains domain-specific rendering logic, every new domain would require modifying the Map widget itself. This would increasingly couple the map engine to individual Sentinel features.

Sentinel therefore needs a stable boundary between:

1. the geographic environment rendered by the map engine; and
2. Sentinel data rendered on top of that environment.

---

## Decision

The Sentinel Map widget will use a layered rendering architecture.

### MapLibre owns the geographic environment

MapLibre GL is responsible for rendering the underlying geographic context.

This includes concerns such as:

- basemap style
- geographic labels
- borders and coastlines
- roads and infrastructure represented by the basemap
- camera and viewport
- optional terrain or other MapLibre-native geographic rendering features

These concerns belong to the Map widget and map engine.

They are independent from Sentinel's operational data.

---

### deck.gl owns Sentinel data visualization

Operational and analytical data displayed over the map will be rendered as deck.gl layers.

Examples may include:

- aircraft
- flight tracks
- vessels
- weather overlays
- alerts
- routes
- H3 cells
- infrastructure events
- fires
- satellite objects
- other future geospatial datasets

The Map widget must not contain knowledge such as:

> "render aircraft this way"

or:

> "if maritime mode, render these markers."

Instead, domain-specific code supplies layer definitions to the Map widget.

The Map widget's responsibility is to compose those layers with MapLibre.

---

## Layer definition boundary

A map layer should expose only the information required for the Map widget to manage it.

At minimum, a layer definition requires:

- stable layer ID
- human-readable label
- default enabled state
- a way to create or provide the corresponding deck.gl layer

Layer-specific filtering or configuration may also be supplied when required by the layer.

The exact abstraction should remain minimal and evolve when additional real domains require it.

Do not create a speculative generic plugin or factory framework before multiple real consumers exist.

---

## Map instance state

Layer configuration belongs to the individual Map widget instance.

This includes state such as:

- enabled layers
- layer filters
- viewport
- zoom
- pitch
- bearing

Therefore, two Map widgets may display different geographic areas and different combinations of layers at the same time.

This state is not inherently global application state.

---

## Layer controls

Controls for map layers belong inside the Map widget.

The Map widget may expose a layer overlay that allows the operator to:

- discover available layers
- enable or disable layers
- expand layer-specific configuration
- search the layer list
- collapse the controls when more map space is needed

This control surface is part of the Map widget, not a separate global workspace sidebar.

---

## Rendering integration

deck.gl integrates with MapLibre through the supported MapLibre overlay integration.

MapLibre remains responsible for the geographic rendering environment while deck.gl renders Sentinel data within the same map view.

The overlay should be created as part of the Map widget lifecycle and updated as layer data or configuration changes.

High-frequency data updates should update layer data rather than recreate the entire map engine.

---

## Data ownership boundary

The Map widget is a renderer.

It does not own application data transport.

Specifically, the Map widget must not:

- open its own WebSocket
- consume Kafka or Redis directly
- own application authentication
- become the global source of entity state

Data reaches the Map widget through frontend application boundaries such as props, hooks, or domain state owned outside the rendering engine.

This keeps transport, domain state, and rendering independently testable.

---

## Extensibility

The architecture must allow the geographic environment and the Sentinel data layers to evolve independently.

A future domain may require a different:

- basemap style
- camera configuration
- terrain configuration
- set of deck.gl layers

Supporting those differences must not require redesigning the fundamental Map widget architecture.

However, this ADR defines the architectural boundary only.

It does not require implementing future map modes before they are needed.

---

## Initial implementation

The first implementation exercises this architecture using aviation.

The aviation implementation proves:

- MapLibre as the underlying map renderer
- deck.gl as the operational data layer renderer
- an aviation data layer
- Map-widget-scoped layer controls
- separation between realtime data transport and map rendering

Future domains extend the same architecture when they are implemented.

Detailed aviation behavior belongs in the relevant implementation phase documentation rather than this ADR.

---

## Consequences

### Positive

- New domains do not require rewriting the Map widget.
- Geographic presentation and operational data remain independent.
- Multiple datasets can be composited on one map.
- Multiple Map widget instances can maintain independent configurations.
- Map rendering remains independent from realtime transport and application state.
- MapLibre-native capabilities can evolve without changing domain layer implementations.

### Trade-offs

- Domain layers must conform to a common map-layer boundary.
- Some future rendering requirements may require extending that boundary.
- MapLibre and deck.gl lifecycle coordination must be handled carefully.
- State ownership between workspace, Map widget, and individual layers must remain explicit.

---

## Non-goals

This ADR does not decide:

- which tile provider Sentinel will use
- which satellite imagery provider will be used
- which DEM provider will supply terrain
- maritime styling
- weather rendering implementation
- persistence of map preferences
- a generic domain plugin framework
- a domain-switching UI

Those decisions are made when a concrete feature requires them.
