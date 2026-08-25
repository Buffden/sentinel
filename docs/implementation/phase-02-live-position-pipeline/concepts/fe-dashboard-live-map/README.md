# FE Dashboard: Scaffolding + Live Map + Flight Rendering

## What this covers

Next.js dashboard scaffold, react-leaflet live map, moving flight markers with tooltips, and
viewport-aware rendering with filters. This closes the Phase 02 vertical slice: an operator
opens the dashboard and sees live flights moving on a map.

---

## Data inventory: what we already have

The position consumer writes everything we need. No new backend fields are required.

| Field | Source | Map use |
| --- | --- | --- |
| `entity_id` | Redis hash + pub/sub | Marker key, tooltip |
| `callsign` | Redis hash + pub/sub | Tooltip, search filter |
| `lat`, `lon` | Redis hash + pub/sub | Marker position |
| `altitude_m` | Redis hash + pub/sub | Tooltip, altitude filter, marker color band |
| `speed_mps` | Redis hash + pub/sub | Tooltip |
| `course_deg` | Redis hash + pub/sub | Marker rotation (direction arrow) |
| `on_ground` | Redis hash | Airborne/ground toggle filter |
| `entity_subtype` | Redis hash | Type filter (fixed_wing, rotorcraft, uav, etc.) |
| `live_geo_cell` | Redis hash + pub/sub | Viewport culling (see below) |
| `last_seen_ms` | Redis hash | Staleness indicator in tooltip |

The pub/sub payload already carries: `entity_id`, `entity_type`, `timestamp_ms`, `lat`, `lon`,
`altitude_m`, `speed_mps`, `course_deg`, `callsign`, `live_geo_cell`.

For `on_ground` and `entity_subtype` (not in pub/sub payload): the API seeds the initial
state from `entity:live:{entity_id}` on page load and the client tracks them locally.
Position updates refresh only the fields in the pub/sub payload; the client merges.

---

## The viewport problem

OpenSky can return 5,000+ aircraft globally. Rendering 5,000 moving Leaflet markers is a
DOM performance failure. Sending 5,000 position updates per tick over WebSocket is a bandwidth
failure. We need to scope both the initial load and the live stream to what is actually visible.

### Our options

#### Option A: client-side bounding box filter (simplest, does not scale)

Server sends every position update to every client. Client drops updates whose `lat`/`lon`
falls outside the current Leaflet map bounds.

- Pros: no API changes, zero server logic.
- Cons: full global stream over WebSocket, client CPU for every update.
- Verdict: acceptable for local dev with a bounded OpenSky bbox. Not production-ready.

#### Option B: server-side bounding box filter

Client sends current viewport bbox `{minLat, maxLat, minLon, maxLon}` to API on pan/zoom.
API checks `lat`/`lon` of each pub/sub message before forwarding to that client.

- Pros: simple geometry, easy to reason about.
- Cons: lat/lon comparison on every message for every connected client. Does not use our
  existing spatial index. The bbox has to be re-sent on every pan/zoom event.
- Verdict: workable but ignores the H3 investment.

#### Option C: server-side H3 cell subscription (recommended)

The H3 sorted sets (`geo-cell:{cell}`) already exist in Redis. The API can use them in two ways:

**Initial load:** client sends its viewport bbox. API computes the H3 cells (at resolution 3
or 4, covering large areas) that overlap the bbox using `h3.polygonToCells`. For each cell,
`ZRANGE geo-cell:{cell} 0 -1` returns entity IDs. API fetches `HGETALL entity:live:{entity_id}`
for each and sends the initial snapshot.

**Live stream:** after the initial load, the client sends its set of subscribed H3 cells to
the API. When a `position-updates` message arrives, the API checks `live_geo_cell` against the
client's subscribed cell set. If the cell is in the set, forward; otherwise drop.

On pan/zoom: client computes new subscribed cells (Leaflet bounds + `h3.polygonToCells`) and
sends a `subscribe` message to update the set. API swaps the cell set for that connection.

This means:
- Initial load is a spatial query, not a full scan.
- The live stream is one set-membership check per message per connection.
- We reuse the existing Redis spatial index instead of adding new server logic.

**H3 resolution for viewport subscriptions:** resolution 3 cells are ~12,000 km2 (roughly
the size of a small country). Resolution 4 cells are ~1,700 km2. At typical map zoom levels
showing a country or region, 10-30 cells cover the viewport cleanly. Fewer cells means a
simpler subscription set.

Note: `live_geo_cell` in the pub/sub payload is at `LIVE_H3_RESOLUTION` (set in the position
consumer). The viewport subscription resolution should match or be coarser. If they differ, the
API must check whether the pub/sub cell is a child of any subscribed cell using `h3.cellToParent`.
Simplest path: use the same resolution for both.

#### Option D: client-side clustering (future)

Libraries like `supercluster` cluster nearby markers at low zoom and expand at high zoom.
This reduces DOM node count. Orthogonal to viewport culling: do option C first, add clustering
later if needed.

### Chosen approach for this phase

**Option A for initial implementation, Option C as the target.**

Start with Option A (client-side bbox filter) to get the map working end-to-end quickly.
Document Option C as the upgrade path. The API endpoint and WebSocket protocol should be
designed to support Option C without a breaking change.

---

## Filters

These are the filters that are meaningful given our data. All are client-side except the
viewport subscription, which is server-side.

### Viewport (implicit, always active)

Not a UI control. The visible map bounds define what is rendered. Handled via Option A/C above.

### Airborne / on ground toggle

`on_ground` boolean from the initial seed. Default: show airborne only (on_ground = false).
Toggle to also show ground traffic. Simple because the field is already in the Redis hash.

### Entity subtype

`entity_subtype` values from OpenSky: `fixed_wing`, `rotorcraft`, `uav`, `lighter_than_air`,
`unknown`. Multi-select checkboxes. Lets the operator focus on e.g. UAVs only.

### Altitude band

`altitude_m` range slider: min/max in metres (or feet, converted for display). Hides aircraft
outside the selected altitude range. Useful for focusing on low-altitude traffic or cruise-level
traffic separately. Null altitude (no transponder altitude) is treated as visible unless the
operator explicitly hides unknowns.

### Callsign search

Text input. As the operator types, markers for non-matching callsigns dim (opacity 0.2) rather
than disappear entirely. The matched flight stays visible. This is a soft filter: it highlights
rather than hides, so spatial context is preserved.

### Speed range (optional, deprioritised)

`speed_mps` range. Lower priority than the above four. Add if there is a clear need.

### What we do NOT filter on in v1

- Squawk code (7500 hijack, 7700 emergency): interesting but needs dedicated alert handling,
  not a map filter.
- Origin/destination: not in ADS-B data.
- Airline/operator: not in ADS-B data.

---

## Initial load strategy

On page load and on viewport change:

1. Client sends `GET /entities/live?bbox={minLat},{minLon},{maxLat},{maxLon}` to the API.
2. API scans `entity:live:*` keys whose `lat`/`lon` fall within the bbox (Option A), or
   queries the H3 sorted sets for the covering cells (Option C).
3. Returns array of entity snapshots with all filter fields.
4. Client renders initial markers.
5. Client opens WebSocket, sends `subscribe` message with the same bbox (or H3 cells).
6. API starts forwarding `position-updates` pub/sub messages that match.

The seed call prevents a blank map on first load (pub/sub is at-most-once; without the seed,
the client waits for the next update cycle to see anything).

---

## WebSocket message protocol (client-API)

```
// client -> API: declare viewport for live stream filtering
{ "type": "subscribe", "bbox": { "minLat": 49, "minLon": -8, "maxLat": 61, "maxLon": 2 } }

// API -> client: live position update (forwarded from position-updates pub/sub)
{
  "type": "position",
  "entity_id": "abc123",
  "entity_type": "aircraft",
  "timestamp_ms": 1787634583000,
  "lat": 51.5,
  "lon": -0.1,
  "altitude_m": 10150,
  "speed_mps": 220.5,
  "course_deg": 270,
  "callsign": "BA100",
  "live_geo_cell": "87194ad33ffffff"
}
```

The `type` field namespaces position updates from alert events (added in Phase 03).

---

## Marker design

Each aircraft is a small directional arrow SVG rotated to `course_deg`. This is standard in
flight tracking UIs and immediately readable.

Marker states:
- Normal: white arrow, dark outline.
- Stale (no update for > 60s): grey, reduced opacity.
- On ground: square icon instead of arrow.
- Matched by callsign search: highlighted (yellow border).

Color band by altitude (optional, Phase 02 scope):
- < 1,000 m: low (orange)
- 1,000 - 8,000 m: mid (blue)
- > 8,000 m: cruise (white)

---

## Tooltip content

Shown on marker hover:

```
BA100                    (callsign, or "---" if null)
ICAO: abc123             (entity_id)
Alt: 10,150 m            (altitude_m, formatted)
Speed: 793 km/h          (speed_mps * 3.6)
Course: 270 deg          (course_deg)
Type: fixed_wing         (entity_subtype)
Last seen: 3s ago        (now - last_seen_ms)
```

`last_seen_ms` ages in real time using a client-side interval so the operator can see
when a flight is going stale without waiting for the next pub/sub tick.

---

## Component structure

```
app/
  page.tsx                 (root: map + filter panel layout)
  components/
    MapView.tsx            (react-leaflet MapContainer + tile layer)
    FlightMarker.tsx       (single marker, tooltip, rotation from course_deg)
    FlightLayer.tsx        (renders all visible FlightMarker instances)
    FilterPanel.tsx        (airborne toggle, subtype checkboxes, altitude slider, callsign search)
  hooks/
    useLivePositions.ts    (WebSocket connection, position update merge, client-side filter apply)
    useViewportSeed.ts     (GET /entities/live on mount and on map move end)
```

`useLivePositions` holds a `Map<entity_id, EntityState>` in a ref (not state, to avoid
re-render on every tick). A debounced `setState` flushes the map to React state at ~5Hz
so the UI updates smoothly without thrashing.

---

## API endpoints needed

| Endpoint | Purpose |
| --- | --- |
| `GET /entities/live?bbox=...` | seed map on load and viewport change |
| `WS /ws` | live position stream + alert stream (Phase 03 adds alerts to same socket) |

The `GET /entities/live` endpoint queries Redis `entity:live:*` with a bbox filter. For
Option A it scans all keys and filters by lat/lon. For Option C it uses `h3.polygonToCells`
+ `ZRANGE geo-cell:{cell}`.

---

## What is explicitly out of scope for this phase

- Alert overlays on map (Phase 03).
- Route path history polylines (future).
- Clustering (future).
- Entity detail panel / click-through (Phase 09).
- Authentication (Phase 03 adds auth; the map can be unauthed locally for now).
