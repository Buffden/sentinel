# ADR-015: v1 Reference Route Model for Deviation Detection

**Status:** Accepted
**Date:** 2026-08-10

---

## Context

ADR-014 introduced the Deviation Detector as a stateless service that compares each normalised position against a route baseline and publishes `OUT_OF_RANGE` / `IN_RANGE` events to `deviation.candidates`. The original design described that baseline as a TimescaleDB continuous aggregate (`route_baseline`) that computes the per-entity per-hour average latitude and longitude from position history.

This approach has two fundamental problems:

1. **The algorithm is incorrect.** A commercial aircraft flies a consistent path: departure → cruise → approach. Averaging the lat/lon across all positions in an hour produces a coordinate somewhere over the route midpoint — not on the actual path flown. A deviation detector comparing position to this average would raise false positives for normal flight segments at the beginning or end of the route and miss true deviations from the expected track.

2. **Cold-start problem.** Reliable statistical inference of a route requires weeks of repeated flights. OpenSky does not provide 30 days of history for every aircraft in the monitored airspace. A newly seen entity has no baseline to compare against, and a sparse one produces noise.

Statistical route learning is a useful future capability (trajectory clustering, sequential waypoint inference from historical tracks). It is not implementable correctly with the data available in v1.

---

## Decision

Route deviation in v1 uses a **deterministic reference route** model, not a statistical one.

### Reference route store

Two plain PostgreSQL tables hold explicitly defined route data for synthetic entities:

```sql
route_references (
  route_id           TEXT PRIMARY KEY,
  entity_id          TEXT NOT NULL,
  route_name         TEXT NOT NULL,
  corridor_threshold_metres REAL NOT NULL,
  source             TEXT NOT NULL,  -- 'synthetic' | 'manual'
  created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
)

route_reference_points (
  route_id           TEXT NOT NULL REFERENCES route_references(route_id),
  sequence_no        INTEGER NOT NULL,
  lat                DOUBLE PRECISION NOT NULL,
  lon                DOUBLE PRECISION NOT NULL,
  PRIMARY KEY (route_id, sequence_no)
)
```

This schema replaces the single flat `reference_routes` table used previously. Separating the route header from its waypoints makes it easier to assign a per-corridor threshold and describe the route metadata.

### Deviation Detector logic

1. On each `position.normalized` event, look up whether the entity has an assigned route in `route_references`.
2. If none: skip — no event emitted. Real ADS-B/AIS entities with no assigned route produce no deviation candidates.
3. If found: fetch all `route_reference_points` for the route, find the nearest **route segment** (not nearest waypoint) by computing minimum perpendicular distance from the entity's position to each segment (point[i] → point[i+1]). If the perpendicular foot falls outside the segment, use the distance to the nearer endpoint. Publish `nearest_segment_index` (index `i` of the segment start) and `deviation_metres`.
4. If `deviation_metres > corridor_threshold_metres`: publish `OUT_OF_RANGE`.
5. Otherwise: publish `IN_RANGE`.

### Real entities

Live ADS-B aircraft and AIS vessels have no assigned reference route in v1 and are silently skipped by the Deviation Detector. Route deviation demonstrations use synthetic entities injected by the load generator, which assigns a known reference route at launch.

### Historical analytics aggregate (renamed, optional)

If a TimescaleDB continuous aggregate over position history is retained, it must be named `position_hourly_summary` and used exclusively for analytical investigation (heat maps, track replays). It must not be used as input to anomaly detection.

---

## Reasoning

**Correctness over completeness.** A deterministic reference route produces exactly the anomalies the demo expects, with no false positives from averaging. A statistical route would require explaining why the "baseline" is wrong for a flight on a different leg of the route — not a useful interview conversation.

**Demonstrable in a demo.** The load generator controls where synthetic entities go. Deviating a synthetic entity from its reference route produces a predictable, injectable anomaly that can be scripted for a live demo.

**Honest scope boundary.** Documenting that real entities are excluded in v1 is more defensible than deploying a broken algorithm. The v2 path (trajectory clustering, ML-based route inference) is a natural extension.

---

## Alternatives Considered

### Statistical lat/lon averaging per time bucket (rejected)

Described above — the algorithm is geometrically incorrect for any non-trivial route. Averaging A→B→C positions does not produce the expected route corridor. Rejected as incorrect.

### Sequence of lat/lon bounding corridors (manual for real entities, rejected)

Could work, but requires curating reference routes for every real entity in the monitored airspace — which grows without bound. Not feasible in v1. Synthetic entities with assigned routes achieve the same demo outcome without the maintenance burden.

### Defer route deviation entirely to v2 (rejected)

US-04 is part of the defined use case set. Removing it entirely would shrink the demonstrated surface area. The reference-route model gives a correct, deliverable implementation within the scope.

---

## Consequences

- `route_baseline` continuous aggregate is not created. The init script does not include it.
- `route_references` and `route_reference_points` tables are created by the init script and seeded by the load generator at startup.
- The Deviation Detector reads from `route_reference_points` (not from any aggregate or position history table).
- Phase 04, Phase 01, DATA_MODEL.md, and ARCHITECTURE.md all describe the schema as defined here.
- Real ADS-B/AIS deviation detection is explicitly out of scope in v1. This constraint must be documented in US-04.
- Future work: v2 route-learning from historical tracks using trajectory clustering (DBSCAN, Frechet distance) over N days of position history.
