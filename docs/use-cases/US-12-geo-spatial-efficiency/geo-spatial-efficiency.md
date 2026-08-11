# US-12: Efficient Geospatial Access

**Actor:** System
**Status:** Defined

---

## Story

As the system, I want historical regional queries and live proximity candidate lookup to avoid unnecessary full scans as entity volume grows.

---

## Acceptance Criteria

- Historical region + time-window queries use TimescaleDB time chunk exclusion plus the `(geo_cell, observed_at DESC)` index.
- `geo_cell` is explicitly treated as an indexed column, not a TimescaleDB partition/sharding dimension.
- The API translates a requested historical region into `HISTORY_H3_RESOLUTION` cells before querying `position_history`.
- Live proximity lookup uses Redis `geo-cell:*` sorted sets at `LIVE_H3_RESOLUTION` rather than scanning every live entity.
- When an entity crosses a live H3 boundary, its old-cell membership is removed and its new-cell membership is added.
- Stale/out-of-order events cannot regress Redis live state or spatial membership.
- The Correlation Worker queries a computed k-ring of live cells and performs exact distance checks after candidate reduction.
- `HISTORY_H3_RESOLUTION` and `LIVE_H3_RESOLUTION` can be tuned independently.

---

## Flow Diagrams

### Geo-Cell Write

![Geo-Cell Write](../../../diagrams/docs/use-cases/US-12-geo-spatial-efficiency/geo-cell-write.svg)

The Position Consumer computes historical and live H3 cells. Historical H3 is persisted as an indexed TimescaleDB column. Live H3 membership is maintained in Redis sorted sets under the same timestamp guard as the live entity hash.

### Regional Query

![Regional Query](../../../diagrams/docs/use-cases/US-12-geo-spatial-efficiency/regional-query.svg)

A historical query maps the requested region to H3 cells, filters `position_history.geo_cell`, and combines that filter with an `observed_at` time range. TimescaleDB eliminates irrelevant time chunks; the composite index narrows rows within the selected chunks.

### Historical vs Live Spatial Indexing

![H3 Selectivity](../../../diagrams/docs/use-cases/US-12-geo-spatial-efficiency/hot-spot-distribution.svg)

H3 does not create separate TimescaleDB chunks. The historical H3 value is an indexed column inside time chunks. Spatial bucketing into separate keys happens in Redis for the live proximity candidate index.

---

## Architectural Justification

Justifies: [ADR-006 - H3 Spatial Indexing](../../adr/ADR-006-geo-cell-sharding-key.md)

Sentinel deliberately uses H3 in two different ways. TimescaleDB remains time-partitioned and uses H3 to improve historical query selectivity. Redis uses separate H3 sorted sets to reduce the live proximity search space from all entities to fresh entities in nearby cells. Exact distance remains the final proximity test.
