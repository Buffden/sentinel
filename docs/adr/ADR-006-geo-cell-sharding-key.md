# ADR-006: Geo-Cell Sharding Key Design and Hot-Spot Mitigation

**Status:** Accepted
**Date:** 2026-08-05

---

## Context

TimescaleDB partitions position history by time (hypertables). Within each time partition, we need a spatial partitioning strategy so that queries for "all entities in region R during time window T" hit a minimal set of chunks.

The naive approach  - partitioning only by time  - means a geo-region query must scan all entities in the time window regardless of location. At high entity counts, this is expensive.

We also need to avoid hot-spots: a single cell covering a busy airspace (e.g. over London or JFK) would receive a disproportionate share of writes, creating a bottleneck.

---

## Decision

Use `geo_cell` (H3 resolution 5) as an indexed query column on `position_history`. TimescaleDB partitions the hypertable by `observed_at` (time only). `geo_cell` narrows queries within time chunks — it is not a partition dimension.

---

## Reasoning

**H3 hexagonal grid.** H3 (Uber's geospatial indexing system) divides the world into hexagonal cells at configurable resolutions. Hexagons have the property of equal area and equidistant neighbours, which avoids the distortion of rectangular grid cells near the poles and produces more uniform write distribution for geographically spread entities.

**Resolution choice.** H3 resolution 5 produces cells of approximately 250 km²  - large enough that most regional queries fit within a small number of adjacent cells, small enough that busy airspaces are spread across multiple cells rather than concentrated in one.

**Hot-spot mitigation.** High-traffic airspaces (dense corridors, major airports) naturally span multiple H3 cells at resolution 5. Write load is distributed across cells proportionally to actual entity density  - no single cell dominates unless the entity distribution is genuinely point-concentrated (unlikely at resolution 5).

**Query efficiency.** A bounding-box region query is translated into a set of H3 cell IDs that overlap the bounding box. The TimescaleDB query filters on `geo_cell IN (...)` and `observed_at BETWEEN T1 AND T2`. TimescaleDB chunk exclusion reduces the time dimension; the `(geo_cell, observed_at)` index reduces the spatial scan within each chunk.

---

## Alternatives Considered

### Geohash (rejected)
- Rectangular cells  - area varies significantly at different latitudes (cells near poles are much smaller in real area than cells at the equator)
- Neighbour lookup is less uniform than H3 (geohash neighbours are not always adjacent in the index)
- H3 is the more modern and better-supported choice for geospatial indexing

### Partition by entity_id only (rejected)
- Makes "all entities in region R" queries require a full scan across all entity partitions
- Does not co-locate spatially nearby entities in the same chunk
- Efficient only for single-entity time-range queries, not for regional queries

### No spatial partitioning (time-only hypertable) (rejected)
- Simple, but regional queries scan all entities in the time window
- Acceptable at small entity counts; does not scale

---

## Consequences

- The H3 library must be available in the Position Consumer to compute `geo_cell` at ingest time, and in the Correlation Worker for live proximity scoping.
- `HISTORY_H3_RESOLUTION` (used for the TimescaleDB `geo_cell` column) and `LIVE_H3_RESOLUTION` (used for the Redis `geo-cell:*` sorted set) are separate configuration values. They need not be identical — historical queries and live proximity scoping have different access patterns and density requirements. POC-03 validates the right values.
- `HISTORY_H3_RESOLUTION` defaults to 5 for v1. Changing it requires rewriting the `geo_cell` column (expensive migration).
- `LIVE_H3_RESOLUTION` may be tuned independently based on entity density, `PROXIMITY_THRESHOLD_METRES`, and the number of cells scanned per event. The Correlation Worker computes the k-ring radius from `PROXIMITY_THRESHOLD_METRES` and the cell edge length at `LIVE_H3_RESOLUTION` — not hardcoded to k-ring(1).
- Queries must translate a bounding box to H3 cell IDs before hitting the database — done in the API or consumer, not in SQL.
- `geo_cell` is not a TimescaleDB partition dimension — it is an index column. TimescaleDB partitions by time (`observed_at`) only. The `(geo_cell, observed_at DESC)` index provides geo-scoped efficiency within time chunks.
- If `LIVE_H3_RESOLUTION != HISTORY_H3_RESOLUTION`, the Position Consumer computes two separate cell IDs per ping: one for the `geo_cell` column (history) and one for the Redis sorted set key (live).
