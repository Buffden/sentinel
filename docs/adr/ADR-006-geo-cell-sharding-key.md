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

Use a geo-cell prefix as part of the TimescaleDB chunk key, derived from an H3 hexagonal grid at a fixed resolution. The composite partition key is `(geo_cell, time_bucket)`.

---

## Reasoning

**H3 hexagonal grid.** H3 (Uber's geospatial indexing system) divides the world into hexagonal cells at configurable resolutions. Hexagons have the property of equal area and equidistant neighbours, which avoids the distortion of rectangular grid cells near the poles and produces more uniform write distribution for geographically spread entities.

**Resolution choice.** H3 resolution 5 produces cells of approximately 250 km²  - large enough that most regional queries fit within a small number of adjacent cells, small enough that busy airspaces are spread across multiple cells rather than concentrated in one.

**Hot-spot mitigation.** High-traffic airspaces (dense corridors, major airports) naturally span multiple H3 cells at resolution 5. Write load is distributed across cells proportionally to actual entity density  - no single cell dominates unless the entity distribution is genuinely point-concentrated (unlikely at resolution 5).

**Query efficiency.** A bounding-box region query is translated into a set of H3 cell IDs that overlap the bounding box. The TimescaleDB query filters on `geo_cell IN (...)` and `time_bucket BETWEEN T1 AND T2`, hitting only the relevant chunk combinations.

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

- The H3 library must be available in the ingestion/consumer service to compute the geo_cell value at ingest time
- Resolution 5 is fixed for v1  - changing resolution requires re-partitioning the table (expensive migration)
- Queries must translate a bounding box to a list of H3 cell IDs before hitting the database  - this translation is done in the API or consumer, not in SQL
