# ADR-006: H3 Spatial Indexing for Historical Queries and Live Proximity

**Status:** Accepted
**Date:** 2026-08-05

---

## Context

Sentinel has two different geospatial access patterns:

1. historical queries such as "all positions in region R during time window T";
2. live proximity detection, where each incoming entity should be compared only with nearby fresh entities rather than the entire live population.

These access patterns use H3 for different purposes and must not be confused with TimescaleDB partitioning.

TimescaleDB partitions `position_history` by `observed_at` only. H3 does **not** create separate TimescaleDB chunks or distribute writes across H3 cells. Historical H3 values are ordinary indexed columns inside time chunks.

---

## Decision

Use two independently configurable H3 resolutions:

- `HISTORY_H3_RESOLUTION` — used to compute `position_history.geo_cell`, an indexed query column in TimescaleDB;
- `LIVE_H3_RESOLUTION` — used to maintain Redis `geo-cell:{h3_cell_id}` sorted sets for live proximity candidate lookup.

For v1, `HISTORY_H3_RESOLUTION` starts at 5 and is validated experimentally. `LIVE_H3_RESOLUTION` is tuned independently based on entity density and `PROXIMITY_THRESHOLD_METRES`.

---

## Historical Query Model

`position_history` is a TimescaleDB hypertable partitioned only by `observed_at`.

A regional query translates the requested region into H3 cells in application code and executes a query shaped like:

```sql
WHERE geo_cell IN (...)
  AND observed_at BETWEEN $t1 AND $t2
```

TimescaleDB chunk exclusion reduces the **time** dimension. The `(geo_cell, observed_at DESC)` index narrows the **spatial** scan inside the selected time chunks.

H3 therefore improves query selectivity; it is not a second partition dimension and is not a write-sharding mechanism for TimescaleDB.

---

## Live Proximity Model

The Position Consumer computes `live_geo_cell` and maintains Redis sorted sets:

```text
geo-cell:{live_geo_cell}
member = entity_id
score  = last_seen_ms
```

On an accepted newer/current ping:

1. read the entity's previous `live_geo_cell` before overwriting live state;
2. if the cell changed, `ZREM` the entity from the previous `geo-cell:*` sorted set;
3. `ZADD` the entity to the current cell with score = `last_seen_ms`;
4. update `entity:live:{entity_id}` consistently with the same timestamp guard.

The Correlation Worker queries the incoming entity's cell plus a computed H3 k-ring using `ZRANGEBYSCORE`, then performs exact distance calculations only for fresh candidate entities.

The k-ring radius is derived from `PROXIMITY_THRESHOLD_METRES` and H3 cell geometry at `LIVE_H3_RESOLUTION`; it is not hardcoded.

---

## Reasoning

**Separate access patterns deserve separate resolutions.** Historical regional queries and live proximity lookup optimize for different things. A resolution that is useful for a TimescaleDB index may be too coarse or too fine for Redis candidate density.

**H3 is a candidate-reduction mechanism, not the final distance test.** Cell membership narrows the search space. Exact geodesic distance still determines whether a pair is within `PROXIMITY_THRESHOLD_METRES`.

**Sorted sets age stale members logically.** `last_seen_ms` as the score lets the Correlation Worker exclude stale entities with a lower-bound score filter even if an entity disappears without a final cleanup event.

**Time partitioning remains simple.** Keeping TimescaleDB partitioned only by `observed_at` preserves straightforward hypertable behavior and chunk exclusion while the composite `(geo_cell, observed_at DESC)` index handles the regional query pattern.

---

## Alternatives Considered

### H3 as a TimescaleDB partition dimension (rejected)
- Would complicate hypertable partitioning and operational reasoning.
- Sentinel does not need spatially separate chunks for its expected v1 scale.
- The desired historical access pattern is adequately served by time chunk exclusion plus a composite index.

### Geohash (rejected)
- Neighbor handling is less uniform for the live candidate-search use case.
- H3 provides convenient neighborhood expansion and a consistent cell model for both historical indexing and live candidate reduction.

### Global live-entity scan (rejected)
- Makes each proximity check O(total live entities).
- Redis H3 sorted sets reduce comparisons to entities in the relevant nearby cells.

---

## Consequences

- The Position Consumer computes both `history_geo_cell` and `live_geo_cell` when the configured resolutions differ.
- `position_history.geo_cell` is indexed but does not affect TimescaleDB chunk placement.
- Redis `geo-cell:*` membership must move correctly when an entity crosses an H3 boundary (`ZREM` old, `ZADD` new).
- Stale/out-of-order events must not regress either `entity:live:*` or `geo-cell:*` membership.
- The API translates historical geographic query regions into `HISTORY_H3_RESOLUTION` cells before querying TimescaleDB.
- The Correlation Worker uses `LIVE_H3_RESOLUTION` and a computed k-ring for live candidate lookup, then performs exact distance checks.
- Both resolutions are tuning parameters that must be validated with realistic entity density and query/load tests before being treated as fixed operational values.
