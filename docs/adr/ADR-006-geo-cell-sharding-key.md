# ADR-006: H3 Geo-Cell Indexing Strategy

**Status:** Accepted
**Date:** 2026-08-05

---

## Context

Sentinel needs two related but different geospatial access patterns:

1. historical queries such as "positions in region R during time window T";
2. live proximity candidate reduction before exact distance calculation.

TimescaleDB already partitions `position_history` by time. Redis holds live entity state. The design needs spatial indexing without pretending that H3 creates a second TimescaleDB partition dimension.

---

## Decision

Use H3 in two independent access patterns:

- `HISTORY_H3_RESOLUTION` → compute `history_geo_cell`, stored in TimescaleDB column `position_history.geo_cell` and indexed with `(geo_cell, observed_at DESC)`;
- `LIVE_H3_RESOLUTION` → compute `live_geo_cell`, used as the suffix for Redis sorted sets `geo-cell:{h3_cell_id}`.

TimescaleDB partitions the hypertable by `observed_at` **only**. `geo_cell` is an indexed query column, not a partition or shard dimension.

---

## Historical Query Path

A regional query translates the requested geographic bounds into a set of H3 cells at `HISTORY_H3_RESOLUTION`, then queries:

```sql
WHERE geo_cell IN (...)
  AND observed_at BETWEEN $from AND $to
```

TimescaleDB chunk exclusion narrows the **time** dimension. The `(geo_cell, observed_at DESC)` index narrows rows **inside the selected time chunks**.

H3 does not create one TimescaleDB chunk per geo cell and should never be described as distributing writes across geo-cell chunks.

---

## Live Proximity Path

The Position Consumer maintains:

```text
geo-cell:{live_geo_cell}
```

as a Redis sorted set where member=`entity_id` and score=`last_seen_ms`.

When an entity moves between cells:

1. read its previous `live_geo_cell`;
2. `ZREM` it from the old cell when the cell changed;
3. `ZADD` it to the current cell with score=`last_seen_ms`.

The Correlation Worker reads the incoming cell plus a computed k-ring using `ZRANGEBYSCORE` with a freshness lower bound, then performs exact distance calculations on the resulting candidates.

---

## Resolution Choice

The two resolutions are intentionally separate because their optimization targets differ:

- history resolution trades index selectivity against the number of cells in regional queries;
- live resolution trades candidate density against the number of neighboring cells scanned for proximity.

`HISTORY_H3_RESOLUTION` may start at 5 for v1, but both values should be validated experimentally against realistic data density and query patterns.

---

## Alternatives Considered

### H3 as a TimescaleDB partition dimension — rejected

This would misrepresent the chosen schema and complicate hypertable design. Sentinel uses time partitioning only.

### No historical spatial index — rejected

Time-window queries would scan all rows in relevant chunks regardless of region.

### Global Redis live set — rejected

Every proximity calculation would compare against all tracked entities. H3-scoped sorted sets reduce the candidate set before exact distance computation.

### One H3 resolution for both paths — not required

It may be adequate initially, but the access patterns differ enough that separate configuration is cleaner and more tunable.

---

## Consequences

- Position Consumer computes history and live H3 cells per accepted ping.
- `geo_cell` remains an ordinary indexed TimescaleDB column.
- Redis `geo-cell:*` is the live spatial candidate index.
- Correlation Worker computes k-ring radius from `PROXIMITY_THRESHOLD_METRES` and `LIVE_H3_RESOLUTION`, not from a hardcoded ring size.
- Changing `HISTORY_H3_RESOLUTION` for already persisted rows requires historical rewrite/backfill.
- Documentation and diagrams must not depict separate TimescaleDB chunks by `geo_cell`.
