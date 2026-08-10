# POC-03: H3 + TimescaleDB

**Branch:** `poc/h3-timescaledb`
**Status:** Not started

---

## Risk

ADR-006 claims H3 + TimescaleDB produces efficient regional queries without hot-spots. Specifically:

1. `geo_cell` as an index column (not a partition dimension) at `HISTORY_H3_RESOLUTION` provides efficient geo-scoped queries within time chunks.
2. A separate `LIVE_H3_RESOLUTION` may be needed for Redis proximity scoping — the right resolution depends on entity density and `PROXIMITY_THRESHOLD_METRES`.
3. The k-ring radius for proximity scoping should be computed from `PROXIMITY_THRESHOLD_METRES`, not hardcoded to k-ring(1).

These must be confirmed before the position consumer and correlation worker are built around them.

---

## Goal

1. Prove the TimescaleDB schema (`observed_at` partition + `geo_cell` index) produces efficient regional queries.
2. Validate `HISTORY_H3_RESOLUTION` (default: 5) for position history queries.
3. Validate `LIVE_H3_RESOLUTION` for Redis live proximity scoping and determine the correct k-ring radius.
4. Confirm idempotency constraint.

---

## Validate

### TimescaleDB (historical queries)

- H3 library installation and cell ID computation for sample lat/lon coordinates at resolution 5
- TimescaleDB hypertable creation with `observed_at TIMESTAMPTZ` as the partition column; `geo_cell` as an index column only (not a partition dimension)
- Insert synthetic position rows across multiple cells and time ranges
- Run a bounding-box query (translated to H3 cell IDs) and confirm via `EXPLAIN ANALYZE` that:
  - TimescaleDB chunk exclusion fires on the `observed_at` dimension
  - The `(geo_cell, observed_at DESC)` index is used within the relevant chunks
- Confirm no single H3 cell at resolution 5 dominates for a representative set of ADS-B coordinates — hot-spot check (US-12)
- Validate `ON CONFLICT (entity_id, timestamp_ms) DO NOTHING` correctly ignores duplicate inserts (US-11)

### Redis live proximity (live H3 resolution)

- Test k-ring scoping at resolution 5: for a given `PROXIMITY_THRESHOLD_METRES`, compute the required k-ring radius and verify it covers the expected area
- Confirm that the formula `k = ceil(PROXIMITY_THRESHOLD_METRES / cell_edge_length)` gives no false negatives for the test threshold
- Test whether resolution 5 or a higher resolution is more efficient for the expected entity density (fewer false candidate fetches from `entity:live:*`)
- Document the chosen `LIVE_H3_RESOLUTION` with reasoning and whether it differs from `HISTORY_H3_RESOLUTION`
- Validate ZADD/ZRANGEBYSCORE sorted set pattern for live membership management

---

## Done When

- Query plan for regional bounding-box + time-window query shows chunk exclusion on `observed_at` and index use on `geo_cell`
- `HISTORY_H3_RESOLUTION` confirmed as 5 (or revised with documented reason)
- `LIVE_H3_RESOLUTION` chosen and documented; k-ring radius formula validated
- Hot-spot check: no single cell dominates for realistic ADS-B coordinates
- Inserting the same position row 3 times results in exactly 1 row
- `EXPIRE proximity-episode:{pair_key}` TTL behaviour confirmed in Redis

---

## ADR Coverage

- [ADR-002 - TimescaleDB over Cassandra](../../adr/ADR-002-timescaledb-over-cassandra.md)
- [ADR-006 - Geo-Cell Sharding Key Design](../../adr/ADR-006-geo-cell-sharding-key.md)
- [ADR-015 - v1 Reference Route Model](../../adr/ADR-015-v1-reference-route-model.md) (no continuous aggregate needed)

## Use Case Coverage

- [US-11](../../use-cases/US-11-idempotent-writes/idempotent-writes.md) - idempotent writes (TimescaleDB)
- [US-12](../../use-cases/US-12-geo-spatial-efficiency/geo-spatial-efficiency.md) - geo-spatial efficiency
