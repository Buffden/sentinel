# POC-03: H3 + TimescaleDB

**Branch:** `poc/h3-timescaledb`
**Status:** Not started

---

## Risk

ADR-006 makes a specific claim - H3 resolution 5 combined with a composite partition key `(geo_cell, time_bucket)` produces efficient regional queries without hot-spots. Additionally, US-04 requires TimescaleDB continuous aggregates for route baseline computation. Both claims must be confirmed before the position consumer is built around them.

---

## Goal

Prove the geo-cell sharding key design and continuous aggregate baseline work as specified.

---

## Validate

- H3 library installation and cell ID computation for sample lat/lon coordinates
- TimescaleDB hypertable creation with composite partition key `(geo_cell, time_bucket)`
- Insert synthetic position rows across multiple cells and time buckets
- Run a bounding-box query (translated to H3 cell IDs) and confirm via `EXPLAIN` that it hits only the expected chunks (US-12)
- Confirm no single H3 cell at resolution 5 dominates for a representative set of ADS-B coordinates - hot-spot check (US-12)
- Create a continuous aggregate on position history and confirm it materialises a per-entity time-bucket rollup suitable for route baseline comparison (US-04)
- Validate `ON CONFLICT (entity_id, timestamp_ms) DO NOTHING` correctly ignores duplicate inserts (US-11)

---

## Done When

- Query plan shows chunk exclusion working correctly for geo + time filters
- Resolution 5 is confirmed as the right trade-off (or revised with a documented reason)
- Continuous aggregate refreshes correctly and the baseline query returns expected results on synthetic data
- Inserting the same position row 3 times results in exactly 1 row in the table

---

## ADR Coverage

- [ADR-002 - TimescaleDB over Cassandra](../../adr/ADR-002-timescaledb-over-cassandra.md)
- [ADR-006 - Geo-Cell Sharding Key Design](../../adr/ADR-006-geo-cell-sharding-key.md)

## Use Case Coverage

- [US-04](../../use-cases/US-04-route-deviation-alert/route-deviation-alert.md) - route deviation baseline computation
- [US-11](../../use-cases/US-11-idempotent-writes/idempotent-writes.md) - idempotent writes (TimescaleDB)
- [US-12](../../use-cases/US-12-geo-spatial-efficiency/geo-spatial-efficiency.md) - geo-spatial efficiency
