# ADR-002: TimescaleDB over Cassandra for Position History

**Status:** Accepted
**Date:** 2026-08-05

---

## Context

Position history is the highest-volume write in the system. Every entity broadcasts a position ping every 0.5–10 seconds. Queries against this store are always of the form:

- "Give me all positions for entity X in the last N hours"
- "Give me all entities that were in geo-region R between time T1 and T2"

The store needs to handle high ingest throughput, efficient time-range queries, and geo-spatial filtering.

---

## Decision

Use TimescaleDB (PostgreSQL extension) as the position history store. The hypertable is partitioned by `observed_at TIMESTAMPTZ` (daily chunks). `geo_cell` is an indexed query column, not a partition dimension.

---

## Reasoning

**Query shape matches hypertable sharding.** TimescaleDB's hypertables partition data automatically by time. Within each time chunk, a `geo_cell` index narrows spatial queries — queries for "entities in region R during window T1–T2" use chunk exclusion on time and index scanning on geo_cell. `geo_cell` is an indexed column, not a partition dimension.

**SQL with geospatial extensions.** PostGIS integrates directly with TimescaleDB, giving full geospatial query capability (bounding-box filters, distance calculations) without a separate geo-indexing service.

**`observed_at` as the partition column.** Sentinel partitions on `observed_at TIMESTAMPTZ` for clearer time semantics and convenient time-oriented querying. `observed_at` is computed at ingest as `to_timestamp(timestamp_ms / 1000.0)`. `timestamp_ms` is kept as source metadata and the idempotency key component. A pre-bucketed `time_bucket` column stored alongside the data would be redundant — TimescaleDB's `time_bucket()` function operates on `observed_at` at query time.

**Route deviation uses reference routes, not continuous aggregates.** The `route_baseline` continuous aggregate was originally planned here but dropped. Averaging lat/lon per entity per 1-hour bucket does not produce a meaningful route corridor — the average of A→B→C positions lands somewhere in the middle of the route. Statistical route modeling is deferred to future work. Route deviation in v1 uses a static `reference_routes` table seeded from the synthetic generator's known route definition, giving deterministic and injectable anomalies.

**Operational familiarity.** PostgreSQL tooling (psql, pg_dump, standard JDBC/pgx drivers) is widely understood. Cassandra requires a separate mental model and operational runbook.

---

## Alternatives Considered

### Apache Cassandra (rejected)
- Excellent write throughput at scale, but query flexibility is severely constrained by the partition key chosen at schema design time
- Cassandra does not support arbitrary range scans efficiently  - every query pattern must be anticipated at schema time
- No native geospatial support; geo-filtering would require application-level post-processing or a secondary index hack
- Operational complexity (tuning compaction, repair, consistency levels) is high relative to the benefit at this scale
- The right choice if write volume is in the tens of millions of events per second and geo-queries are not required  - not the case here

### InfluxDB (rejected)
- Purpose-built time-series, good write throughput
- Limited relational query capability  - joining position history with entity metadata is awkward
- No geospatial support without external tooling
- Less control over sharding strategy compared to TimescaleDB hypertables

---

## Consequences

- TimescaleDB runs as a PostgreSQL extension  - Docker image is `timescale/timescaledb-ha`
- Geo-cell is an indexed query column on `position_history` — not a partition dimension. The hypertable partitions on `observed_at` only. See ADR-006 for geo-cell design rationale.
- `route_references` and `route_reference_points` tables replace the `route_baseline` continuous aggregate. Their schema is documented in DATA_MODEL.md and ADR-015. Seeded from the synthetic generator at startup.
- `observed_at` and `timestamp_ms` are both stored. `observed_at` is the partition/query column; `timestamp_ms` is the idempotency key component and source fidelity record.
