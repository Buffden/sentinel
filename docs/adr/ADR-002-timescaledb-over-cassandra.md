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

Use TimescaleDB (PostgreSQL extension) as the position history store, sharded on a composite key of geo-cell and time-bucket.

---

## Reasoning

**Query shape matches hypertable sharding.** TimescaleDB's hypertables partition data automatically by time. Combined with a geo-cell prefix on the partition key, queries for "entities in region R during window T1–T2" hit a small, predictable set of chunks rather than scanning the full table.

**SQL with geospatial extensions.** PostGIS integrates directly with TimescaleDB, giving full geospatial query capability (bounding-box filters, distance calculations) without a separate geo-indexing service.

**Continuous aggregates.** TimescaleDB supports materialised time-bucket rollups, which will be useful for route baseline computation (anomaly detection needs a historical average track per entity).

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
- Geo-cell sharding key design is a separate decision  - see ADR-006
- Continuous aggregates add schema complexity but are not required for v1 anomaly detection
