# ADR-002: TimescaleDB over Cassandra for Position History

**Status:** Accepted
**Date:** 2026-08-05

---

## Context

Position history is Sentinel's highest-volume durable write. Primary queries are:

- positions for one entity over a time range;
- positions for entities inside a geographic region over a time range.

The store must support sustained ingest, time-window filtering, indexed regional filtering, and replay-safe duplicate handling without introducing unnecessary operational complexity.

---

## Decision

Use TimescaleDB as the position-history store.

`position_history` is a hypertable partitioned by `observed_at TIMESTAMPTZ` only, with a 1-hour chunk interval and a 48-hour retention policy. `geo_cell` is an indexed query column, not a partition/shard dimension.

---

## Reasoning

**Time-window access matches hypertables.** TimescaleDB automatically partitions by time, allowing chunk exclusion for bounded historical queries.

**Regional queries remain indexed.** Sentinel computes `history_geo_cell` in application code and stores it in `geo_cell`. Queries filter by both H3 cell IDs and `observed_at`, using time chunk exclusion plus the `(geo_cell, observed_at DESC)` index inside selected chunks.

**SQL and PostgreSQL tooling are sufficient.** The required access patterns fit SQL well and reuse familiar migration, inspection, backup, and query-planning tools.

**Route deviation does not need a statistical continuous aggregate.** The earlier `route_baseline` idea was rejected because averaging historical latitude/longitude does not describe a route corridor. v1 uses deterministic `route_references` and `route_reference_points` seeded for synthetic entities; see ADR-015.

---

## Alternatives Considered

### Cassandra — rejected

Excellent high-scale writes but more rigid query-shape design and substantially more operational complexity than the portfolio-scale access pattern requires.

### InfluxDB — rejected

Strong time-series focus, but Sentinel benefits from PostgreSQL relational semantics and straightforward coexistence of plain relational tables such as alerts/users/workspaces.

### TimescaleDB spatial partitioning by H3 — rejected for v1

Sentinel deliberately uses time partitioning only. H3 is an indexed filter for historical queries and a separate Redis live-index strategy; see ADR-006.

---

## Consequences

- `observed_at` is the hypertable time/partition column.
- `timestamp_ms` is retained as source event-time metadata.
- duplicate position writes converge through unique `(entity_id, observed_at)` + `ON CONFLICT DO NOTHING`.
- `geo_cell` is an ordinary indexed column inside time chunks.
- `route_references` + `route_reference_points` are plain PostgreSQL tables on the same TimescaleDB instance.
- query performance must be validated with `EXPLAIN ANALYZE` using realistic time ranges and H3 cell sets.
