# US-12: Efficient Regional Position Queries

**Actor:** System
**Status:** Defined

---

## Story

As the system, I want position history queries for "all entities in region R during time window T" to be efficient at high entity counts so that the alert evaluator and API do not time out under load.

---

## Acceptance Criteria

- A bounding-box + time-window query returns results within an acceptable latency threshold at the expected entity count
- The query plan shows chunk exclusion working - only chunks covering the relevant geo-cells and time buckets are scanned
- A single high-traffic area (e.g. a busy airport or shipping lane) does not create a write hot-spot that degrades ingest performance
- Changing the bounding box does not require a schema change or index rebuild

---

## Flow Diagrams

### Geo-Cell Write

![Geo-Cell Write](../../../diagrams/docs/use-cases/US-12-geo-spatial-efficiency/geo-cell-write.svg)

The position consumer computes the H3 cell ID from the event coordinates at ingest time and stores it as part of the composite partition key alongside the time bucket.

### Regional Query

![Regional Query](../../../diagrams/docs/use-cases/US-12-geo-spatial-efficiency/regional-query.svg)

The API or alert evaluator translates a bounding box into a set of H3 cell IDs in application code, then queries TimescaleDB with those cell IDs so chunk exclusion restricts the scan to only the relevant spatial and time partitions.

### Hot-Spot Distribution

![Hot-Spot Distribution](../../../diagrams/docs/use-cases/US-12-geo-spatial-efficiency/hot-spot-distribution.svg)

Shows how a high-traffic area such as a busy airport naturally maps to multiple H3 cells at resolution 5, distributing write load across multiple chunks rather than concentrating it in one.

---

## Architectural Justification

Justifies: [ADR-006 - Geo-Cell Sharding Key Design](../../adr/ADR-006-geo-cell-sharding-key.md)

A time-only TimescaleDB hypertable requires scanning all entities in the time window regardless of location - at high entity counts this is a full partition scan. The geo-cell prefix on the composite partition key `(geo_cell, time_bucket)` restricts scans to only the chunks covering the queried spatial region. H3 resolution 5 (~250 km² cells) ensures that high-traffic areas are naturally spread across multiple cells, preventing any single cell from becoming a write hot-spot.
