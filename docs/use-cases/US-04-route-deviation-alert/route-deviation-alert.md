# US-04: Route Deviation Alert

**Actor:** Operator
**Status:** Defined

---

## Story

As an operator, I want to receive an alert when an entity's current track diverges significantly from its established historical baseline so that I can identify route deviations.

---

## Acceptance Criteria

- The system maintains a historical route baseline per entity derived from past position history
- A deviation alert is emitted when the current position diverges from the baseline by more than `ROUTE_DEVIATION_THRESHOLD_METRES`
- The baseline is computed from a configurable historical window (e.g. the last 30 days of tracks for the same entity)
- A single transient position ping that falls outside the baseline does not trigger an alert - sustained deviation does

---

## Flow Diagrams

### Baseline Computation

![Baseline Computation](../../../diagrams/docs/use-cases/US-04-route-deviation-alert/baseline-computation.svg)

Position history written to TimescaleDB is rolled up into a continuous aggregate that materialises the expected route per entity per time bucket.

### Deviation Detection

![Deviation Detection](../../../diagrams/docs/use-cases/US-04-route-deviation-alert/deviation-detection.svg)

The alert evaluator compares the current position from Redis against the materialised baseline and emits an alert when sustained deviation exceeds the threshold.

### Transient vs Sustained

![Transient vs Sustained](../../../diagrams/docs/use-cases/US-04-route-deviation-alert/transient-vs-sustained.svg)

A single position ping outside the baseline increments a counter but does not trigger an alert; only N consecutive out-of-baseline pings cross the sustained threshold and emit.

---

## Architectural Justification

Justifies: [ADR-002 - TimescaleDB for Position History](../../adr/ADR-002-timescaledb-over-cassandra.md)

Route baseline computation requires materialised historical track averages per entity across a configurable time window. TimescaleDB continuous aggregates produce time-bucket rollups that serve this query without scanning raw position rows on every evaluation cycle. The geo-cell + time-bucket sharding key (ADR-006) also ensures that historical queries for a specific entity in a specific region hit a minimal set of chunks.
