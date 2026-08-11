# ADR-014: Hybrid Input Model for the Alert Evaluator

**Status:** Accepted
**Date:** 2026-08-09

---

## Context

The original Alert Evaluator design directly queried Redis, TimescaleDB, and Neo4j for multiple anomaly types. That concentrated detection logic and persistence coupling in one service.

Some anomaly inputs naturally fit event streams; signal loss does not because it is the absence of an event. The design therefore separates **fact detection** from **alert interpretation** while keeping signal-loss scanning where it belongs.

---

## Decision

Use a hybrid input model:

- Signal loss: scheduled Redis scan of `entity:live:*` by the Alert Evaluator.
- Route deviation: stateless Deviation Detector publishes `deviation.candidates`.
- Unscheduled proximity: Correlation Worker publishes `proximity.candidates` after exact distance confirmation and `KNOWN_ASSOCIATE` filtering.
- Composite interpretation: Alert Evaluator combines `proximity.candidates` with Redis `alert-state` / `recent-loss`.

The Alert Evaluator does **not** query Neo4j in the current v1 contract.

---

## Deviation Detector

A Node.js service consumes `position.normalized`, reads deterministic reference routes, and publishes one classification per eligible synthetic ping.

| Direction | Contract |
| --- | --- |
| Consumes | `position.normalized` — group `deviation-detector` |
| Reads TimescaleDB | `route_references`, `route_reference_points` |
| Publishes | `deviation.candidates` |

It is stateless and emits `OUT_OF_RANGE` or `IN_RANGE` on every eligible ping. Sustained-ping counting, replay guards, and alert episode state belong to the Alert Evaluator in `deviation-state:{entity_id}`.

`route_baseline` does not exist in the accepted v1 design; see ADR-015.

---

## Correlation Worker

The Correlation Worker consumes `position.normalized`, uses Redis H3 live indexes to reduce candidate pairs, computes exact distance, and owns graph-specific relationship filtering.

For a new proximity episode:

1. canonicalize the pair;
2. check Neo4j `KNOWN_ASSOCIATE` for that specific pair;
3. persist/update proximity evidence;
4. publish `proximity.candidates` **only if no known-associate relationship exists**.

The candidate event therefore already means:

> a new exact proximity episode exists and the pair is unscheduled in v1.

The Alert Evaluator must not perform the same known-associate query again.

---

## Alert Evaluator

| Detection | Input | Evaluator responsibility |
| --- | --- | --- |
| Signal loss | Scheduled Redis scan | detect absence, suppress repeated emission with `alert-state` |
| Route deviation | `deviation.candidates` | own sustained episode state and emit ROUTE_DEVIATION |
| Proximity | `proximity.candidates` | emit UNSCHEDULED_PROXIMITY unless qualifying loss state exists |
| Composite | candidate + `alert-state` / `recent-loss` | emit COMPOSITE and reference individual alerts to supersede |

The evaluator's only TimescaleDB read is `position_history` when a last-known signal-loss position is needed for the alert payload.

---

## Why No Direct Neo4j Read in Alert Evaluator

The old design retained a targeted Neo4j lookup during composite assembly. That became redundant once the Correlation Worker contract was strengthened:

- `KNOWN_ASSOCIATE` is already checked before candidate publication;
- `proximity.candidates` already carries canonical pair identity, `episode_start_ms`, midpoint location, and distance at detection;
- composite timing/state is owned in Redis, not Neo4j.

Keeping the second lookup would duplicate authorization of the same domain fact and add an unnecessary database dependency/failure mode to the Alert Evaluator.

Neo4j remains essential for:

- Correlation Worker relationship/evidence writes and known-associate filtering;
- API investigation and historical relationship evidence.

---

## Why Signal Loss Remains a Redis Scan

An entity that stops transmitting emits no event announcing its silence. A scheduled last-seen scan is therefore the simplest correct model. Artificial heartbeats or per-entity timer events would add components without improving correctness for v1.

---

## Candidate Topic Retention

`deviation.candidates` and `proximity.candidates` are derived transient rule inputs and use short retention (target 1 hour). Durable facts remain in TimescaleDB/Neo4j. Historical backfill must not route old candidate streams into the live alert path.

---

## Consequences

- Alert Evaluator remains the full Alert Layer and owns final rule interpretation.
- Alert Evaluator no longer depends on Neo4j.
- Correlation Worker owns `KNOWN_ASSOCIATE` filtering before Kafka publication.
- Deviation Detector owns stateless geometric classification only.
- Redis owns active/recent loss and sustained deviation state.
- The `alerts` Kafka topic remains the sole alert output consumed by the API.
- Failure tests should prove that a Neo4j outage blocks new proximity fact creation in the Correlation Worker but does not independently disable signal-loss or route-deviation evaluation in the Alert Evaluator.
