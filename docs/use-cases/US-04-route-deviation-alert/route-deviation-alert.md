# US-04: Route Deviation Alert

**Actor:** Operator
**Status:** Defined

---

## Story

As an operator, I want to receive an alert when an entity's current track diverges significantly from its assigned reference route so that I can identify route deviations.

---

## Acceptance Criteria

- Route deviation detection applies to **synthetic entities only** in v1. Real ADS-B/AIS entities have no reference route and are skipped. Statistical baseline modeling from historical lat/lon averages does not produce a meaningful route corridor and is deferred to future work.
- Each synthetic entity has a reference route in `route_references` (with `corridor_threshold_metres`) and ordered waypoints in `route_reference_points`
- A deviation alert is emitted when the current position is further than `corridor_threshold_metres` from the nearest **route segment** (minimum perpendicular distance), sustained across `DEVIATION_SUSTAINED_PINGS` consecutive pings
- The Deviation Detector is stateless — it classifies every ping as `OUT_OF_RANGE` or `IN_RANGE`. The Alert Evaluator owns episode state (`deviation-state:{entity_id}` hash: `count`, `episode_start_ms`, `last_processed_ms`, `alert_emitted`)
- A single transient out-of-range ping does not trigger an alert — sustained deviation does; `alert_emitted` flag prevents re-emission within the same episode; `last_processed_ms` prevents replay regressions

---

## Flow Diagrams

### Reference Route Setup

![Reference Route Setup](../../../diagrams/docs/use-cases/US-04-route-deviation-alert/baseline-computation.svg)

At startup, the synthetic load generator seeds reference routes into TimescaleDB. Each synthetic entity has one assigned route: a header record with a corridor threshold and an ordered list of waypoints defining the expected path. Real ADS-B/AIS entities have no assigned route and are skipped by the Deviation Detector.

### Deviation Detection

![Deviation Detection](../../../diagrams/docs/use-cases/US-04-route-deviation-alert/deviation-detection.svg)

The Deviation Detector statlessly classifies each position against the reference route and publishes `OUT_OF_RANGE` or `IN_RANGE` events to `deviation.candidates` (one per eligible ping). The Alert Evaluator consumes these events; it guards against replay via `last_processed_ms`, increments a counter on `OUT_OF_RANGE`, and emits exactly one ROUTE_DEVIATION alert per episode after `DEVIATION_SUSTAINED_PINGS` consecutive out-of-range pings. On `IN_RANGE`, the episode state is deleted and the counter resets. The API writes the alert to the alerts table (status: NEW, idempotent on replay) before pushing to scope-matched WebSocket connections.

### Transient vs Sustained

![Transient vs Sustained](../../../diagrams/docs/use-cases/US-04-route-deviation-alert/transient-vs-sustained.svg)

A single position ping outside the baseline increments a counter but does not trigger an alert; only N consecutive out-of-baseline pings cross the sustained threshold and emit.

---

## Architectural Justification

Justifies: [ADR-015 - v1 Reference Route Model](../../adr/ADR-015-v1-reference-route-model.md)

Route deviation in v1 uses deterministic reference routes (`route_references` + `route_reference_points`) rather than a statistical lat/lon average. Statistical averaging across mixed flight phases (departure, cruise, approach) produces a midpoint coordinate that is not on any actual route segment, causing false positives. Reference routes are explicitly assigned to synthetic entities and give injectable, predictable anomalies for demos. Real ADS-B/AIS entities have no assigned route and are skipped.
