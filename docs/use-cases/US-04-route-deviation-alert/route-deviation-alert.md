# US-04: Route Deviation Alert

**Actor:** Operator
**Status:** Defined

---

## Story

As an operator, I want an alert when a synthetic entity remains outside its assigned reference-route corridor so that sustained route deviation is visible without reacting to one noisy position.

---

## Acceptance Criteria

- Route deviation applies to synthetic entities with assigned reference routes only in v1.
- `route_references` stores the route header and corridor threshold; `route_reference_points` stores ordered waypoints.
- Deviation Detector computes minimum point-to-segment distance from each eligible position to the route.
- Detector publishes `OUT_OF_RANGE` or `IN_RANGE` on every eligible ping and remains stateless.
- Alert Evaluator owns `deviation-state:{entity_id}` with `count`, `episode_start_ms`, `last_processed_ms`, and `alert_emitted`.
- `timestamp_ms <= last_processed_ms` is ignored as duplicate/out-of-order classification.
- ROUTE_DEVIATION is emitted after `DEVIATION_SUSTAINED_PINGS` consecutive OUT_OF_RANGE events and only once per episode.
- IN_RANGE deletes the episode state and allows a later deviation to become a new episode.

---

## Flow Diagrams

### Reference Route Setup

![Reference Route Setup](../../../diagrams/docs/use-cases/US-04-route-deviation-alert/baseline-computation.svg)

The historical filename is retained for generated-diagram link stability, but this is **reference-route setup**, not statistical baseline computation. The synthetic generator seeds deterministic route headers and waypoints.

### Deviation Detection

![Deviation Detection](../../../diagrams/docs/use-cases/US-04-route-deviation-alert/deviation-detection.svg)

The detector performs geometry only. The evaluator interprets sustained classifications and emits the alert.

### Transient vs Sustained

![Transient vs Sustained](../../../diagrams/docs/use-cases/US-04-route-deviation-alert/transient-vs-sustained.svg)

One OUT_OF_RANGE classification is insufficient. Only a sustained run crosses the threshold; an IN_RANGE classification resets the episode.

---

## Architectural Justification

Justifies: [ADR-014 - Hybrid Input Model](../../adr/ADR-014-alert-evaluator-hybrid-input-model.md), [ADR-015 - v1 Reference Route Model](../../adr/ADR-015-v1-reference-route-model.md)

A deterministic reference route is deliberately used instead of a historical lat/lon average. Averaging mixed flight/vessel path phases does not produce a meaningful corridor and would create false deviations rather than an interview-defensible v1 rule.
