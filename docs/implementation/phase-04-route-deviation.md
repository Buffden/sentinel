# Phase 04 — Route Deviation

## Goal

Add the second anomaly type into the already-proven alert delivery path.

```text
position.normalized → Deviation Detector → deviation.candidates → Alert Evaluator → alerts → existing API persistence/delivery
```

## What to Build

- seed `route_references` and `route_reference_points` for synthetic entities
- synthetic generator with scripted route behavior
- stateless Deviation Detector emitting `OUT_OF_RANGE` or `IN_RANGE` for every eligible ping
- point-to-segment perpendicular distance calculation
- `deviation-state:{entity_id}` in Alert Evaluator
- `last_processed_ms` replay guard
- `DEVIATION_SUSTAINED_PINGS` threshold
- deterministic `ROUTE_DEVIATION` emission

## Required Failure Experiments

- replay same `OUT_OF_RANGE` timestamp; count does not double-increment
- return in range mid-episode; state resets
- go out of range again; new episode starts
- route-deviation alert appears through the existing API without special-case delivery code

## Exit Criteria

Signal-loss and route-deviation alerts coexist through the same canonical alert pipeline, and replay does not corrupt episode state or duplicate durable alerts.
