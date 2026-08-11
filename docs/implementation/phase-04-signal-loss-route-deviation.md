# Phase 04 — Signal Loss + Route Deviation

## Goal

Implement the first two anomaly types independently before combining them.

---

## Signal Loss

### What to build

- Redis-based Alert Evaluator leader lease (SET NX PX; Lua compare-and-expire renewal; compare-before-DEL release)
- scheduled scan of `entity:live:*` hashes — reads `last_seen_ms`, compares to `SIGNAL_LOSS_THRESHOLD_MS`
- signal-loss episode state (`alert-state:{entity_id}` hash)
- deterministic `SIGNAL_LOSS` alert emission to `alerts` topic

### Learning goals

- leader election and lease semantics
- why absence cannot be detected from a stream
- scheduled polling vs event-driven detection
- distributed coordination under failure
- failover and lease expiry

### Required failure experiments

- run two Alert Evaluator instances; confirm only one holds the lease
- kill the leader; confirm the follower acquires the lease and resumes scanning
- revive the old leader; confirm it cannot renew a lease now held by the new leader
- trigger signal loss repeatedly on the same dark entity; confirm exactly one alert is emitted

---

## Route Deviation

### What to build

```text
position.normalized
        ↓
Deviation Detector
        ↓
deviation.candidates
        ↓
Alert Evaluator
```

- seed `route_references` and `route_reference_points` for synthetic entities
- synthetic generator publishes scripted pings to `position.normalized`
- Deviation Detector: stateless — classifies every eligible ping as `OUT_OF_RANGE` or `IN_RANGE`
- point-to-segment perpendicular distance calculation
- Alert Evaluator: `deviation-state:{entity_id}` episode state, `last_processed_ms` replay guard, `DEVIATION_SUSTAINED_PINGS` threshold, `ROUTE_DEVIATION` alert emission

### Learning goals

- stateless classification vs stateful episode tracking
- event-time replay guards
- sustained rule evaluation (threshold over multiple pings)
- geographic distance calculations

### Required failure experiments

- replay `OUT_OF_RANGE` events with the same timestamps; confirm count does not double-increment
- move a synthetic entity in-range mid-episode; confirm episode resets and no alert is emitted
- move it back out-of-range; confirm a new episode starts from zero

---

## Exit Criteria

- signal loss alert is emitted exactly once per dark episode regardless of how many scan cycles pass
- route deviation alert is emitted only after `DEVIATION_SUSTAINED_PINGS` consecutive out-of-range pings
- replay of the same events produces no duplicate durable alerts
- leader failover does not produce a duplicate alert for the same episode
