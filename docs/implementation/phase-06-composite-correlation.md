# Phase 06 — Composite Correlation

## Goal

Implement Sentinel's key correlated anomaly:

```text
SIGNAL_LOSS + PROXIMITY → COMPOSITE
```

This is the most complex state machine in the system. Implement and test both paths completely before moving on.

---

## Two Paths to Test

### Active-dark path

Entity A goes dark. While still dark, Entity A's last-known position comes close to Entity B.

Expected outcome:

- `alert-state:{entity_a_id}` exists with `composite_issued=0`
- Alert Evaluator detects the proximity via `proximity.candidates`
- emits `COMPOSITE` alert with `supersedes_alert_ids` referencing the `SIGNAL_LOSS` alert
- sets `composite_issued=1` on `alert-state:{entity_a_id}`
- a second proximity event for the same episode does not emit a second `COMPOSITE`

### Recent-loss path

Entity A goes dark, a `SIGNAL_LOSS` is emitted. Entity A resumes broadcasting. Position Consumer writes `recent-loss:{entity_a_id}` and deletes `alert-state:{entity_a_id}`. Entity A then comes close to Entity B within `COMPOSITE_CORRELATION_WINDOW_MS`.

Expected outcome:

- `recent-loss:{entity_a_id}` exists
- Alert Evaluator emits `COMPOSITE` and DELs `recent-loss:{entity_a_id}`
- subsequent proximity events for Entity A find no `recent-loss` key and produce `UNSCHEDULED_PROXIMITY`

### Window expiry

Entity A goes dark, resumes, but no proximity occurs within `COMPOSITE_CORRELATION_WINDOW_MS`.

Expected outcome:

- `recent-loss:{entity_a_id}` TTL expires
- subsequent proximity produces `UNSCHEDULED_PROXIMITY` only

---

## Learning Goals

- stateful event correlation across time
- temporal correlation windows using Redis TTLs as enforcement
- consumed keys as "single-use correlation opportunities"
- event time (why `dark_since_ms` uses source telemetry timestamp)
- incident supersession and what it means for the operator

---

## Exit Criteria

- all three paths above pass deterministic tests using the synthetic generator
- a `COMPOSITE` alert is emitted exactly once per signal-loss episode regardless of how many proximity events arrive
- the `SIGNAL_LOSS` alert referenced in `supersedes_alert_ids` is correctly identified
- the correlation window enforces the boundary: proximity after expiry produces `UNSCHEDULED_PROXIMITY`
