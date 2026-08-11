# Phase 06 — Composite Correlation

## Goal

Implement Sentinel's key correlated anomaly:

```text
SIGNAL_LOSS + UNSCHEDULED PROXIMITY → COMPOSITE
```

## Paths to Test

### Active-dark
A signal-loss episode is still active when proximity arrives. Emit one COMPOSITE and mark the episode so another candidate cannot emit a second composite.

### Recent-loss
The entity resumes before proximity. Position Consumer writes `recent-loss:{entity_id}` before removing active dark state. If proximity arrives within the TTL window, emit COMPOSITE and consume the recent-loss key.

### Window expiry
If the recent-loss TTL expires before proximity, the later encounter remains `UNSCHEDULED_PROXIMITY` only.

## Required Failure Experiments

- repeated candidate for one signal-loss episode produces one COMPOSITE
- replay after COMPOSITE emission creates no duplicate durable composite
- test events immediately before and after correlation-window expiry
- verify deterministic `supersedes_alert_ids`

## Exit Criteria

Active-dark, recent-loss, and expiry paths pass deterministic tests; one COMPOSITE is produced per qualifying signal-loss episode; and the existing alert delivery path persists/exposes it.
