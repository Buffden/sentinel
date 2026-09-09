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
- crash before the `proximity.candidates` input offset commits (candidate decision survives; redelivery reproduces the same result)
- crash after the offset commits but before decision-record deletion (orphaned decision — a harmless cleanup leak, not a correctness failure)
- a `NEW` referenced alert transitions to `SUPERSEDED` when the composite is inserted
- an `ACKNOWLEDGED` referenced alert also transitions to `SUPERSEDED`
- a `RESOLVED` referenced alert is never retroactively superseded — it remains `RESOLVED`

## Exit Criteria

Active-dark, recent-loss, and expiry paths pass deterministic tests; one COMPOSITE is produced per qualifying signal-loss episode; and the existing alert delivery path persists/exposes it.

**Scope note (added after CP3C, before CP4):** the API's atomic `COMPOSITE` insert + active-alert supersession — originally sketched under Phase 08 — is pulled forward into this phase (CP5B). Without it, a published `COMPOSITE` would land as a plain extra row next to the alerts it's meant to replace, since the API's alert sink has no special-casing by alert type; Phase 06 would not actually deliver its own stated goal (consolidating weak signals into one incident) without this piece. Phase 08's plan is trimmed to remove this responsibility once CP5B lands. A minimal frontend checkpoint (CP5C) is also added — a low-fidelity SVG mockup approved before implementation, showing `COMPOSITE` alerts elevated and `NEW` alerts they supersede visibly marked, using only `supersedes_alert_ids` off the WebSocket path Phase 03 already built. It does not include operator ACK/Resolve controls — those remain Phase 08 scope, and `ACKNOWLEDGED`/`RESOLVED` supersession behavior is verified as a backend test (seeded state), not demonstrated in this phase's UI.
