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
- `SIGNAL_LOSS` arrives at the API before `COMPOSITE` — normal case, supersession applies directly
- `COMPOSITE` arrives at the API before the `SIGNAL_LOSS` it references — the out-of-order case (see Pre-CP5B); both orderings must converge to the identical durable state

## Exit Criteria

Active-dark, recent-loss, and expiry paths pass deterministic tests; one COMPOSITE is produced per qualifying signal-loss episode; and the existing alert delivery path persists/exposes it.

**Scope note (added after CP3C, before CP4):** the API's atomic `COMPOSITE` insert + active-alert supersession — originally sketched under Phase 08 — is pulled forward into this phase (CP5B). Without it, a published `COMPOSITE` would land as a plain extra row next to the alerts it's meant to replace, since the API's alert sink has no special-casing by alert type; Phase 06 would not actually deliver its own stated goal (consolidating weak signals into one incident) without this piece. Phase 08's plan is trimmed to remove this responsibility once CP5B lands. Phase 08's remaining scope is narrower than its current plan text still implies: the API already persists an alert and publishes to Redis `alert-events`, and the WebSocket server already subscribes to that channel for fan-out (`alertSink.ts`/`wsServer.ts`, built in Phase 03) — that basic mechanism is not something Phase 08 still needs to build from scratch. Not corrected in Phase 08's own file today, consistent with changing scope only when we land there; noted here so it isn't lost by then.

**Pre-CP5B (added before CP5B):** supersession cannot assume the referenced `SIGNAL_LOSS` row already exists in the API's database when a `COMPOSITE` arrives. The signal-loss scan writes `alert-state` to Redis before its own Kafka publish completes, and the proximity-candidate consumer runs concurrently on a separate loop — it can see that `alert-state`, resolve `COMPOSITE` eligibility, and publish to the same `alerts` topic before the `SIGNAL_LOSS` message the composite is meant to supersede actually arrives at the API. This is a real race, not a hypothetical one, given the evaluator's Kafka producer has no idempotence guarantee on send-order. CP5B must converge to the same durable state regardless of arrival order; how (an upsert/placeholder pattern on the API side, or some other mechanism) is Pre-CP5B's own design decision, resolved before CP5B is coded, not invented inline during it.

A minimal frontend checkpoint (CP5C) is also added — a low-fidelity SVG mockup approved before implementation, showing `COMPOSITE` alerts elevated and the alerts in `supersedes_alert_ids` visibly marked, rendered generically rather than assuming a fixed set of alert types. In the normal Phase 06 path a candidate is decided `COMPOSITE` instead of `UNSCHEDULED_PROXIMITY`, never both, so `supersedes_alert_ids` typically names one alert (`SIGNAL_LOSS`), not two. CP5C does not include operator ACK/Resolve controls — those remain Phase 08 scope, and `ACKNOWLEDGED`/`RESOLVED` supersession behavior is verified as a backend test (seeded state), not demonstrated in this phase's UI.
