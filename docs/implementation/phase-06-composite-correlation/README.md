# Phase 06 — Composite Correlation

## Goal

Implement Sentinel's key correlated anomaly:

```text
SIGNAL_LOSS + UNSCHEDULED PROXIMITY → COMPOSITE
```

See [`phase-06-composite-correlation.md`](phase-06-composite-correlation.md) for the original phase plan (goal, paths to test, required failure experiments, exit criteria) — unchanged, kept as the source plan this README tracks progress against.

---

## Checkpoint progress

| Checkpoint | Commit | Scope | Status |
| --- | --- | --- | --- |
| CP1 | `7e67457` | Bound `recent-loss` correlation window with an atomic `MULTI` (`HSET` + `PEXPIRE` + `DEL`) TTL handoff | Done |
| Pre-CP2A | `9e67ad7` | Restore ADR-005: scope Alert Evaluator candidate-consumer Kafka group membership to the perceived lease holder | Done |
| Pre-CP2B | `4952e95` | Define the composite correlation eligibility formula and both-entities-qualify tie-break in `DATA_MODEL.md`, before any code depended on it | Done |
| CP2 | `1efa70c` | Read-only composite eligibility resolution (`resolveEntityLossEpisode`, `selectWinningEpisode`, `resolveCompositeEligibility`) — no Redis mutation, no Kafka emission, not wired into `handleProximityCandidate` | Done |
| Pre-CP3A | — | Design/document the crash-safe claim + decision protocol for consuming a signal-loss episode into a `COMPOSITE` — no code | Done |
| CP3 | — | Implement the CLAIM/FINALIZE Lua scripts, the decision-record read/write, and the revised atomic `alert-state` → `recent-loss` handoff, per Pre-CP3A | **Pending** |
| CP4 | — | Deterministic `COMPOSITE` construction (pure builder, no Redis mutation) | Pending |
| CP5 | — | Wire claim + publish into `handleProximityCandidate`; API atomic supersession | Pending |
| CP6 | — | Exit verification: failure experiments, real-state inspection | Pending |

Look up any commit's full diff with `git show <hash>` from the repository root. Pre-CP3A's commit hash will be filled in once it's actually committed.

**Pre-CP3A's resolution** (see [`concepts/composite-claim-protocol/`](concepts/composite-claim-protocol/) and `DATA_MODEL.md`'s "Composite claim and decision protocol"): claims are identified by the full proximity-episode identity (`{pair_key}:{episode_start_ms}`), not a bare `pair_key`; the `alert-state` → `recent-loss` handoff must become a single atomic Lua transfer once claim fields are mutable, not the current read-then-`MULTI`-write; and a separate candidate-level decision record (`alert-decision:{pair_key}:{episode_start_ms}`) makes each candidate's alert-type decision sticky across Kafka redelivery in **both** directions (COMPOSITE cannot flip to UNSCHEDULED_PROXIMITY or vice versa on replay). None of this is implemented — CP3 is where it gets built.

---

## Contents

| Path | Description |
| --- | --- |
| [`phase-06-composite-correlation.md`](phase-06-composite-correlation.md) | Original phase plan: goal, paths to test, required failure experiments, exit criteria |
| [`concepts/`](concepts/README.md) | Concept notes and checkpoint debriefs, in reading order |

No `exit-verification.md` yet — Phase 06 is not complete. It will be added once every checkpoint above is Done, following the Phase 05 pattern.
