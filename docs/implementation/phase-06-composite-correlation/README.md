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
| Pre-CP3A | — | Define the crash-safe claim/publish protocol for consuming a signal-loss episode into a `COMPOSITE` | **Pending** |
| CP3 | — | Implement the atomic Redis claim (whatever Pre-CP3A resolves) | Pending |
| CP4 | — | Deterministic `COMPOSITE` construction (pure builder, no Redis mutation) | Pending |
| CP5 | — | Wire claim + publish into `handleProximityCandidate`; API atomic supersession | Pending |
| CP6 | — | Exit verification: failure experiments, real-state inspection | Pending |

Look up any commit's full diff with `git show <hash>` from the repository root.

**Pre-CP3A is explicitly unresolved.** Two real correctness problems were identified during design discussion and are not yet decided:

1. A naive "claim episode, then publish" design permanently loses the composite if the process crashes between the two — the claim burns the only chance, with no retry path (unlike signal-loss, which the docs explicitly accept losing one alert for; unlike proximity candidates, which get a "next ping" retry that composite correlation has no equivalent of).
2. A naive "publish, then delete `recent-loss`" design (the wording `DATA_MODEL.md` currently uses) can cause a Kafka-redelivered candidate to find no episode at all, decide "not eligible," and wrongly publish a duplicate `UNSCHEDULED_PROXIMITY` alongside an already-published `COMPOSITE` — two different alert types/IDs, not a harmless idempotent duplicate.

Do not treat either resolution sketched in prior conversation as accepted. Nothing about the claim/publish state machine is implemented, and `DATA_MODEL.md` has not yet been revised for it.

---

## Contents

| Path | Description |
| --- | --- |
| [`phase-06-composite-correlation.md`](phase-06-composite-correlation.md) | Original phase plan: goal, paths to test, required failure experiments, exit criteria |
| [`concepts/`](concepts/README.md) | Concept notes and checkpoint debriefs, in reading order |

No `exit-verification.md` yet — Phase 06 is not complete. It will be added once every checkpoint above is Done, following the Phase 05 pattern.
