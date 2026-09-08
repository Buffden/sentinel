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
| Pre-CP3A | `2fd1104` | Design/document the crash-safe claim + decision protocol for consuming a signal-loss episode into a `COMPOSITE` — no code | Done |
| CP3A | `222bd3d` | Make the `alert-state` → `recent-loss` handoff coordination-safe: a single atomic Lua transfer, not read-then-`MULTI`-write. Position Consumer only; no Alert Evaluator claim code | Done |
| CP3B | `927c2d9` | Implement `claimCompositeEpisode`/`finalizeCompositeEpisode` — representation-independent Lua primitives across `alert-state`/`recent-loss`, identified by `entity_id` + `expected_dark_since_ms` + `candidate_id`. No Kafka emission, no `alert-decision` records, not wired into `handleProximityCandidate` | Done |
| CP3C | — | Implement `readCandidateDecision`/`writeCandidateDecisionIfAbsent` for `alert-decision:{pair_key}:{episode_start_ms}` — write-once immutable, idempotent create, throws on conflict rather than silently overwriting. Not wired into Kafka handling; no deletion logic yet | Done |
| CP4 | — | Deterministic `COMPOSITE` construction (pure builder, no Redis mutation) | Pending |
| CP5 | — | Wire claim + decision + publish into `handleProximityCandidate`; API atomic supersession; decision-record deletion after input offset commit | Pending |
| CP6 | — | Exit verification: failure experiments, real-state inspection | Pending |

Look up any commit's full diff with `git show <hash>` from the repository root. CP3C's commit hash will be filled in once it's actually committed.

**Pre-CP3A's resolution** (see [`concepts/composite-claim-protocol/`](concepts/composite-claim-protocol/) and `DATA_MODEL.md`'s "Composite claim and decision protocol"): claims are identified by the full proximity-episode identity (`{pair_key}:{episode_start_ms}`), not a bare `pair_key`; the `alert-state` → `recent-loss` handoff must become a single atomic Lua transfer once claim fields are mutable, not a read-then-`MULTI`-write; and a separate candidate-level decision record (`alert-decision:{pair_key}:{episode_start_ms}`) makes each candidate's alert-type decision sticky across Kafka redelivery in **both** directions (COMPOSITE cannot flip to UNSCHEDULED_PROXIMITY or vice versa on replay). The handoff (CP3A), CLAIM/FINALIZE (CP3B, see [`concepts/composite-episode-claim/`](concepts/composite-episode-claim/)), and the decision record (CP3C, see [`concepts/candidate-decision-record/`](concepts/candidate-decision-record/)) are all implemented; only wiring them into `handleProximityCandidate` (CP5) is not.

**CP3A, CP3B, and CP3C are three separate Redis correctness mechanisms, not one "CP3."** CP3A touches only the Position Consumer's handoff and adds no Alert Evaluator claim code. CP3B adds CLAIM/FINALIZE with no Kafka emission, no `alert-decision` records, and no `handleProximityCandidate` wiring. CP3C adds the decision record with no Kafka wiring and no deletion logic (that depends on the input-offset lifecycle CP5 wires up). A `proximity.candidates` message still unconditionally becomes `UNSCHEDULED_PROXIMITY` today — all three mechanisms exist and are fully tested in isolation, but `handleProximityCandidate` calls none of them yet. Each got its own commit and its own concept documentation.

---

## Contents

| Path | Description |
| --- | --- |
| [`phase-06-composite-correlation.md`](phase-06-composite-correlation.md) | Original phase plan: goal, paths to test, required failure experiments, exit criteria |
| [`concepts/`](concepts/README.md) | Concept notes and checkpoint debriefs, in reading order |

No `exit-verification.md` yet — Phase 06 is not complete. It will be added once every checkpoint above is Done, following the Phase 05 pattern.
