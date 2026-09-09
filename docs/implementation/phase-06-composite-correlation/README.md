# Phase 06 — Composite Correlation

## Goal

Implement Sentinel's key correlated anomaly:

```text
SIGNAL_LOSS + UNSCHEDULED PROXIMITY → COMPOSITE
```

See [`phase-06-composite-correlation.md`](phase-06-composite-correlation.md) for the original phase plan (goal, paths to test, required failure experiments, exit criteria) — the source plan this README tracks progress against, updated in place only for accepted architectural changes (per `CLAUDE.md`), not rewritten casually.

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
| CP3C | `1009865` | Implement `readCandidateDecision`/`writeCandidateDecisionIfAbsent` for `alert-decision:{pair_key}:{episode_start_ms}` — write-once immutable, idempotent create, throws on conflict rather than silently overwriting. Not wired into Kafka handling; no deletion logic yet | Done |
| CP4 | — | Pure, deterministic `COMPOSITE` alert builder — no Redis, no Kafka, no API. Alert Evaluator only | Pending |
| CP5A | — | Wire CP2 → CP3B → CP3C → CP4 into `handleProximityCandidate`; publish `COMPOSITE`/`UNSCHEDULED_PROXIMITY`; finalize + decision cleanup ordered after `commitOffsets()`. Alert Evaluator only | Pending |
| CP5B | — | API atomic `COMPOSITE` insert + supersession of referenced active alerts, in one DB transaction. API only | Pending |
| CP5C | — | SVG mockup → developer approval → minimal `COMPOSITE`/`SUPERSEDED` presentation, driven entirely by `supersedes_alert_ids` on the already-existing WebSocket path (Phase 03) — no Phase 08 fan-out infrastructure required. Dashboard only | Pending |
| CP6 | — | Backend failure experiments (including `NEW`/`ACKNOWLEDGED` → `SUPERSEDED`, `RESOLVED` stays terminal) + a UI sanity pass for the transitions CP5C actually renders | Pending |

Look up any commit's full diff with `git show <hash>` from the repository root.

**CP4/CP5A/CP5B/CP5C replace the earlier single "CP4"/"CP5" sketch** after review (including a cross-check against a second model) surfaced two corrections worth recording:

1. **CP5B pulls "atomic COMPOSITE insert + supersession" forward from Phase 08's plan.** Phase 08's plan document currently lists this under its own "What to Build" and exit criteria — that has to be trimmed once CP5B is accepted, or the same feature is scheduled twice. This isn't just tidiness: without CP5B, a published `COMPOSITE` would land as a plain fourth row next to the alerts it's supposed to replace (the API's alert sink has no special-casing by type, by design since Phase 05), which means Phase 06 wouldn't actually deliver on its own goal of consolidating weak signals. CP5B has to happen inside Phase 06 for Phase 06 to work as designed.
2. **CP5C must not fake Phase 08's ACK/Resolve controls to demonstrate `ACKNOWLEDGED → SUPERSEDED`.** `PATCH /alerts/:alert_id` doesn't exist yet — that's genuinely Phase 08 scope. CP5C's UI can only show `NEW → SUPERSEDED` for real, since `NEW` is where every alert already arrives with no operator action needed. `ACKNOWLEDGED → SUPERSEDED` and `RESOLVED` staying terminal are CP6 backend tests (seed the state directly), not something the CP5C UI needs to demonstrate.

CP5C is gated by `CLAUDE.md`'s existing Workspace Visual Language rule: a low-fidelity SVG mockup, developer approval, *then* implementation — not implied, an explicit first step. The open design question the mockup has to resolve: how are superseded individual alerts shown — grayed with a badge in the flat feed, collapsed as children under the composite card, or removed from the active feed entirely. Current lean is "composite as parent, superseded evidence nested underneath," partly because correlation is the project's central demo story and partly because `supersedes_alert_ids` is already a list in the data model — a parent/children UI mirrors that shape instead of reconstructing it from a flat badge — but this gets decided from the actual mockup, not from planning prose.

CP5A's decision-cleanup ordering (`create decision -> publish output -> finalize claim -> commit input offset -> delete decision`) is already the accepted design in `DATA_MODEL.md`'s "Composite claim and decision protocol" (Pre-CP3A) — CP5A's job is to implement it against the real `autoCommit: false` / `commitOffsets()` boundary already in `evaluator.ts`, and its required failure tests must include both crash-before-commit (decision survives, redelivery reproduces the same result) and crash-after-commit-before-delete (orphaned decision — a harmless cleanup leak, not a correctness failure).

---

**Pre-CP3A's resolution** (see [`concepts/composite-claim-protocol/`](concepts/composite-claim-protocol/) and `DATA_MODEL.md`'s "Composite claim and decision protocol"): claims are identified by the full proximity-episode identity (`{pair_key}:{episode_start_ms}`), not a bare `pair_key`; the `alert-state` → `recent-loss` handoff must become a single atomic Lua transfer once claim fields are mutable, not a read-then-`MULTI`-write; and a separate candidate-level decision record (`alert-decision:{pair_key}:{episode_start_ms}`) makes each candidate's alert-type decision sticky across Kafka redelivery in **both** directions (COMPOSITE cannot flip to UNSCHEDULED_PROXIMITY or vice versa on replay). The handoff (CP3A), CLAIM/FINALIZE (CP3B, see [`concepts/composite-episode-claim/`](concepts/composite-episode-claim/)), and the decision record (CP3C, see [`concepts/candidate-decision-record/`](concepts/candidate-decision-record/)) are all implemented; only wiring them into `handleProximityCandidate` (CP5A), the API's atomic supersession (CP5B), and the frontend presentation (CP5C) are not.

**CP3A, CP3B, and CP3C are three separate Redis correctness mechanisms, not one "CP3."** CP3A touches only the Position Consumer's handoff and adds no Alert Evaluator claim code. CP3B adds CLAIM/FINALIZE with no Kafka emission, no `alert-decision` records, and no `handleProximityCandidate` wiring. CP3C adds the decision record with no Kafka wiring and no deletion logic (that depends on the input-offset lifecycle CP5A wires up). A `proximity.candidates` message still unconditionally becomes `UNSCHEDULED_PROXIMITY` today — all three mechanisms exist and are fully tested in isolation, but `handleProximityCandidate` calls none of them yet. Each got its own commit and its own concept documentation. CP4/CP5A/CP5B/CP5C carry the same discipline forward — one service per checkpoint, no bundling.

---

## Contents

| Path | Description |
| --- | --- |
| [`phase-06-composite-correlation.md`](phase-06-composite-correlation.md) | Original phase plan: goal, paths to test, required failure experiments, exit criteria |
| [`concepts/`](concepts/README.md) | Concept notes and checkpoint debriefs, in reading order |

No `exit-verification.md` yet — Phase 06 is not complete. It will be added once every checkpoint above is Done, following the Phase 05 pattern.
