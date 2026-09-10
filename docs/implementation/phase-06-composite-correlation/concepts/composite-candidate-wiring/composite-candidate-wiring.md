# Composite Candidate Wiring: Design and Learning Reference

Plain language first, then technical depth, then the code. Use this to understand, inspect, and defend CP5A.

---

## What this checkpoint wires together, and what it deliberately doesn't touch

CP5A is the orchestration layer that turns every prior Phase 06 mechanism, built and tested in isolation, into the live behavior of `handleProximityCandidate`: CP2's eligibility resolution, CP3A/CP3B's claim protocol, CP3C's sticky decision record, and CP4's pure alert builder. Before this checkpoint, `handleProximityCandidate` unconditionally published `UNSCHEDULED_PROXIMITY` for every candidate; none of those mechanisms were ever called. CP5A is the first point where a `proximity.candidates` message can actually become a `COMPOSITE` alert.

CP5A does not touch the API, the frontend, or CP5B's atomic supersession. It also does not add anything CP4, CP3C, or CP3B didn't already define; its job is sequencing existing primitives correctly, plus the small number of new primitives the design review before this checkpoint (`DATA_MODEL.md`'s Pre-CP5A resolutions) specifically identified as missing: reason-aware FINALIZE, a claim-release primitive, and the decision to retain (not delete) decision records.

---

## Concepts in plain language

### The four branches `handleProximityCandidate` can take

Every `proximity.candidates` message resolves through exactly one of these:

1. **A decision record already exists** (redelivery). Replay it: build the alert from the stored decision, publish, and if it's `COMPOSITE`, FINALIZE its episode too, whether or not an earlier attempt already reached FINALIZE.
2. **Fresh candidate, CP2 finds no qualifying loss (or CLAIM on the winner fails, for any reason).** Decide `UNSCHEDULED_PROXIMITY`.
3. **Fresh candidate, CP2 finds a winner, CLAIM succeeds.** Decide `COMPOSITE`, build via CP4, publish, FINALIZE.
4. **Fresh candidate, this process's own CLAIM succeeded, but a different decision for the same `candidate_id` already won the write race.** Release the stray claim, adopt the canonical decision, and process it exactly like branch 1.

Branches 1, 3, and 4 all end up calling the same `publishCompositeAlert`/`publishDecision` code, not three separate implementations. That's deliberate: "replay an existing decision," "process a decision I just made," and "process a decision someone else made that I have to adopt" are the same operation once a `CandidateDecision` object exists, regardless of where it came from.

### Why CLAIM failure never triggers a fallback to the other pair member

`claimCompositeEpisode` collapses `CLAIMED_BY_OTHER`, `ALREADY_ISSUED`, and `NO_EPISODE` into one `false`, and CP5A does not try to distinguish them, because all three mean the same thing here: the episode CP2's deterministic tie-break selected is not available to this candidate. Retrying against the non-selected pair member would make the final correlation depend on which entity's CLAIM happened to fail, not on the accepted source-time tie-break, so the result becomes `UNSCHEDULED_PROXIMITY` outright (see `DATA_MODEL.md`'s Pre-CP5A(a)).

### Why a decision-write conflict releases a claim instead of just failing

`writeCandidateDecisionIfAbsent` throwing `CandidateDecisionConflictError` means a different decision already exists for this exact `candidate_id`, reachable under the same leader-overlap ADR-005 already accepts (not merely hypothetical: `DATA_MODEL.md`'s Pre-CP5A(b) works through a real Redis-state-divergence construction). If this process's own CP2 resolution had already succeeded at CLAIM before hitting the conflict, that claim is now stray: nobody will ever FINALIZE it, and since `alert-state` carries no TTL, an unreleased claim can block every future candidate from ever claiming that episode. `handleProximityCandidate` releases it (best-effort; the release's own outcome doesn't block adopting the canonical decision) before falling through to the same replay path branch 1 uses. Claim and finalize ownership is keyed by `candidate_id`, not by which process instance calls the Lua, so finalizing the *adopted* decision (if it's `COMPOSITE`) is exactly as legitimate as if this process had written it itself.

### Why FINALIZE's three outcomes are handled differently, not collapsed to a boolean

`SUCCESS` and `NO_EPISODE` both let processing complete (the second just logs a warning: the key is gone, so nothing else can reuse or corrupt it, and the alert already published stands as the only evidence). `NOT_CLAIMED` is different in kind: the episode still exists, but its claim no longer matches this `candidate_id`, an invariant violation, not a routine outcome. `publishCompositeAlert` throws `CompositeFinalizeInvariantError` in that case, and the throw is what prevents the caller from ever reaching `commitOffsets()` for this message, since `startCandidateConsumerSession`'s `eachMessage` calls `commitOffsets()` only after `handleProximityCandidate` resolves.

### Why nothing deletes the decision record

CP5A does not add a `deleteCandidateDecision` function and does not set a TTL. `DATA_MODEL.md`'s Pre-CP5A(d) works through why: deleting immediately after commit is unsafe under the same leader-overlap this whole design already accepts (a slower, still-alive second process can free-write a *different* decision for the same `candidate_id` after the first one deletes it), and a TTL doesn't fix that if the record is still explicitly deleted in the common case. For this checkpoint, `alert-decision:*` records are retained indefinitely once written; reclaiming them safely is deferred to a later checkpoint, once `proximity.candidates` has an explicit, documented retention/replay contract to size a TTL against.

### Where `entity_type` and `detected_at_ms` come from

Both are read fresh inside `handleProximityCandidate` on every call, replay included, never inside CP4's pure `buildCompositeAlert`. `entity_type` comes from `entity:live:{selected_entity_id}`, the pair member the composite is actually anchored to, which is not always `entity_a_id` the way `UNSCHEDULED_PROXIMITY`'s primary entity always is. `detected_at_ms` is `Date.now()`, deliberately allowed to differ across a replay or a redelivery, since it's processing time, never part of `alert_id`.

---

## Map to code

| Concept | Where |
| --- | --- |
| Full orchestration | `handleProximityCandidate`, `services/alert-evaluator/src/evaluator.ts` |
| `UNSCHEDULED_PROXIMITY` build/publish | `publishUnscheduledProximityAlert`, same file |
| `COMPOSITE` build/publish/FINALIZE | `publishCompositeAlert`, same file |
| Branch dispatch (replay, fresh, adopted decision alike) | `publishDecision`, same file |
| Reason-aware FINALIZE | `FinalizeResult`, `finalizeCompositeEpisode`, `services/alert-evaluator/src/composite.ts` |
| FINALIZE's invariant-violation throw | `CompositeFinalizeInvariantError`, same file |
| Stray-claim release | `releaseCompositeClaim`, same file |
| Design resolutions this wiring implements | `DATA_MODEL.md`'s "Composite claim and decision protocol", Pre-CP5A(a)-(d) |

---

## Retention questions

1. Why do branches 1, 3, and 4 all funnel through the same `publishDecision`/`publishCompositeAlert` code instead of three separate implementations?
2. Why does a CLAIM failure never retry against the other pair member, even when it independently qualifies?
3. Walk through, in order, what `handleProximityCandidate` does when its own CLAIM succeeds but the subsequent decision write conflicts with an already-canonical decision.
4. Why is `NOT_CLAIMED` a thrown error while `NO_EPISODE` is a warning, when both mean FINALIZE didn't set `composite_issued`?
5. Why does CP5A not delete `alert-decision:*` records, even though CP3C's original design assumed it would?
6. Why is `entity_type` read from `entity:live:{selected_entity_id}` rather than always `entity_a_id`?

---

## Completion checklist

- [ ] I can trace all four branches `handleProximityCandidate` can take, from a real `proximity.candidates` message to a real published alert
- [ ] I can explain why CLAIM failure has no fallback to the other pair member
- [ ] I can explain the decision-conflict recovery sequence (release, adopt, replay-process) and why claim ownership is keyed by `candidate_id`, not process identity
- [ ] I can explain why `NOT_CLAIMED` blocks the offset commit and `NO_EPISODE` does not
- [ ] I can explain why decision records are retained indefinitely in this checkpoint, and what has to be true before that changes
- [ ] I ran the integration suite and the manual inspection myself and can interpret both
