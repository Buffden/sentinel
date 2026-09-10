# Composite Alert Builder: Design and Learning Reference

Plain language first, then technical depth, then the code. Use this to understand, inspect, and defend CP4.

---

## The one question this builder answers, and the ones it deliberately doesn't

`buildCompositeAlert` answers exactly: *given a decision that has already been made, and the candidate evidence that decision was made against, what is the exact `COMPOSITE` alert object?* It does not decide whether this candidate should become a `COMPOSITE` (CP2's eligibility resolution and CP3C's decision record already did that), it does not claim or finalize a signal-loss episode (CP3B), and it does not touch Redis or Kafka at all. It is the last, purely mechanical step: turning an already-frozen decision into the concrete payload shape `DATA_MODEL.md` specifies.

This narrowness is deliberate, continuing the same one-checkpoint-one-mechanism discipline CP3A/CP3B/CP3C already established:

| | Candidate decision (CP3C) | Alert builder (CP4) |
| --- | --- | --- |
| Answers | What did we decide for this exact candidate | What does that decision look like as an alert |
| Reads | `alert-decision:{candidate_id}` from Redis | Nothing, it takes its inputs as plain arguments |
| Can fail on | A conflicting prior decision (throws `CandidateDecisionConflictError`) | A decision whose `selected_entity_id` isn't actually in the candidate pair (throws) |

---

## Concepts in plain language

### Why this has to be a pure function, not a helper that reaches into Redis or the clock

Every other piece of Phase 06 built so far exists specifically to make the *decision* durable and replay-safe: the claim (CP3B) stops two candidates fighting over the same loss episode, and the decision record (CP3C) stops a Kafka redelivery flipping that decision. None of that protection is worth anything if the *shape of the alert itself* can vary between the first attempt and a redelivered retry. If `buildCompositeAlert` read `Date.now()` or `config.COMPOSITE_CORRELATION_WINDOW_MS` internally, two calls for the identical decision (one before a crash, one after Kafka redelivers the same `proximity.candidates` message) could legitimately produce two different `detected_at_ms` or `correlation_window_ms` values. That's not a correctness bug in the sense of violating an invariant like `alert_id` uniqueness, but it does mean "the same logical event" gets represented two different ways, which undermines the entire point of building this as a deterministic step. So every operational value the builder would otherwise be tempted to read for itself, `entity_type`, `detectedAtMs`, `correlationWindowMs`, is instead a parameter the caller supplies. The guarantee this buys is narrow but exact: **same explicit inputs → same output**, always, with nothing hidden inside the function that could make that untrue.

Note what this guarantee does *not* claim by itself: CP4 doesn't guarantee the caller (CP5A) actually passes a stable `detectedAtMs`/`correlationWindowMs` across a redelivery. That's CP5A's responsibility once it exists; CP4 only guarantees that *if* it does, the output is identical.

### Why the payload is nested, not flat like `UNSCHEDULED_PROXIMITY`'s

`DATA_MODEL.md` specifies COMPOSITE's payload as "nested signal-loss + proximity evidence", read literally here as two distinct sub-objects (`payload.signal_loss`, `payload.proximity`), not flattened into one object the way `UNSCHEDULED_PROXIMITY`'s payload happens to be. A `COMPOSITE` alert is fundamentally reporting on two separate pieces of evidence that happened to correlate: the loss episode and the proximity encounter. Keeping them as separate sub-objects mirrors that in the data itself, rather than mixing fields from two different sources into one flat namespace where a reader can't easily tell which evidence category a field belongs to.

`entity_type` deliberately does **not** appear inside either nested object, even though it's readily available at build time. It already exists as a top-level field on the alert (`entity_type`, alongside `entity_id`); duplicating it inside `payload.signal_loss` or `payload.proximity` would just create a second value that could drift from the first with no mechanism keeping them in sync.

### Why `priority` is `ELEVATED`, not `STANDARD`

`DATA_MODEL.md` defines `priority` as `STANDARD` or `ELEVATED` but did not originally state which `alert_type` gets which value; `SIGNAL_LOSS` and `UNSCHEDULED_PROXIMITY` both hardcode `STANDARD` in `evaluator.ts`. The first implementation pass copied that pattern for `COMPOSITE` without checking whether it actually fit. It didn't: US-06 (`docs/use-cases/US-06-composite-alert/composite-alert.md`) states the story explicitly, correlating a signal-loss episode with an unscheduled-proximity episode into "one elevated incident," and `ELEVATED` was, until this checkpoint, unused anywhere in the codebase. `DATA_MODEL.md` now documents the mapping explicitly (`SIGNAL_LOSS`/`UNSCHEDULED_PROXIMITY` are `STANDARD`, `COMPOSITE` is `ELEVATED`) so this is an accepted contract other alert types can be checked against later, not an implicit default buried in this one builder.

### Why `entity_id`/`counterparty_entity_id` come from the decision, and fail closed on mismatch

`decision.selected_entity_id` is the pair member whose signal-loss episode this composite is anchored to: that's the entity the whole correlation is *about*, so it becomes the alert's primary `entity_id`, the same convention `UNSCHEDULED_PROXIMITY` already uses for its own primary/counterparty split. The other pair member becomes `counterparty_entity_id`, found by comparing `selected_entity_id` against `candidate.entity_a_id`/`entity_b_id`.

If `selected_entity_id` matches neither, that's not a case to guess through: it means the `CompositeCandidateDecision` and the `ProximityCandidateMessage` don't actually describe the same encounter, which should never happen if CP2/CP3C's identity discipline held upstream. Silently falling back to a default (e.g. always treating `entity_a_id` as counterparty) would produce an alert that looks structurally valid but names the wrong relationship, exactly the kind of quiet corruption that's hardest to catch later, since nothing about the resulting alert looks malformed on its face. `buildCompositeAlert` throws instead, the same fail-closed posture `CandidateDecisionConflictError` already applies to a conflicting decision write.

### Why `alert_id` uses `dark_since_ms`, not `episode_start_ms`

`DATA_MODEL.md`'s canonical identity is `{pair_key}:COMPOSITE:{dark_since_ms}`, anchored to the signal-loss episode being upgraded, not the proximity episode that triggered the upgrade. This matters because the same signal-loss episode could in principle correlate with more than one proximity candidate before its correlation window closes (though CP3B's claim prevents more than one of them from actually winning); anchoring identity to `dark_since_ms` means the composite's identity tracks *which loss episode got upgraded*, which is the fact worth deduplicating on, rather than *which specific candidate happened to trigger it*.

### Why the candidate's own evidence isn't re-validated here

`buildCompositeAlert` trusts `candidate.pair_key`/`entity_a_id`/`entity_b_id`/`lat`/`lon`/`distance_at_detection` as given. It doesn't re-derive `pair_key` from the two entity ids or re-check that `entity_a_id <= entity_b_id`. Those invariants belong to whoever constructs a `ProximityCandidateMessage` in the first place (the Correlation Worker, per `DATA_MODEL.md`). CP4's contract is with the already-validated decision and candidate objects it's handed, not with re-verifying every upstream invariant a third time.

---

## Map to code

| Concept | Where |
| --- | --- |
| Payload shape | `CompositeAlertPayload`, `services/alert-evaluator/src/composite.ts` |
| Full alert shape | `CompositeAlert`, same file |
| The builder itself | `buildCompositeAlert`, same file |
| Its inputs | `CompositeCandidateDecision` (CP3C) and `ProximityCandidateMessage` (`services/alert-evaluator/src/evaluator.ts`, imported as a type-only import to avoid a runtime circular dependency once CP5A makes `evaluator.ts` import from `composite.ts`) |

---

## Retention questions

1. Why does `buildCompositeAlert` take `entityType`, `detectedAtMs`, and `correlationWindowMs` as parameters instead of reading them from Redis, `config`, or the clock itself?
2. Why is the payload nested (`signal_loss` / `proximity`) instead of flat like `UNSCHEDULED_PROXIMITY`'s payload?
3. Why does `entity_type` not appear inside `payload.signal_loss` or `payload.proximity`, even though it would be easy to include?
4. Walk through what would happen, concretely, downstream, if the counterparty-mismatch guard silently picked `candidate.entity_a_id` instead of throwing.
5. Why is `alert_id` built from `dark_since_ms` rather than `episode_start_ms`?
6. Why is `COMPOSITE`'s `priority` `ELEVATED` when `SIGNAL_LOSS` and `UNSCHEDULED_PROXIMITY` are both `STANDARD`, and what accepted document justifies that?

---

## Completion checklist

- [ ] I can explain why "same explicit inputs → same output" is a narrower, more precise claim than "same decision + candidate always gives identical output"
- [ ] I can explain why `entity_type` is deliberately excluded from the nested payload
- [ ] I can explain why the mismatch guard throws instead of defaulting to a counterparty
- [ ] I can explain why COMPOSITE's `alert_id` anchors to the loss episode's `dark_since_ms`, not the proximity candidate's `episode_start_ms`
- [ ] I can explain why `COMPOSITE`'s priority is `ELEVATED` and where that rule is now documented
- [ ] I ran the unit test suite and the manual inspection myself and can interpret both
