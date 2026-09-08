# Atomic Signal-Loss Handoff — Design and Learning Reference

Plain language first, then technical depth, then the code. Use this to understand, inspect, and defend CP3A.

---

## What changed, and why CP1 wasn't wrong when it was written

CP1's handoff (`recent-loss-handoff` concept, commit `7e67457`) was `HGETALL alert-state` followed by a separate `MULTI` that wrote `recent-loss` from that snapshot. That was genuinely safe at the time — the hash held only static episode evidence (`dark_since_ms`, `signal_loss_alert_id`), and nothing external could change those values between the read and the write in a way that mattered.

Pre-CP3A's design (commit `2fd1104`) added two *mutable* coordination fields to the same hashes: `composite_issued` and `composite_claim_candidate_id`, both writable by the Alert Evaluator at any time via the future CLAIM/FINALIZE primitives (CP3B). That single fact — a field the Position Consumer reads can now be written by a different process between the read and the write — is what turns CP1's safe read-then-write into a real race. This isn't a refactor for its own sake; it's a direct, mechanical consequence of a different checkpoint's design decision, traced by hand before any CLAIM code was written (see `composite-claim-protocol`'s Experiment 2).

---

## Concepts in plain language

### The exact race

![Read-Then-Write Race](../../../../../diagrams/docs/implementation/phase-06-composite-correlation/concepts/atomic-signal-loss-handoff/read-then-write-race.svg)

| Step | Actor | Operation | `alert-state` / `recent-loss` state |
| --- | --- | --- | --- |
| 1 | Position Consumer | `HGETALL alert-state` | reads `composite_claim_candidate_id = ""` |
| 2 | Alert Evaluator | `CLAIM` (Lua, atomic) for candidate `Y` | `alert-state.composite_claim_candidate_id` is now `"Y"` |
| 3 | Position Consumer | `MULTI(HSET recent-loss from step 1, DEL alert-state) EXEC` | `recent-loss.composite_claim_candidate_id` is written as `""` — from the **stale** step-1 snapshot |
| 4 | — | — | `alert-state` is gone; `recent-loss` exists with no claim; candidate `Y`'s claim (written at step 2) is unrecoverable |

The bug isn't in either operation individually — `HGETALL` is a normal read, and the `MULTI` is still internally atomic. The bug is the *gap between* step 1 and step 3, which is not covered by either operation's own atomicity guarantee: nothing prevents step 2 from landing in between.

### Why a single Lua script closes it, and a second `MULTI` wouldn't

A `MULTI` still requires the values to be read *before* queuing the write commands — the read and the write remain two separate round-trips from the client's perspective, so the gap doesn't move, it's just still there. The fix has to make the read *and* the write one atomic unit from Redis's point of view. A Lua script does exactly that: `redis.call('HGET', ...)` followed by `redis.call('HSET', ...)` inside one script executes as a single, uninterruptible unit on the server — no other client's command, including a concurrent CLAIM, can land between them. This is the same reasoning CP1's own concept doc already gave for why `MULTI` was right for HSET+PEXPIRE+DEL (no conditional logic, no external mutable state) and why Lua is used everywhere else in this codebase for compare-and-swap-shaped problems. CP3A's handoff is now the latter, not the former.

### What the script deliberately does *not* do

`SIGNAL_LOSS_HANDOFF_LUA` reads whatever `composite_issued`/`composite_claim_candidate_id` currently hold and carries them forward unconditionally — it does not interpret, validate, or gate on their values. That's intentional: CP3A's job is coordination-safety for the *transfer*, not eligibility or claim logic, which belong entirely to CP2 (already built) and CP3B (not yet built). A handoff that tried to also enforce claim semantics would blur two checkpoints' responsibilities into one piece of code.

---

## Diagrams

Two diagrams, each earning its place for a different reason:

- **The race this checkpoint fixes** (above, `read-then-write-race.svg`) — a genuinely new diagram, because it's the one thing CP1's diagram cannot show: two *concurrent* actors (Position Consumer and Alert Evaluator) both touching `alert-state`. CP1's diagram has only one actor and was never wrong to have only one — this checkpoint exists precisely because a second actor was introduced by a different checkpoint's design (Pre-CP3A).
- **The happy-path handoff shape** (write `recent-loss` fields, attach TTL, remove `alert-state`; binary crash boundary) — unchanged by CP3A, still accurately shown by `recent-loss-handoff-sequence.svg` (CP1's concept). Not redrawn here; see [`recent-loss-handoff`](../recent-loss-handoff/recent-loss-handoff.md) for that diagram. What changed is *which Redis primitive* provides the atomicity (Lua instead of `MULTI`) and *which fields* move (five now, not three) — a code-map and prose change, not a new shape worth a second diagram.

---

## Map to code

| Concept | Where |
| --- | --- |
| Atomic handoff script | `SIGNAL_LOSS_HANDOFF_LUA` — `services/position-consumer/src/consumer.ts` |
| Entry point (unchanged signature shape, now returns `boolean`) | `clearSignalLossEpisode` — same file |
| Claim-survival test | `consumer.integration.test.ts` — "a claim already present on alert-state survives the transition unchanged" |

---

## Retention questions

1. Why was CP1's read-then-`MULTI`-write handoff genuinely safe when it was written, and what specific later decision made it unsafe?
2. Walk through the exact race by hand: which two operations interleave, and at which precise point does the claim get lost?
3. Why does wrapping the read in a second `MULTI` not fix the race, but a single Lua script does?
4. Why does the handoff script carry `composite_issued`/`composite_claim_candidate_id` forward unconditionally instead of validating them?

---

## Completion checklist

- [ ] I can explain why CP1's original handoff wasn't a bug when it was written
- [ ] I can trace the exact race by hand without looking at the trace above
- [ ] I can explain why `MULTI` cannot fix this but Lua can, in terms of what "atomic" actually covers in each case
- [ ] I can explain why this checkpoint deliberately does not interpret the claim fields it carries forward
- [ ] I ran the real-Redis test suite and the separate manual inspection myself and can interpret both
