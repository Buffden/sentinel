# Composite Claim + Decision Protocol Debrief

Pre-CP3A — design/documentation checkpoint. No application code changed; there is no test suite to run. The evidence here is tracing each crash scenario by hand against the resolved protocol, and confirming the documentation changes are complete and consistent.

---

## Experiment 1: trace the claim-identity bug the original proposal missed

Original (rejected) design: `composite_claim = pair_key`.

```text
pair A:B, proximity episode #1 at episode_start_ms=1000 -> resolves eligible, claims A:B
pair A:B, proximity episode #2 at episode_start_ms=5000 -> a genuinely later, different encounter

Under the rejected design:
  episode #2's claim attempt sees composite_claim == "A:B" (already set by episode #1)
  -> indistinguishable from episode #1 being redelivered
  -> episode #2 cannot be told apart from a retry of episode #1
```

Under the resolved design, `candidate_id = {pair_key}:{episode_start_ms}`:

```text
episode #1's candidate_id = "A:B:1000"
episode #2's candidate_id = "A:B:5000"
-> distinct claim identities, correctly told apart
```

| Check | Expected | Observed |
| --- | --- | --- |
| A later, genuinely different proximity episode for the same pair is distinguishable from a redelivery | yes | PASS — by construction, once claims are keyed by full `candidate_id` |

---

## Experiment 2: trace the CP1-handoff race by hand

| Time | Actor | Operation | `alert-state:X` / `recent-loss:X` state |
| --- | --- | --- | --- |
| t0 | Position Consumer | `HGETALL alert-state:X` | reads `{ dark_since_ms: D, composite_claim_candidate_id: "" }` |
| t1 | Alert Evaluator | `CLAIM` (Lua) on `alert-state:X`, candidate `Y` | `composite_claim_candidate_id` was `""` → claim succeeds → `alert-state:X.composite_claim_candidate_id = "Y"` |
| t2 | Position Consumer | `MULTI(HSET recent-loss:X from the t0 snapshot, DEL alert-state:X) EXEC` | `recent-loss:X.composite_claim_candidate_id` written as `""` — the **stale** t0 value |
| after | — | — | candidate `Y`'s claim, written at t1, is gone — unrecoverable |

This confirms the review finding directly: a read-then-write handoff loses a claim that lands in the gap between t0 and t2. The resolved design (a single Lua script performing the read-and-transfer atomically) closes this by construction — there is no t0/t2 gap for a t1 to land in, because Redis serializes the whole transfer as one operation relative to the CLAIM script. See [`atomic-signal-loss-handoff`](../atomic-signal-loss-handoff/atomic-signal-loss-handoff.md) for the implemented fix (CP3A) and its own sequence diagram of this exact race.

| Check | Expected | Observed |
| --- | --- | --- |
| Read-then-write handoff can silently drop a concurrent claim | yes (this is the bug) | Confirmed by hand-trace |
| A single atomic Lua transfer closes the gap | yes | Confirmed by construction — not yet implemented, tracked as an open item for CP3 |

---

## Experiment 3: trace both directions of the decision-flip bug

**Direction 1 — COMPOSITE already issued, redelivery must not downgrade it:**

```text
attempt 1: CP2 resolves eligible -> CLAIM succeeds -> COMPOSITE published -> crash before offset commit
attempt 2 (redelivery): alert-decision:{candidate_id} still exists (never deleted -- offset never committed)
  -> replay the recorded COMPOSITE decision, republish with the SAME alert_id
  -> no UNSCHEDULED_PROXIMITY emitted
```

**Direction 2 — nothing qualified yet, redelivery must not upgrade it:**

```text
attempt 1: CP2 resolves nothing eligible -> decision = UNSCHEDULED_PROXIMITY -> published -> crash before offset commit
           (signal-loss scan runs in between, opening a fresh alert-state for one pair member)
attempt 2 (redelivery): alert-decision:{candidate_id} still exists from attempt 1
  -> replay the recorded UNSCHEDULED_PROXIMITY decision, republish with the SAME alert_id
  -> no COMPOSITE emitted, despite alert-state now existing
```

| Check | Expected | Observed |
| --- | --- | --- |
| Redelivery after a COMPOSITE decision never downgrades to UNSCHEDULED_PROXIMITY | yes | Confirmed by hand-trace against the decision-record-first flow |
| Redelivery after an UNSCHEDULED_PROXIMITY decision never upgrades to COMPOSITE | yes | Confirmed by hand-trace — this is exactly the direction the loss-episode claim alone does NOT cover |

---

## Experiment 4: confirm the documentation changes are complete

```bash
grep -n "composite_claim_candidate_id\|alert-decision" docs/DATA_MODEL.md
grep -n "consumes/deletes\|A qualifying composite consumes" docs/DATA_MODEL.md docs/use-cases/US-06-composite-alert/composite-alert.md
```

| Check | Expected | Observed |
| --- | --- | --- |
| `alert-state`/`recent-loss` field lists include the new claim fields | yes | PASS |
| No remaining doc text claims `recent-loss` is deleted on composite consumption | yes | PASS — both `DATA_MODEL.md` and `US-06` corrected |
| `US-06` cross-links to the new protocol section | yes | PASS |
| Phase 06 README checkpoint table reflects Pre-CP3A as resolved (design), CP3 as still Pending (implementation) | yes | PASS |

---

## Engineering debrief

**Data flow:** none yet — this checkpoint constrains what CP3 is allowed to build, the same role Pre-CP2B played for CP2.

**Trade-off:** this protocol is materially more state than the original CP1-era sketch (one flag, `composite_issued`) — now two coordination fields on the loss hashes plus an entirely separate decision-record key. That's a deliberate cost: the alternative (a single flag, deletion-on-consumption) was traced by hand to two distinct correctness bugs, not hypothetical ones.

**Failure behaviour:** every crash point in the full CLAIM → decision → publish → FINALIZE → offset-commit → decision-delete sequence was traced above. The only failure mode without a designed guarantee is an orphaned decision record (crash between offset commit and delete) — explicitly accepted as a cleanup leak, not a correctness failure, since nothing consults an orphaned record again.

## Manual inspection commands

Not applicable — no runtime component exists yet. Re-run the hand-traces above against the actual CP3 implementation once it exists, as that checkpoint's own debrief.

## Knowledge-check questions

1. Reconstruct the claim-identity bug from Experiment 1 without looking at the trace — why does `pair_key` alone conflate two different encounters?
2. Reconstruct the CP1-handoff race from Experiment 2 — which two operations interleave, and why does a single Lua script close the gap?
3. Explain both directions of the decision-flip bug in your own words, and why the loss-episode claim alone only protects one of them.

## Optional manual tweak

None — this is a design checkpoint. The equivalent exercise once CP3 exists: deliberately kill the Alert Evaluator process at each of the five crash points named in the flow diagram and confirm real Redis/Kafka state matches what this document predicts.

## Next

CP3: implement the CLAIM/FINALIZE Lua scripts and the decision-record read/write in `composite.ts`, and revise `clearSignalLossEpisode` in `consumer.ts` into the single atomic Lua transfer this protocol requires. Still not wired into `handleProximityCandidate` — that remains CP5's scope.

---

## Key observations

| Concept | Observed |
| --- | --- |
| Claim-identity bug | Confirmed by hand-trace; resolved by using `{pair_key}:{episode_start_ms}` |
| CP1-handoff race | Confirmed by hand-trace; resolved by design (single atomic Lua transfer) — not yet implemented |
| Decision-flip bug, both directions | Both confirmed and closed by the decision-record-first flow |
| Documentation completeness | `DATA_MODEL.md`, `US-06`, phase README all consistent; no code touched |
