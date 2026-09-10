# Composite Candidate Wiring Debrief

CP5A, commit hash filled in once committed.

---

## Setup

```bash
make up
make topics
cd services/alert-evaluator
```

---

## Experiment 1: automated suite against real Redis and Kafka

```bash
npx tsc --noEmit
npx vitest run
```

```text
Test Files  4 passed (4)
     Tests  87 passed (87)
```

17 new tests in `evaluator.integration.test.ts` (real Redpanda `alerts` topic, real Redis), plus 8 new tests in `composite.integration.test.ts` for the new `releaseCompositeClaim` primitive and FINALIZE's reason-aware return type, on top of the existing suites (all still passing, none touched in behavior). Full run repeated 3 times consecutively with no flakiness; the real-concurrency test (below) repeated 5 times in isolation, also no flakiness.

| Test | Proves |
| --- | --- |
| a fresh candidate with a qualifying `RECENT` loss on entity_b becomes `COMPOSITE`, entity_type sourced from the selected entity | CP2 → CLAIM → decision → CP4 → FINALIZE wired end to end; `entity_type` reads from `entity:live:{selected_entity_id}`, not always `entity_a_id` |
| replaying an existing `COMPOSITE` decision republishes and finalizes again (idempotent) | Branch 1 (existing decision) calls FINALIZE too, not just build+publish; a second FINALIZE on an already-issued episode is a safe no-op |
| CLAIM failure decides `UNSCHEDULED_PROXIMITY` with no fallback to the other pair member, even when it independently qualifies | Pre-CP5A(a): the non-selected episode is left completely untouched, never claimed by this candidate |
| a decision-write conflict after a successful CLAIM releases the stray claim and converges on one decision | Pre-CP5A(b), exercised via a real `Promise.all` race against a directly-written competing decision; correct regardless of which side actually wins |
| replaying a `COMPOSITE` decision whose claim was never actually held throws and never marks the episode issued | Pre-CP5A(c): `NOT_CLAIMED` is a thrown `CompositeFinalizeInvariantError`, distinct from `NO_EPISODE`'s warn-and-continue |
| `finalizeCompositeEpisode` returns `NO_EPISODE`/`NOT_CLAIMED`/`SUCCESS` (not boolean) | The three-way FINALIZE contract, including the representation-independent `NO_EPISODE` case after a key expires between CLAIM and FINALIZE |
| `releaseCompositeClaim` clears a live claim, finds it after a CP3A handoff, treats an already-expired episode as nothing to release, and refuses to touch a claim it doesn't own or one already finalized | All three preconditions from `DATA_MODEL.md`'s Pre-CP5A(b) RELEASE contract, proven against real Redis |

One caught-and-fixed bug during test construction: the first version of the "CLAIM failure, no fallback" test seeded entity_b with a *smaller* `gap_ms` than entity_a while pre-claiming entity_a under a different candidate. CP2's own deterministic tie-break correctly picked entity_b instead (smaller gap wins), so the pre-claimed entity was never the one CP2 selected, and the test hung waiting for an alert that never arrived. Fixed by swapping the `dark_since_ms` values so entity_a is genuinely the tie-break winner the pre-claim needs to land on. Left as a note here because it is itself a small piece of evidence that the tie-break logic runs for real, not just in the assertions.

---

## Experiment 2: manual inspection against the real dev stack

Ran `handleProximityCandidate` directly via `tsx` against real Redis and a real Kafka producer connection, not a test double:

```ts
await redis.hset(`alert-state:${entity_a_id}`, {
  dark_since_ms: String(darkSinceMs),
  signal_loss_alert_id: `${entity_a_id}:SIGNAL_LOSS:${darkSinceMs}`,
  composite_issued: '0',
});
await redis.hset(`entity:live:${entity_a_id}`, { entity_type: 'aircraft' });

await handleProximityCandidate(candidate);
```

Observed:

```text
before: alert-state -> {
  signal_loss_alert_id: 'demo-a-...:SIGNAL_LOSS:1789009958382',
  dark_since_ms: '1789009958382',
  composite_issued: '0'
}
{ ... } composite alert emitted
after CLAIM+FINALIZE: alert-state -> {
  composite_claim_candidate_id: 'demo-a-...:demo-b-...:1789009963382',
  signal_loss_alert_id: 'demo-a-...:SIGNAL_LOSS:1789009958382',
  dark_since_ms: '1789009958382',
  composite_issued: '1'
}
decision record -> {
  selected_entity_id: 'demo-a-...',
  candidate_id: 'demo-a-...:demo-b-...:1789009963382',
  loss_source: 'ACTIVE',
  decision: 'COMPOSITE',
  signal_loss_alert_id: 'demo-a-...:SIGNAL_LOSS:1789009958382',
  dark_since_ms: '1789009958382',
  resumed_at_ms: ''
}
{ ... } composite alert emitted
after replay: alert-state (still composite_issued=1, unchanged) -> {
  composite_issued: '1',
  dark_since_ms: '1789009958382',
  signal_loss_alert_id: 'demo-a-...:SIGNAL_LOSS:1789009958382',
  composite_claim_candidate_id: 'demo-a-...:demo-b-...:1789009963382'
}
```

| Check | Expected | Observed |
| --- | --- | --- |
| Fresh candidate with a qualifying loss claims and issues the episode | yes | PASS |
| A real `COMPOSITE` alert is published (visible via the service's own structured log) | yes | PASS |
| Decision record persists after publish, no deletion attempted | yes | PASS |
| Redelivery (calling the handler again for the same candidate) republishes without re-mutating `alert-state` | yes | PASS |

---

## Engineering debrief

**Data flow:** `handleProximityCandidate` checks `readCandidateDecision` first, before CP2 ever runs. A fresh candidate flows through CP2 → CLAIM (or not) → `writeCandidateDecisionIfAbsent` → `publishDecision`, which dispatches to `publishUnscheduledProximityAlert` or `publishCompositeAlert` by decision type. All three entry points that end in a `COMPOSITE` decision (existing-decision replay, a fresh successful CLAIM, an adopted post-conflict decision) converge on the same `publishCompositeAlert`, which always attempts FINALIZE, never just build-and-publish.

**Trade-off:** decision records are retained indefinitely for this checkpoint, no TTL, no deletion. This trades unbounded `alert-decision:*` growth in Redis for correctness under the leader-overlap ADR-005 already accepts: `DATA_MODEL.md`'s Pre-CP5A(d) shows a TTL alone doesn't help if the record is still explicitly deleted in the common case, so the safer choice for this checkpoint was to not delete at all, and defer reclamation until `proximity.candidates` has an explicit retention contract to size a TTL against.

**Failure behaviour:** a CLAIM failure (any of three underlying Lua reasons) always resolves `UNSCHEDULED_PROXIMITY`, never a retry. A decision-write conflict after a successful CLAIM releases the stray claim (best-effort; its own outcome doesn't gate recovery) and adopts the canonical decision. FINALIZE's `NOT_CLAIMED` throws and blocks the caller from ever reaching `commitOffsets()`, since `startCandidateConsumerSession`'s `eachMessage` calls it only after `handleProximityCandidate` resolves; `NO_EPISODE` warns and lets processing complete, since the key being gone means nothing else can reuse or corrupt it either.

## Manual inspection commands

```bash
cd services/alert-evaluator
npx vitest run src/evaluator.integration.test.ts
npx vitest run src/composite.integration.test.ts
```

```bash
docker exec sentinel-redis redis-cli HGETALL "alert-state:<entity_id>"
docker exec sentinel-redis redis-cli HGETALL "alert-decision:<pair_key>:<episode_start_ms>"
docker exec sentinel-redpanda rpk topic consume alerts -n 1
```

## Knowledge-check questions

1. Why do all three paths that end in a `COMPOSITE` decision (replay, fresh, adopted) call the same `publishCompositeAlert`, rather than three separate build/publish/finalize implementations?
2. Trace what happens if `handleProximityCandidate`'s own CLAIM succeeds on entity A, but the decision write then conflicts with an already-canonical `COMPOSITE/entity-B` decision for the same `candidate_id`.
3. Why does `NOT_CLAIMED` have to prevent the offset commit, while `NO_EPISODE` does not?
4. Why doesn't this checkpoint add `deleteCandidateDecision`, even though CP3C's original design assumed a delete step would eventually exist?
5. What specifically would have to become true before decision records could safely be deleted or given a TTL?

## Optional manual tweak

Seed two entities with qualifying loss episodes at different `gap_ms` values (matching the "CLAIM failure" test's setup), pre-claim the tie-break winner under a different `candidate_id` via `claimCompositeEpisode`, and confirm by hand (via `redis-cli`) that the losing entity's episode is genuinely never touched, not merely that the alert type comes out right.

## Next

CP5B: the API's atomic `COMPOSITE` insert and convergent supersession of the alerts it references, resolving the out-of-order arrival case Pre-CP5B still has to design (a `COMPOSITE` reaching the API before the `SIGNAL_LOSS` it supersedes). Not started as part of this checkpoint.

---

## Key observations

| Concept | Observed |
| --- | --- |
| Automated suite | 87/87 PASS across 4 test files, 25 new tests total, repeated runs with no flakiness |
| Real output inspection | Fresh CLAIM+FINALIZE, persisted decision record, idempotent replay, all observed directly against real Redis and a real Kafka publish |
| Fail-closed FINALIZE | `NOT_CLAIMED` threw and was distinguished from `NO_EPISODE`'s warn-and-continue, proven against real Redis state, not simulated |
| No fallback on CLAIM failure | The non-selected pair member's episode was left completely untouched, verified directly in Redis, not just inferred from the published alert type |
| Decision-conflict recovery | Exercised via a genuine `Promise.all` race, correct regardless of which side won, verified 5 consecutive runs with no flakiness |
