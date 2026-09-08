# Composite Episode Claim Debrief

CP3B — commit hash filled in once committed.

---

## Setup

```bash
make up
cd services/alert-evaluator
```

---

## Experiment 1: automated suite against real Redis

```bash
node_modules/.bin/vitest run
```

```text
Test Files  3 passed (3)
     Tests  56 passed (56)
```

18 new tests, covering every scenario required:

| Test | Proves |
| --- | --- |
| claims an active episode with no prior claim | Baseline CLAIM on `alert-state` |
| claims a recent episode with no prior claim | Baseline CLAIM on `recent-loss` |
| the same candidate calling CLAIM twice succeeds both times | Retry is not fenced by the claimant's own prior claim |
| a different candidate is rejected once the episode is claimed | Fencing works |
| rejects a mismatched `dark_since_ms` and leaves state unchanged | Wrong episode fails; before/after `HGETALL` `toEqual` |
| rejects an episode that is already `composite_issued=1` | An upgraded episode cannot be re-claimed |
| finds the episode in `recent-loss` when CP3A handed it off before CLAIM ran | Representation independence, the checkpoint's core guarantee |
| two concurrent different-candidate CLAIM calls: exactly one succeeds | Real Redis Lua serialization, not just application logic |
| CLAIM does not touch `recent-loss` TTL | `PTTL` captured before/after, asserted non-increasing |
| FINALIZE fails when there is no prior claim | — |
| FINALIZE fails for a candidate that does not hold the claim | Ownership, not just episode identity, gates FINALIZE |
| FINALIZE succeeds for the candidate that holds the claim | Baseline |
| FINALIZE is idempotent: the same candidate finalizing twice succeeds both times | The exact redelivery-safety property this checkpoint exists for |
| FINALIZE finds and finalizes the episode after it moved to `recent-loss` between CLAIM and FINALIZE | Representation independence across the two calls, not just within one |
| FINALIZE does not touch `recent-loss` TTL | Same non-increasing `PTTL` assertion |

---

## Experiment 2: manual inspection against the real dev stack

Seeded a real active episode, then ran the real primitives (not test doubles) via `tsx` against the live `sentinel-redis` container — two concurrent different-candidate claims, a same-candidate retry, a finalize attempt by the loser, and a finalize by the actual winner:

```bash
docker exec sentinel-redis redis-cli HSET alert-state:demo-cp3b \
  dark_since_ms 1700000000000 \
  signal_loss_alert_id demo-cp3b:SIGNAL_LOSS:1700000000000 \
  composite_issued 0
```

```ts
const [claimA, claimB] = await Promise.all([
  claimCompositeEpisode(redis, entityId, darkSinceMs, candidateA),
  claimCompositeEpisode(redis, entityId, darkSinceMs, candidateB),
]);
```

Observed:

```text
concurrent claims: { claimA: true, claimB: false }
candidate A retry: true
finalize by loser (must be false): false
finalize by winner: true
```

Real state re-inspected afterward:

```bash
$ docker exec sentinel-redis redis-cli HGETALL alert-state:demo-cp3b
dark_since_ms                 1700000000000
signal_loss_alert_id          demo-cp3b:SIGNAL_LOSS:1700000000000
composite_issued              1
composite_claim_candidate_id  demo-pair-x:demo-pair-y:1700000030000
```

| Check | Expected | Observed |
| --- | --- | --- |
| Exactly one of two concurrent different-candidate claims succeeds | yes | PASS — `claimA: true, claimB: false` |
| The winning candidate's own retry still succeeds | yes | PASS |
| The losing candidate cannot finalize | `false` | PASS |
| The winning candidate can finalize | `true` | PASS |
| Final `composite_claim_candidate_id` matches the real winner (candidate A) | yes | PASS |
| Final `composite_issued` | `1` | PASS |

---

## Engineering debrief

**Data flow:** `claimCompositeEpisode`/`finalizeCompositeEpisode` each run a single `redis.eval` against `KEYS = [alert-state:{entity_id}, recent-loss:{entity_id}]`. The Lua script's `find_match` checks each key's `dark_since_ms` against the expected value, uses whichever one matches, and applies the CLAIM or FINALIZE gate logic entirely inside that one script execution.

**Trade-off:** both primitives take raw `entity_id`/`expected_dark_since_ms`/`candidate_id` rather than a `QualifyingLossEpisode` object from CP2, deliberately decoupling CP3B from CP2's specific return shape — a caller extracts the three primitives it needs from whatever CP2 (or, later, a replayed decision) gives it. This keeps CP3B usable independent of exactly how a future caller arrives at those three values.

**Failure behaviour:** every failure path (`NO_EPISODE`, `ALREADY_ISSUED`, `CLAIMED_BY_OTHER`, `NOT_CLAIMED`) leaves both Redis hashes completely unchanged — proven directly by the "leaves state unchanged" test's before/after `HGETALL` equality check, not merely assumed from reading the Lua. Concurrent-caller exclusivity is proven against the real server, not asserted from Lua's documented atomicity — two genuinely concurrent `eval` calls from the same test process, real Redis serializing them.

## Manual inspection commands

```bash
docker exec sentinel-redis redis-cli HGETALL alert-state:<entity_id>
docker exec sentinel-redis redis-cli HGETALL recent-loss:<entity_id>
docker exec sentinel-redis redis-cli PTTL recent-loss:<entity_id>
```

## Knowledge-check questions

1. Why do CLAIM and FINALIZE search both `alert-state` and `recent-loss` inside a single script call rather than checking one, then the other, as two separate round-trips?
2. Explain the exact crash scenario that makes FINALIZE's idempotency necessary — what would go wrong on redelivery without it?
3. Why is "exactly one of two concurrent claims succeeds" a real Redis Lua guarantee here, not something the TypeScript wrapper has to coordinate?

## Optional manual tweak

Re-run the manual inspection but call `finalizeCompositeEpisode` with a `candidate_id` that has the *same pair_key* as the winner but a different `episode_start_ms` (a genuinely different candidate for the same pair) — confirm it's rejected exactly like an unrelated candidate would be, proving the full `candidate_id`, not just `pair_key`, is what's checked.

## Next

CP3C: implement `alert-decision:{pair_key}:{episode_start_ms}` read/write — sticky candidate-level replay classification, independent of and complementary to the loss-episode exclusivity CP3B provides. Not wired into Kafka handling; no deletion logic yet (that depends on the input-offset lifecycle CP5 wires up).

---

## Key observations

| Concept | Observed |
| --- | --- |
| Automated suite | 56/56 PASS, 18 new tests |
| Real concurrent claim | Exactly one winner, real Redis Lua serialization |
| Real cross-primitive ownership check | Loser cannot finalize; winner can |
| TTL discipline | Neither CLAIM nor FINALIZE increases `recent-loss`'s `PTTL` |
