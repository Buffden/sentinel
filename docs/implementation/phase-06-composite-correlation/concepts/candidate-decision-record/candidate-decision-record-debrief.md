# Candidate Decision Record Debrief

CP3C — commit hash filled in once committed.

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
     Tests  68 passed (68)
```

12 new tests, covering every scenario required:

| Test | Proves |
| --- | --- |
| missing record → read returns `null` | Baseline |
| fails closed on a malformed stored record instead of returning `null` | `readCandidateDecision` distinguishes "nothing decided" from "corrupted" |
| fails closed on a `COMPOSITE` record missing required fields | Same, for a specific realistic corruption shape |
| stores a first `UNSCHEDULED_PROXIMITY` decision | Baseline write |
| idempotent for the same `UNSCHEDULED_PROXIMITY` decision written twice | — |
| stores a first `COMPOSITE` decision with the frozen loss identity | Baseline write, full field set |
| idempotent for the same `COMPOSITE` decision written twice | — |
| `COMPOSITE` attempt after existing `UNSCHEDULED_PROXIMITY` conflicts — original stands | One direction of the decision-flip protection |
| `UNSCHEDULED_PROXIMITY` attempt after existing `COMPOSITE` conflicts — original stands | The other direction |
| two concurrent conflicting writes: exactly one becomes canonical | Real Redis Lua serialization, not application-level coordination |
| does not mutate `alert-state` or `recent-loss` | Scope boundary — this checkpoint touches only its own key |
| sets no TTL on the decision record | Replay lifetime is explicitly not this checkpoint's concern |

---

## Experiment 2: manual inspection against the real dev stack

Ran two genuinely concurrent, **conflicting** writes (one `COMPOSITE`, one `UNSCHEDULED_PROXIMITY`) for the same `candidate_id` against the real `sentinel-redis` container, via `tsx`, not a test double:

```ts
const results = await Promise.allSettled([
  writeCandidateDecisionIfAbsent(redis, composite),
  writeCandidateDecisionIfAbsent(redis, unscheduled),
]);
```

Observed:

```text
concurrent conflicting writes: [ 'fulfilled', 'rejected' ]
rejection is conflict error: true
canonical decision: {"decision":"COMPOSITE","candidate_id":"demo-pair-x:demo-pair-y:1700000050000",...}
replay of winning decision matches canonical: true
```

Real state re-inspected afterward:

```bash
$ docker exec sentinel-redis redis-cli HGETALL "alert-decision:demo-pair-x:demo-pair-y:1700000050000"
decision               COMPOSITE
candidate_id           demo-pair-x:demo-pair-y:1700000050000
selected_entity_id     demo-entity-a
dark_since_ms          1700000000000
loss_source            ACTIVE
signal_loss_alert_id   demo-entity-a:SIGNAL_LOSS:1700000000000
resumed_at_ms

$ docker exec sentinel-redis redis-cli PTTL "alert-decision:demo-pair-x:demo-pair-y:1700000050000"
-1
```

| Check | Expected | Observed |
| --- | --- | --- |
| Exactly one of two concurrent conflicting writes succeeds | yes | PASS — `fulfilled`/`rejected` |
| The rejection is specifically `CandidateDecisionConflictError` | yes | PASS |
| The canonical record matches whichever write actually won | yes | PASS |
| Replaying the winning decision again is idempotent-safe | yes | PASS |
| `PTTL` is `-1` (exists, no expiry) | yes | PASS |

---

## Engineering debrief

**Data flow:** `writeCandidateDecisionIfAbsent` runs one `redis.eval` against `alert-decision:{candidate_id}` that checks existence and either creates the record or compares it against the request, all inside one script. `readCandidateDecision` is a plain `HGETALL` plus a validating reconstruction that throws on anything malformed.

**Trade-off:** the record freezes only the loss-side identity CP2/CP3B already determined, not a constructed alert payload — CP4 owns building the actual deterministic `COMPOSITE`. This keeps CP3C's responsibility narrow at the cost of CP4 needing to do real work from this frozen identity rather than just republishing a stored payload; that's the intended boundary, not an oversight.

**Failure behaviour:** a conflict is a thrown `CandidateDecisionConflictError`, carrying both the requested and existing decision, rather than a return value a caller might accidentally ignore. A malformed stored record fails the same way on read — both are treated as invariant violations that should halt processing of that candidate, not routine outcomes.

## Manual inspection commands

```bash
docker exec sentinel-redis redis-cli HGETALL "alert-decision:<pair_key>:<episode_start_ms>"
docker exec sentinel-redis redis-cli PTTL "alert-decision:<pair_key>:<episode_start_ms>"
```

## Knowledge-check questions

1. Trace what happens if two Alert Evaluator instances (briefly overlapping during a leader failover, per Pre-CP2A's own acknowledged bounded overlap) both try to write a decision for the same `candidate_id` at the same instant.
2. Why does `readCandidateDecision` need to validate a `COMPOSITE` record's required fields, rather than trusting whatever is in Redis?
3. What exactly is compared to decide "idempotent" versus "conflict," and why isn't `signal_loss_alert_id` part of that comparison?

## Optional manual tweak

Manually corrupt a real decision record via `redis-cli` (e.g. `HSET alert-decision:... decision GARBAGE`) and confirm both `readCandidateDecision` and a subsequent `writeCandidateDecisionIfAbsent` for that same `candidate_id` fail loudly rather than silently proceeding.

## Next

CP4: deterministic, pure `COMPOSITE` alert construction from a frozen `CompositeCandidateDecision` — no Redis mutation. Then CP5 wires CP2 → CP3B → CP3C → CP4 → Kafka publish into `handleProximityCandidate` for the first time, and the API's atomic supersession.

---

## Key observations

| Concept | Observed |
| --- | --- |
| Automated suite | 68/68 PASS, 12 new tests |
| Real concurrent conflicting write | Exactly one winner, real Redis Lua serialization |
| Real malformed-record handling | Both read and write fail closed, never silently re-decide |
| TTL discipline | `PTTL` is `-1` — no expiry, replay lifetime deferred to CP5 |
