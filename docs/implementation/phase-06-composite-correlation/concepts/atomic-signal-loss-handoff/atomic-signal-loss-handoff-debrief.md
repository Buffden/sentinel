# Atomic Signal-Loss Handoff Debrief

CP3A — commit hash filled in once committed.

---

## Setup

```bash
make up
cd services/position-consumer
```

---

## Experiment 1: automated suite against real Redis

```bash
node_modules/.bin/vitest run
```

```text
Test Files  2 passed (2)
     Tests  37 passed (37)
```

One new test, the specific regression this checkpoint exists to prevent:

| Test | Proves |
| --- | --- |
| a claim already present on alert-state survives the transition unchanged | `composite_claim_candidate_id` and `composite_issued`, seeded on `alert-state` before the handoff runs, land on `recent-loss` byte-for-byte identical |

The three pre-existing tests (no-op, TTL-bounded conversion, real expiry) still pass unchanged — the observable contract for the *unclaimed* case is identical to CP1's; only the mechanism (Lua vs. `MULTI`) and the field set (five fields vs. three) changed. The TTL-bounded-conversion test's exact-equality assertion was updated to include `composite_issued: '0'` and `composite_claim_candidate_id: ''`, since the Lua script now unconditionally carries those fields forward.

---

## Experiment 2: manual inspection against the real dev stack

Seeded a real `alert-state` with a simulated in-flight claim, then invoked the real `clearSignalLossEpisode` (not a test double) via `tsx`:

```bash
docker exec sentinel-redis redis-cli HSET alert-state:demo-cp3a \
  dark_since_ms 1700000000000 \
  signal_loss_alert_id demo-cp3a:SIGNAL_LOSS:1700000000000 \
  composite_issued 0 \
  composite_claim_candidate_id "demo-pair-x:demo-pair-y:1700000030000"
```

```bash
cat > src/cp3a-manual-check.mts << 'EOF'
import { clearSignalLossEpisode, redis } from './consumer.js';
const applied = await clearSignalLossEpisode('demo-cp3a', 1_700_000_010_000);
console.log('applied:', applied);
await redis.quit();
EOF
npx tsx src/cp3a-manual-check.mts
```

Observed: `applied: true`, and the log line itself already showed the claim carried through. Real state re-inspected afterward:

```bash
$ docker exec sentinel-redis redis-cli EXISTS alert-state:demo-cp3a
0
$ docker exec sentinel-redis redis-cli HGETALL recent-loss:demo-cp3a
dark_since_ms                 1700000000000
resumed_at_ms                 1700000010000
signal_loss_alert_id          demo-cp3a:SIGNAL_LOSS:1700000000000
composite_issued              0
composite_claim_candidate_id  demo-pair-x:demo-pair-y:1700000030000
$ docker exec sentinel-redis redis-cli PTTL recent-loss:demo-cp3a
116529
```

| Check | Expected | Observed |
| --- | --- | --- |
| `alert-state` gone after the transfer | `EXISTS` = 0 | PASS |
| Seeded claim (`composite_claim_candidate_id`) survives byte-for-byte | exact match | PASS |
| `composite_issued` survives unchanged | `0` | PASS |
| TTL attached in the same atomic step | positive, ≤ configured window | PASS — 116529ms |

---

## Engineering debrief

**Data flow:** an accepted resume position calls `clearSignalLossEpisode(entityId, resumedAtMs)` → one `redis.eval(SIGNAL_LOSS_HANDOFF_LUA, ...)` reads current `alert-state` fields, writes all five into `recent-loss`, attaches the TTL, and deletes `alert-state` — all inside one Redis-side script execution.

**Trade-off:** the handoff script does not validate or interpret `composite_issued`/`composite_claim_candidate_id` — it moves them unconditionally. This keeps CP3A's responsibility narrow (coordination-safety for the transfer) and leaves claim semantics entirely to CP3B, at the cost of the handoff being "dumb" about whether what it's carrying forward is meaningful. That's the correct boundary: a handoff that also validated claims would blur two checkpoints together.

**Failure behaviour:** the crash boundary is unchanged from CP1 — binary, not partial. What's new is that the *thing* that can no longer be silently lost includes a live claim, not just episode evidence. Redis's Lua execution model (one script = one uninterruptible unit) is what makes this true regardless of how many other clients are reading or writing the same keys concurrently.

## Manual inspection commands

```bash
docker exec sentinel-redis redis-cli HGETALL alert-state:<entity_id>
docker exec sentinel-redis redis-cli HGETALL recent-loss:<entity_id>
docker exec sentinel-redis redis-cli PTTL recent-loss:<entity_id>
```

## Knowledge-check questions

1. Why did CP1's original read-then-`MULTI`-write handoff not need to be atomic across the read and the write, but CP3A's does?
2. What in Redis's execution model makes a Lua script's `HGET` + `HSET` pair atomic in a way that two separate client round-trips, even inside a `MULTI`, are not?
3. Why does the handoff script carry the claim fields forward without interpreting them, rather than also enforcing claim rules?

## Optional manual tweak

Re-run the manual inspection but seed `composite_issued=1` instead of `0` (simulating an already-finalized composite) and confirm it, too, survives the handoff unchanged — the script doesn't distinguish "claimed" from "finalized," by design.

## Next

CP3B: implement the CLAIM/FINALIZE Lua primitives, representation-independent across `alert-state`/`recent-loss` — since CP3A now guarantees exactly one of those two hashes represents a given episode at any observable instant, CLAIM/FINALIZE can locate "whichever one currently holds it" without caring which. Still no Kafka emission.

---

## Key observations

| Concept | Observed |
| --- | --- |
| Automated suite | 37/37 PASS, 1 new regression-guard test |
| Manual claim-survival check | `composite_claim_candidate_id` and `composite_issued` byte-for-byte identical after transfer |
| Crash boundary | Unchanged from CP1 — still binary, now covers claim fields too |
