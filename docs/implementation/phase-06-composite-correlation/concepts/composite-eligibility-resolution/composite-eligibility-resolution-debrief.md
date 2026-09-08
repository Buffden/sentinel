# Composite Eligibility Resolution Debrief

Commit: `1efa70c` — "feat(alert-evaluator): resolve composite eligibility"

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
     Tests  41 passed (41)
```

17 of those are new, covering every behavior review required:

| Test | Proves |
| --- | --- |
| `composite_issued='0'` + in-window → qualifies | Baseline active-dark case |
| `composite_issued='1'` → rejects even with a valid gap | An already-upgraded episode cannot be reused |
| Negative gap → rejects | Candidate predating the loss is not eligible |
| Gap exactly at `windowMs` → qualifies | Boundary is inclusive |
| Gap `windowMs + 1` → rejects | Boundary is exact, not approximate |
| Missing `signal_loss_alert_id` (active or recent) → rejects | Required-field validation, added after review |
| Missing/malformed `resumed_at_ms` (recent-loss) → rejects | Same, for the recent-loss-specific field |
| **Live `recent-loss` key, real `PTTL > 0`, but gap exceeds window → rejects** | The regression this checkpoint exists to prevent — key existence is never sufficient |
| Both qualify → smaller gap wins | Tie-break, non-tie case |
| Exact tie → lexicographically smaller `entity_id` wins | Tie-break, tie case |
| Before/after `HGETALL` snapshots of both entities are `toEqual` | Read-only, including the *losing* episode |
| `selectWinningEpisode` pure cases (no Redis) | Tie-break logic isolated from Redis entirely |

---

## Experiment 2: manual inspection against the real dev stack

Seeded two real episodes directly, then invoked the real `resolveCompositeEligibility` (not a test double) via `tsx`:

```bash
docker exec sentinel-redis redis-cli HSET alert-state:demo-cp2-a \
  dark_since_ms 1700000000000 signal_loss_alert_id demo-cp2-a:SIGNAL_LOSS:1700000000000 composite_issued 0
docker exec sentinel-redis redis-cli HSET recent-loss:demo-cp2-b \
  dark_since_ms 1700000030000 resumed_at_ms 1700000040000 signal_loss_alert_id demo-cp2-b:SIGNAL_LOSS:1700000030000
```

```bash
cat > src/cp2-manual-check.mts << 'EOF'
import { resolveCompositeEligibility } from './composite.js';
import { redis } from './evaluator.js';
const result = await resolveCompositeEligibility(redis, 'demo-cp2-a', 'demo-cp2-b', 1_700_000_050_000, 60_000);
console.log(JSON.stringify(result, null, 2));
await redis.quit();
EOF
npx tsx src/cp2-manual-check.mts
```

Observed output:

```json
{
  "entity_id": "demo-cp2-b",
  "source": "RECENT",
  "dark_since_ms": 1700000030000,
  "signal_loss_alert_id": "demo-cp2-b:SIGNAL_LOSS:1700000030000",
  "resumed_at_ms": 1700000040000,
  "gap_ms": 20000
}
```

`demo-cp2-a`'s gap was 50000ms; `demo-cp2-b`'s was 20000ms — the smaller gap correctly won.

Real state re-inspected afterward:

```bash
$ docker exec sentinel-redis redis-cli HGETALL alert-state:demo-cp2-a
dark_since_ms         1700000000000
signal_loss_alert_id  demo-cp2-a:SIGNAL_LOSS:1700000000000
composite_issued      0
$ docker exec sentinel-redis redis-cli HGETALL recent-loss:demo-cp2-b
dark_since_ms         1700000030000
resumed_at_ms         1700000040000
signal_loss_alert_id  demo-cp2-b:SIGNAL_LOSS:1700000030000
$ docker exec sentinel-redis redis-cli KEYS '*'
recent-loss:demo-cp2-b
alert-state:demo-cp2-a
```

| Check | Expected | Observed |
| --- | --- | --- |
| Correct winner selected (smaller gap) | `demo-cp2-b` | PASS |
| Source correctly identified | `RECENT` | PASS |
| `resumed_at_ms` carried as evidence | `1700000040000` | PASS |
| Both hashes byte-for-byte unchanged after resolution | identical to seeded state | PASS |
| No new Redis keys created | only the two seeded keys present | PASS |

---

## Engineering debrief

**Data flow:** `resolveCompositeEligibility(redis, entityAId, entityBId, episodeStartMs, windowMs)` resolves both pair members in parallel via `resolveEntityLossEpisode`, then applies the pure `selectWinningEpisode` tie-break. Zero writes anywhere in the call graph.

**Trade-off:** treating `signal_loss_alert_id` (and, for recent-loss, `resumed_at_ms`) as required rather than optional-with-a-default means a genuinely malformed Redis hash produces a clean `null` here instead of a partially-populated result that fails confusingly in a later checkpoint. This was a direct review finding, not the original design.

**Failure behaviour:** the specific regression this checkpoint's test suite is built to catch is a live `recent-loss` key (`PTTL > 0`, real TTL, not expired) still correctly rejected because its source-time gap from `dark_since_ms` exceeds the window — proving key existence was never treated as sufficient, exactly per the Pre-CP2B rule.

## Manual inspection commands

```bash
cd services/alert-evaluator
node_modules/.bin/vitest run

# Direct real-Redis check (adjust entity IDs/timestamps as needed)
docker exec sentinel-redis redis-cli HGETALL alert-state:<entity_id>
docker exec sentinel-redis redis-cli HGETALL recent-loss:<entity_id>
```

## Knowledge-check questions

1. Walk through why checking `alert-state` before `recent-loss` (rather than both, always) is safe, and name the exact commit that establishes the invariant this relies on.
2. Why does a missing `signal_loss_alert_id` reject the episode here instead of defaulting to `''`?
3. Describe the specific Redis state that produces the "live TTL, but rejected" regression-guard test, and explain in your own words why it doesn't qualify.
4. What does the before/after `HGETALL` snapshot test actually prove that the "no mutation" claim in the code comments does not, by itself?

## Optional manual tweak

Re-run the manual inspection with `demo-cp2-a` and `demo-cp2-b` swapped in which one has the smaller gap, and confirm the winner flips accordingly — makes the tie-break's `gap_ms`-first, `entity_id`-second ordering concrete rather than assumed.

## Next

Pre-CP3A: the crash-safe claim/publish protocol needs to be resolved and documented — a naive "claim then publish" design can permanently lose a composite on crash, and a naive "publish then delete `recent-loss`" design can cause a redelivered candidate to wrongly emit a duplicate `UNSCHEDULED_PROXIMITY` alongside an already-published `COMPOSITE`. Neither is implemented yet.

---

## Key observations

| Concept | Observed |
| --- | --- |
| Automated suite | 41/41 PASS, 17 new tests including the live-TTL-but-rejected regression guard |
| Manual resolution | Correct winner (`demo-cp2-b`, gap 20000 < 50000), correct source (`RECENT`) |
| Read-only proof | Both hashes byte-for-byte identical before/after, no new keys |
