# Proximity Episode State Debrief

---

## Experiment: `touchProximityEpisode` against real Redis

Five integration tests, including a real (not simulated) TTL expiry:

```text
Test Files  5 passed (5)
     Tests  26 passed (26)
```

(21 from earlier checkpoints, 5 new.)

| Test | Proves |
| --- | --- |
| starts a new episode on the first confirmation for a pair | Baseline creation, `episode_start_ms` equals the confirmation's own timestamp |
| sets the gap TTL on a new episode | `PEXPIRE` actually applied, `PTTL` is positive and within bound |
| reports an existing episode and keeps its original start time | A later confirmation doesn't reset `episode_start_ms`; `last_seen_ms` advances |
| renews the TTL without moving `last_seen_ms` backward for an out-of-order confirmation | The monotonic guard and the always-renew-TTL behavior both hold at once |
| starts a fresh episode once the previous one has expired | Real 50ms TTL, real 150ms wait, real expiry — the next confirmation gets a brand-new `episode_start_ms`, not the old one |

Cleanup verified: `KEYS proximity-episode:test-*` returned nothing after the suite ran.

---

## Engineering debrief

**Data flow:** given a pair key and a confirmation's source timestamp, one Lua script checks whether `proximity-episode:{pair_key}` exists. If not, it creates the hash with `episode_start_ms = last_seen_ms = observedAtMs` and applies the gap TTL, reporting a new episode. If it exists, it advances `last_seen_ms` only if the new timestamp isn't older, renews the TTL regardless, and reports the existing `episode_start_ms`.

**Trade-off:** correctness under concurrent instances (one atomic Lua round trip) over the simplicity of two plain Redis calls — justified because, unlike the Alert Evaluator, nothing here enforces single-instance ownership of a given pair.

**Failure behaviour:** the two things this guards against are a race between concurrent instances both starting an episode for the same pair (closed by the Lua script's atomicity) and `last_seen_ms` regressing on an out-of-order confirmation (closed by the monotonic check) — while still treating that same out-of-order confirmation as evidence the encounter is alive for TTL purposes.

## Manual inspection commands

```bash
# Inspect an episode's current state and remaining TTL
docker exec sentinel-redis redis-cli HGETALL "proximity-episode:<pair_key>"
docker exec sentinel-redis redis-cli PTTL "proximity-episode:<pair_key>"

# Run the episode-state integration suite
cd services/correlation-worker && node_modules/.bin/vitest run src/episode.integration.test.ts
```

## Knowledge-check questions

1. Why can't "check whether an episode exists" and "create or refresh it" be two separate Redis calls here?
2. What happens to the TTL when a confirmation arrives with an older timestamp than what's already stored, and why?
3. What determines when a proximity episode is considered over?

## Next

Wire `findProximityCandidates` → `filterByDistance` → `touchProximityEpisode` → `mergeProximityEvent` together into the actual `position.normalized` consumer loop, and publish one `proximity.candidates` event per new, unscheduled episode.

---

## Key observations

| Concept | Observed |
| --- | --- |
| New episode | `episode_start_ms` equals the triggering confirmation's own timestamp |
| Existing episode | `episode_start_ms` unchanged, `last_seen_ms` advances |
| Out-of-order confirmation | `last_seen_ms` unchanged, TTL still renewed |
| Real TTL expiry (50ms gap, 150ms wait) | Next confirmation starts a genuinely new episode |
| Test cleanup | PASS — no leftover test keys |
