# Recent-Loss Handoff Debrief

Commit: `7e67457` — "alerts: bound recent-loss correlation window with atomic TTL handoff"

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
     Tests  36 passed (36)
```

Three of those tests are new, exercising `clearSignalLossEpisode` directly against real Redis (not a `.multi()` unit test in isolation, per review feedback — the guarantee under test is "Sentinel actually attaches the TTL when converting `alert-state` to `recent-loss`," not "Redis behaves correctly"):

| Test | Proves |
| --- | --- |
| no-ops when there is no open signal-loss episode | Function is a safe no-op outside the handoff case |
| converts an open episode into a TTL-bounded `recent-loss` and clears `alert-state` | Fields correct, `PTTL` positive and `<=` the configured window, `alert-state` gone |
| `recent-loss` actually disappears once the window elapses | Real 50ms window + 150ms wait — proves genuine Redis expiry, not simulated |

---

## Experiment 2: manual inspection against the real dev stack

Seeded a real `alert-state` entry directly, then invoked the real `clearSignalLossEpisode` function (not a test double) via `tsx`, against the live `sentinel-redis` container:

```bash
docker exec sentinel-redis redis-cli HSET alert-state:demo-cp1b \
  dark_since_ms 1700000000000 \
  signal_loss_alert_id demo-cp1b:SIGNAL_LOSS:1700000000000 \
  composite_issued 0
```

```bash
cat > src/cp1-manual-check.mts << 'EOF'
import { clearSignalLossEpisode, redis } from './consumer.js';
await clearSignalLossEpisode('demo-cp1b', 1_700_000_010_000);
await redis.quit();
EOF
npx tsx src/cp1-manual-check.mts
```

Observed log:

```text
{"level":"info","message":"signal loss episode cleared","entity_id":"demo-cp1b",...}
```

Real state inspected afterward:

```bash
$ docker exec sentinel-redis redis-cli EXISTS alert-state:demo-cp1b
0
$ docker exec sentinel-redis redis-cli HGETALL recent-loss:demo-cp1b
dark_since_ms        1700000000000
resumed_at_ms        1700000010000
signal_loss_alert_id demo-cp1b:SIGNAL_LOSS:1700000000000
$ docker exec sentinel-redis redis-cli PTTL recent-loss:demo-cp1b
117137
```

| Check | Expected | Observed |
| --- | --- | --- |
| `alert-state` gone immediately after | `EXISTS` = 0 | PASS |
| `recent-loss` fields correct | `dark_since_ms`, `resumed_at_ms`, `signal_loss_alert_id` all present | PASS |
| `PTTL` positive and `<=` configured `COMPOSITE_CORRELATION_WINDOW_MS` (120000ms default) | yes | PASS — 117137ms |
| All three commands applied atomically (`HSET` + `PEXPIRE` + `DEL`) | single `MULTI`/`EXEC` | PASS — confirmed by code review of the same `.multi()` chain the test exercises |

---

## Engineering debrief

**Data flow:** an accepted resume position reaches `clearSignalLossEpisode(entityId, resumedAtMs)` → reads `alert-state:{entity_id}` → if an episode was open, one `MULTI` writes `recent-loss` fields, attaches `PEXPIRE`, and deletes `alert-state`, all inside a single `EXEC`.

**Trade-off:** `MULTI` instead of Lua, deliberately inconsistent with the rest of this codebase's atomic operations — because this is three unconditional writes with no read-then-branch logic, which is exactly what `MULTI` is for. Lua stays reserved for compare-and-swap logic elsewhere (leader lease, live-state monotonic guard, proximity-episode touch).

**Failure behaviour:** binary crash boundary — either none of the three commands landed (safe retry on the next accepted position) or all three did (no partial state possible). This is strictly better than the first draft of this fix, which put the `DEL` in a separate call after the `MULTI` and had a real third crash state: `EXEC` succeeded but `alert-state` was still stale.

## Manual inspection commands

```bash
docker exec sentinel-redis redis-cli PTTL recent-loss:<entity_id>
docker exec sentinel-redis redis-cli HGETALL recent-loss:<entity_id>
docker exec sentinel-redis redis-cli EXISTS alert-state:<entity_id>
```

## Knowledge-check questions

1. Why does folding `DEL alert-state` into the same `MULTI` as the `HSET`/`PEXPIRE` produce a strictly better crash boundary than doing it as a separate call afterward?
2. Why is `recent-loss`'s TTL described in `DATA_MODEL.md` as "retention," not "eligibility"?
3. Why does this checkpoint use `MULTI`/`EXEC` when every other atomic Redis operation in this codebase uses a Lua script?

## Optional manual tweak

`COMPOSITE_CORRELATION_WINDOW_MS` defaults to `120_000` (2 minutes) — an implementation choice, not something the accepted docs pin down. Worth tuning once you have a feel for realistic gaps between a resume and a following proximity encounter in the synthetic data.

## Next

Pre-CP2A: the Alert Evaluator's Kafka consumer-group membership needs to be scoped to the leader lease before composite eligibility logic can safely assume single-writer semantics over `alert-state`/`recent-loss`.

---

## Key observations

| Concept | Observed |
| --- | --- |
| Automated suite | 36/36 PASS, 3 new tests exercising the real handoff function |
| Manual `PTTL` after a real handoff | 117137ms, bounded by the 120000ms default window |
| Crash boundary | Binary — no partial state possible after folding `DEL` into the same `MULTI` |
| Atomicity primitive | `MULTI`/`EXEC`, not Lua — no conditional logic involved |
