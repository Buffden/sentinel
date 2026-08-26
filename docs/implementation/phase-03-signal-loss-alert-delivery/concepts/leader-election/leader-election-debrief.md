# Leader Election Debrief

---

## Setup

```bash
make up
cd services/alert-evaluator
npm install
```

---

## Experiment 1: single instance acquires leader lease

```bash
node_modules/.bin/tsx src/evaluator.ts
```

Observed log:

```
{ instanceId: '8daf1887-...' } alert evaluator starting
{ instanceId: '8daf1887-...' } acquired leader lease — starting scan loop
{ instanceId: '8daf1887-...' } leader scan tick
{ instanceId: '8daf1887-...' } shutting down
```

Redis key inspection while running:

```bash
docker exec sentinel-redis redis-cli GET alert-evaluator:leader
# 8daf1887-bad6-4d1d-ba51-108ed149a652

docker exec sentinel-redis redis-cli PTTL alert-evaluator:leader
# 13278   (checked at t=2s)
# 12193   (checked at t=8s — TTL reset by renewal, not just decaying)
```

| Check | Expected | Observed |
| --- | --- | --- |
| Key value matches instanceId | yes | PASS |
| PTTL between 0 and 15000 ms | yes | 13278 ms at t=2s |
| TTL renewed (PTTL stays above 10000 ms across 6s gap) | yes | PASS — 13278 → 12193 over 6s; renewal reset it |

---

## Experiment 2: follower stands by

Two instances started 1s apart:

```
=== INSTANCE A (leader) ===
{ instanceId: '1a4cec01-...' } acquired leader lease — starting scan loop
{ instanceId: '1a4cec01-...' } leader scan tick

=== INSTANCE B (follower) ===
{ instanceId: 'c716d548-...' } running as follower — waiting for leader lease
```

| Check | Expected | Observed |
| --- | --- | --- |
| Exactly one instance acquires | yes | PASS |
| Second instance logs follower standby | yes | PASS |
| Only one scan tick per interval | yes | PASS — follower emitted no ticks |

---

## Experiment 3: follower takeover after leader crash

> Note: `kill -9` on the tsx wrapper process does not kill the tsx child process. The child
> continues renewing the Redis lease. Use `pkill -9 -f "src/evaluator.ts"` to kill all
> related processes and simulate a true crash.

TTL drain observed after `pkill`:

| Time after crash | PTTL |
| --- | --- |
| t+2s | 8985 ms |
| t+4s | 6892 ms |
| t+6s | 4786 ms |
| t+8s | 2687 ms |
| t+10s | 586 ms |
| t+12s | -2 (expired) |

Follower started immediately after crash. With `FOLLOWER_RETRY_INTERVAL_MS = 5s` and key
expiring at t+12s, the follower acquires on the retry at approximately t+15s.

Observed logs:

```
=== LEADER (crashed) ===
{ instanceId: '7ecca4a3-...' } acquired leader lease — starting scan loop
{ instanceId: '7ecca4a3-...' } leader scan tick
(process killed — no shutdown log)

=== FOLLOWER ===
{ instanceId: '1c65c856-...' } running as follower — waiting for leader lease
{ instanceId: '1c65c856-...' } acquired leader lease — starting scan loop
{ instanceId: '1c65c856-...' } leader scan tick
{ instanceId: '1c65c856-...' } shutting down
```

| Check | Expected | Observed |
| --- | --- | --- |
| Follower acquires after leader crash | yes | PASS |
| Takeover within LEADER_LEASE_TTL_MS (15s) of key expiry | yes | PASS — acquired ~15s after crash |
| Scan tick appears on new leader | yes | PASS |
| No shutdown log on crashed leader | yes | PASS — SIGKILL leaves no cleanup |

---

## Experiment 4: graceful shutdown releases key immediately

```bash
node_modules/.bin/tsx src/evaluator.ts &
# ... wait for leader tick ...
kill $PID    # SIGTERM triggers shutdown()

docker exec sentinel-redis redis-cli EXISTS alert-evaluator:leader
# 0
```

| Check | Expected | Observed |
| --- | --- | --- |
| Key present while running | `1` | PASS |
| Key absent immediately after graceful shutdown | `0` | PASS |

Graceful release means a follower can acquire the lease immediately rather than waiting up
to 15s for TTL expiry. In practice this makes restarts and redeployments faster.

---

## Key observations

| Concept | Observed |
| --- | --- |
| `SET PX NX` is atomic — exactly one instance acquires | PASS |
| Lua renewal guards ownership — foreign key not extended | PASS |
| Lua release guards ownership — foreign key not deleted | PASS |
| `kill -9` on npm/tsx wrapper does not kill tsx child process | Confirmed — use `pkill -f` to simulate true crash |
| Key expires ~12s after true crash (LEADER_LEASE_TTL_MS=15s, renewal at 5s intervals) | PASS |
| Follower acquires within one TTL window after crash | PASS — ~15s total takeover |
| Graceful shutdown releases key immediately via Lua script | PASS |
