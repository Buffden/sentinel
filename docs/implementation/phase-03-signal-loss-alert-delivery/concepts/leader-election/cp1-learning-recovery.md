# CP1 Learning Recovery — Leader Election

This document covers the concepts behind CP1 in plain language first, then maps each concept to the actual Sentinel code. Use it to build the understanding needed to explain and defend this checkpoint without AI assistance.

---

## 1. Process vs service instance

A **process** is a running program — an OS-level unit with its own memory and PID. A **service instance** is one copy of your application running as a process.

In production you run multiple instances of the same service for availability: if one crashes, the others keep serving. In Sentinel, the Alert Evaluator is deployed this way. Each instance is the same binary, but they are independent processes. They do not share memory. The only shared state they have is Redis.

In CP1 you can simulate this on one machine by starting `evaluator.ts` in two separate terminals. Two processes, same code, same Redis.

---

## 2. What "leader" means

A leader is the one instance currently allowed to do a specific job. All others (followers) stand by. When the leader fails, a follower takes over.

The job in Sentinel: scan all live entities on a timer and emit signal-loss alerts to Kafka. If two instances both do this job simultaneously, you get duplicate Kafka publishes. The deterministic `alert_id` and idempotent DB write absorb the duplicate at the API, but you have done twice the work and made the system harder to reason about. Leader election reduces that to one active scanner at a time.

---

## 3. Why multiple evaluator instances exist

Not to parallelize the scan — one scan is fast enough. Multiple instances exist for **availability**: if the single active instance crashes, the system should resume scanning automatically within a bounded time, without human intervention. Followers make that possible. One runs; the rest wait.

---

## 4. Leader election

Leader election is the process by which multiple instances agree on exactly one leader at a time, without a central coordinator telling them who wins. In Sentinel this is solved with a single Redis key. Whoever writes that key first wins.

---

## 5. The Redis lease

A lease is a time-limited claim. The leader writes a key with a short TTL (15s). As long as it keeps renewing the TTL before it expires, it holds the lease. If it crashes and stops renewing, the TTL drains to zero, the key disappears, and a follower can claim it.

The key is `alert-evaluator:leader`. Its value is the leader's `instanceId` (a UUID generated at startup). The value matters — it lets the holder prove ownership during renewal and release.

---

## 6. SET NX PX — atomic acquisition

```
SET alert-evaluator:leader {instanceId} PX 15000 NX
```

- `SET key value` — write the key
- `PX 15000` — set TTL to 15000 ms
- `NX` — only write **if the key does not already exist**

Redis processes commands one at a time. `NX` makes this atomic: if two instances send this command at the same moment, exactly one gets `OK` and the other gets `null`. There is no race window. The `NX` flag is what makes the election correct.

---

## 7. Leader renewal

A 15s TTL would expire before the next scan if nothing refreshed it. The leader runs a renewal timer every 5s. Each renewal resets the TTL back to 15s, so the key stays alive as long as the leader process is healthy.

The renewal interval (5s) is well below the TTL (15s), giving three renewal attempts before expiry. This provides tolerance for a single slow renewal without triggering a false takeover.

---

## 8. Follower takeover

Followers poll every 5s, attempting `SET ... NX`. As long as the key exists (leader healthy), all attempts return `null`. When the key expires (leader crashed), the next `SET NX` attempt succeeds and that follower becomes the new leader.

Maximum gap with no active leader:

```
TTL drain time (up to 15s) + follower retry interval (up to 5s) = 20s worst case
```

In the experiment the follower acquired in ~15s because its retry happened to land just after the key expired.

---

## 9. Ownership-safe renew and release with Lua

A plain `PEXPIRE alert-evaluator:leader 15000` would reset the TTL on *whoever's key that is*. If a new leader has already taken over, a stale restarted instance calling `PEXPIRE` would extend the new leader's lease — not its own. That is a bug.

Lua scripts run atomically in Redis. The renew script reads the key first and only extends the TTL if the value matches this instance's ID:

```lua
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('PEXPIRE', KEYS[1], ARGV[2])
else
  return 0
end
```

The release script does the same before deleting:

```lua
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
else
  return 0
end
```

Both operations are read-then-write. Without Lua, another command could slip in between the read and the write. Lua's atomicity closes that gap.

---

## 10. Fail-closed behavior

"Fail closed" means: when you cannot confirm a safety condition, stop doing the risky thing rather than continuing optimistically.

The risky thing here is scanning and publishing alerts. The safety condition is confirmed lease ownership. If the renewal Lua script throws a Redis error (connection drop, timeout), the instance cannot confirm it still owns the lease. Continuing to scan could mean two active leaders both publishing alerts.

The correct response is to treat any renewal error as lease loss, stop scanning, and revert to follower. The cost of stopping unnecessarily is at most one missed scan interval. The cost of continuing without confirmed ownership is duplicate work and harder failure analysis.

---

## 11. Lease loss while the process stays alive

This is a more subtle scenario than a crash. The process is running, but it has lost the Redis key — either because Redis restarted, or because another instance overwrote the key (simulated in Experiment 5 with a direct `SET`).

The renewal timer fires, Lua returns 0 (wrong owner), and `onLeaseLost` is called. The scan loop must stop immediately. The process stays alive and reverts to follower polling. When the injected key expires, it competes normally and may re-acquire.

The important thing: the process never emits alerts, writes Redis episode state, or publishes to Kafka during the period it does not hold the lease.

---

## 12. Why AbortController and not a boolean flag

The original code used `let scanning = false` shared across sessions. Consider this sequence:

1. Instance loses lease — sets `scanning = false`
2. Old scan loop is mid-sleep in `await sleep(30_000)`
3. Instance re-acquires lease — sets `scanning = true`
4. New scan loop starts
5. Old sleep finishes — old loop sees `scanning = true`, continues
6. Two loops now running simultaneously

`AbortController` is scoped to one leader session. When the lease is lost, `ac.abort()` is called. The `sleep()` function listens on the signal and resolves immediately. The old loop exits before the next tick. The new session gets a fresh `AbortController`. There is no shared mutable state between sessions.

---

## 13. Why deterministic alert_id and idempotency are still needed

Leader election is an efficiency mechanism. It reduces the chance of duplicate Kafka publishes, but it does not eliminate it:

- Brief window after Redis restart where two instances both see the key absent and both attempt `SET NX` (only one succeeds, but they may both try)
- Redis Cluster or Sentinel primary failover can make the key invisible to the new primary, allowing a second leader to form momentarily
- The previous leader's Kafka message may already be in-flight when the new leader takes over and publishes the same alert

For these reasons, the `alert_id` is deterministic (`{entity_id}:SIGNAL_LOSS:{dark_since_ms}`) and the API persists alerts with `ON CONFLICT (alert_id) DO NOTHING`. Even if two Kafka messages arrive with the same alert, exactly one durable row is written. Leader election reduces duplicates; idempotency makes them safe.

---

## 14. ASCII flow

```
Instance A starts                      Instance B starts (1s later)
    |                                      |
    |-- SET alert-evaluator:leader A NX -->|         Redis
    |   result: OK (key did not exist)     |           |
    |                                      |           |
    |-- startRenewal() ----------------->  |     key = A, TTL=15s
    |   every 5s: Lua check + PEXPIRE      |           |
    |                                      |-- SET NX ->|
    |   runLeaderSession()                 |   result: null (key exists)
    |   while !aborted:                    |           |
    |     runScan()  [tick]                |   follower polling loop
    |     sleep(30s, signal)               |   every 5s: SET NX -> null
    |                                      |           |
    |-- process killed (pkill) ----------> |     TTL drains to 0
    |   renewal stops, key expires         |     key disappears (~12-15s)
    |                                      |           |
                                           |-- SET NX ->|
                                           |   result: OK
                                           |           |
                                           |-- startRenewal()
                                           |   runLeaderSession()
                                           |   runScan()  [tick]
```

---

## 15. Map mental model to code

| Concept | Where in code |
| --- | --- |
| Unique instance identity | `const instanceId = randomUUID()` — `evaluator.ts:11` |
| Lease acquisition | `leader.tryAcquire()` — `leader.ts:46`, uses `SET PX NX` |
| Lease TTL | `LEADER_LEASE_TTL_MS = 15_000` — `leader.ts:6` |
| Renewal interval | `LEADER_RENEWAL_INTERVAL_MS = 5_000` — `leader.ts:10` |
| Ownership-safe renew | `RENEW_SCRIPT` Lua — `leader.ts:17` |
| Ownership-safe release | `RELEASE_SCRIPT` Lua — `leader.ts:28` |
| Fail-closed on Redis error | `try/catch` in `startRenewal` — `leader.ts:61` |
| Per-session AbortController | `const ac = new AbortController()` — `evaluator.ts:26` |
| Lease loss stops scan loop | `ac.abort()` in renewal callback — `evaluator.ts:30` |
| Abortable sleep | `sleep(ms, ac.signal)` — `evaluator.ts:37`, `evaluator.ts:78` |
| Follower polling | `while (true)` with `sleep(FOLLOWER_RETRY_INTERVAL_MS)` — `evaluator.ts:50` |
| Graceful release on shutdown | `leader.release()` in `shutdown()` — `evaluator.ts:64` |
| Scan placeholder (CP2 TODO) | `runScan()` body — `evaluator.ts:15` |

---

## 16. Redis lab

Run these against a live Sentinel stack (`make up`). Start `evaluator.ts` first, then run the commands in a second terminal.

```bash
# 1. Confirm the leader key exists and holds a UUID
docker exec sentinel-redis redis-cli GET alert-evaluator:leader

# 2. Check the TTL — run twice 6s apart; it should reset, not decay to zero
docker exec sentinel-redis redis-cli PTTL alert-evaluator:leader
sleep 6
docker exec sentinel-redis redis-cli PTTL alert-evaluator:leader

# 3. Start a second instance in a third terminal; confirm it logs "follower"
#    and that GET still shows only the first instance's UUID

# 4. Simulate lease loss without killing the process
docker exec sentinel-redis redis-cli SET alert-evaluator:leader fake-other-instance PX 15000
# Watch the first instance log: "lease lost — aborting leader session"
# Watch it log: "running as follower — waiting for leader lease"

# 5. Speed up reacquisition by deleting the injected key
docker exec sentinel-redis redis-cli DEL alert-evaluator:leader
# Confirm the original instance re-acquires and logs a scan tick

# 6. Simulate a crash
pkill -9 -f "src/evaluator.ts"
# Watch PTTL drain; confirm follower acquires within 20s

# 7. Confirm graceful release
node_modules/.bin/tsx src/evaluator.ts &
# wait for tick...
kill %1    # SIGTERM
docker exec sentinel-redis redis-cli EXISTS alert-evaluator:leader
# Expected: 0 (key gone immediately)
```

---

## 17. Safe manual tweak

In `evaluator.ts`, temporarily change `FOLLOWER_RETRY_INTERVAL_MS` from `5_000` to `2_000`. Kill the leader with `pkill`. Observe the follower acquires faster because it retries more frequently. Verify the log timestamps confirm the shorter interval. Restore to `5_000` when done.

This makes the relationship between retry interval and takeover latency concrete without touching any Redis, Kafka, or persistence logic.

---

## 18. Retention questions

1. Two instances start at exactly the same millisecond and both execute `SET alert-evaluator:leader {id} PX 15000 NX`. What happens and why is only one the leader?

2. The leader's renewal timer fires but the Lua script returns 0. What does the code do next, and why does it not simply retry?

3. A restarted instance executes `leader.release()` during shutdown. The key currently belongs to a different instance that took over while this one was restarting. What does the Lua release script do and why?

4. Why is `PEXPIRE` alone not safe for renewal? What is the specific failure scenario?

5. The original code used `let scanning = false`. Walk through the exact sequence of events that causes two concurrent scan loops when using a shared boolean.

6. Why is `AbortController` scoped per leader session rather than being a single instance-level variable?

7. Leader election reduces duplicate Kafka publishes but does not eliminate them. Name one scenario where two publishes of the same alert can still occur despite correct leader election.

8. What is the maximum time in seconds the system can have no active leader after a crash, given `LEADER_LEASE_TTL_MS = 15000` and `FOLLOWER_RETRY_INTERVAL_MS = 5000`? Derive the answer.

9. What does `runScan()` currently do in CP1? What will it do in CP2?

10. If Redis becomes completely unavailable, what does a running leader instance do? Is this the correct behavior?

---

## 19. CP1 completion checklist

Consider CP1 learning complete when you can answer yes to all of these without looking at the code or docs:

- [ ] I can explain why multiple evaluator instances exist (availability, not parallelism)
- [ ] I can describe what `SET key value PX 15000 NX` does and why `NX` is what makes election correct
- [ ] I can explain why plain `PEXPIRE` is unsafe for renewal and what Lua atomicity solves
- [ ] I can trace the exact sequence of events from leader crash to follower acquiring the lease
- [ ] I can explain the shared-boolean bug and why `AbortController` fixes it
- [ ] I can explain fail-closed: what triggers it, what stops, and why stopping is safer than continuing
- [ ] I can name one scenario where duplicate Kafka publishes occur despite correct leader election, and explain why deterministic `alert_id` absorbs it
- [ ] I can run the Redis lab commands from memory and interpret the output
- [ ] I can state what CP1 delivered (leader election skeleton, lease lifecycle) and what CP2 adds (real signal-loss scan replacing the `runScan()` placeholder)
