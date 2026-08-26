# Leader Election — Design and Learning Reference

Plain language first, then technical depth, then the code. Use this to understand, inspect, and defend the CP1 implementation.

---

## Why one active evaluator

The Alert Evaluator scans all `entity:live:*` hashes on a fixed interval and emits alerts to Kafka. If two instances run simultaneously, both scan the same entities and both publish the same alert. The deterministic `alert_id` and idempotent DB write at the API absorb the duplicate at persistence time, but the double Kafka publish and double WebSocket delivery are wasteful and harder to reason about in failure analysis.

Leader election gives one instance the exclusive right to scan. All other instances stand by as followers. If the leader crashes, a follower takes over automatically within a bounded time.

This is not a correctness requirement — it is an efficiency and clarity requirement. The system remains correct without leader election because of deterministic `alert_id` plus `ON CONFLICT (alert_id) DO NOTHING`. Leader election reduces unnecessary work.

---

## Concepts in plain language

### 1. Process vs service instance

A **process** is a running program — an OS-level unit with its own memory and PID. A **service instance** is one copy of your application running as a process. Multiple instances of the same service run as independent processes. They do not share memory. The only shared state between Alert Evaluator instances is Redis.

In CP1 you can simulate multiple instances on one machine by starting `evaluator.ts` in two separate terminals. Two processes, same code, same Redis.

### 2. What "leader" means

A leader is the one instance currently allowed to do a specific job. All others (followers) stand by. When the leader fails, a follower takes over. In Sentinel the job is: scan all live entities on a timer and emit signal-loss alerts to Kafka.

### 3. Why multiple instances exist

Not to parallelize the scan — one scan is fast enough. Multiple instances exist for **availability**: if the single active instance crashes, the system should resume scanning automatically within a bounded time, without human intervention. Followers make that possible.

### 4. The Redis lease

A lease is a time-limited claim. The leader writes a key with a short TTL (15s). As long as it keeps renewing the TTL before it expires, it holds the lease. If it crashes and stops renewing, the TTL drains to zero, the key disappears, and a follower can claim it.

The key is `alert-evaluator:leader`. Its value is the leader's `instanceId` (a UUID generated at startup). The value matters — it lets the holder prove ownership during renewal and release.

### 5. Why Redis and not ZooKeeper or etcd

The rest of the system already depends on Redis for live state, pub/sub, and episode state. Adding a separate coordination service for one lease key is not justified at this scale. Redis `SET NX PX` provides a correct single-instance distributed lock for a single-leader scenario. Its limitations (no linearizable reads, replication lag on Sentinel/Cluster failover) are acceptable because the deterministic `alert_id` is the correctness backstop.

### 6. SET NX PX — atomic acquisition

```text
SET alert-evaluator:leader {instanceId} PX 15000 NX
```

- `PX 15000` — set TTL to 15000 ms
- `NX` — only write if the key does not already exist

Redis processes commands one at a time. `NX` makes this atomic: if two instances send this command at the same moment, exactly one gets `OK` and the other gets `null`. There is no race window. The `NX` flag is what makes the election correct.

### 7. Leader renewal

A 15s TTL would expire during a long scan if nothing refreshed it. The leader runs a renewal timer every 5s. Each renewal resets the TTL back to 15s, so the key stays alive as long as the leader process is healthy. Three renewal attempts fit within one TTL, giving tolerance for a single slow renewal without triggering a false takeover.

### 8. Follower takeover

Followers poll every 5s, attempting `SET ... NX`. As long as the key exists, all attempts return `null`. When the key expires, the next attempt succeeds and that follower becomes leader.

Maximum gap with no active leader:

```text
TTL drain time (up to 15s) + follower retry interval (up to 5s) = 20s worst case
```

### 9. Ownership-safe renew and release with Lua

A plain `PEXPIRE` would reset the TTL on whoever's key that is. If a new leader has already taken over, a stale restarted instance calling `PEXPIRE` would extend the new leader's lease — not its own.

Lua scripts run atomically in Redis. The renew script reads the key first and only extends the TTL if the value matches this instance's ID:

```lua
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('PEXPIRE', KEYS[1], ARGV[2])
else
  return 0
end
```

Returns `1` if renewed (still owner), `0` if not (lease lost or expired).

The release script does the same before deleting:

```lua
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
else
  return 0
end
```

Called during graceful shutdown. Allows a follower to acquire immediately rather than waiting for TTL expiry. Without Lua, a GET followed by a PEXPIRE could have another command slip in between. Lua's atomicity closes that gap.

### 10. Fail-closed behavior

When you cannot confirm a safety condition, stop doing the risky thing rather than continuing optimistically. The risky thing is scanning and publishing alerts. The safety condition is confirmed lease ownership.

If the renewal Lua script throws a Redis error (connection drop, timeout), the instance cannot confirm it still owns the lease. The correct response is to treat any renewal error as lease loss, stop scanning, and revert to follower. The cost of stopping unnecessarily is at most one missed scan interval. The cost of continuing without confirmed ownership is duplicate work and harder failure analysis.

### 11. Lease loss while the process stays alive

The process is running, but it has lost the Redis key — either because Redis restarted, or because another instance overwrote the key. The renewal timer fires, Lua returns 0 (wrong owner), and `onLeaseLost` is called. The scan loop stops immediately. The process stays alive and reverts to follower polling. When the injected key expires, it competes normally and may re-acquire.

The process must not emit alerts, write Redis episode state, or publish to Kafka during the period it does not hold the lease.

### 12. Why AbortController and not a boolean flag

The original code used `let scanning = false` shared across sessions. The failure sequence:

1. Instance loses lease — sets `scanning = false`
2. Old scan loop is mid-sleep in `await sleep(30_000)`
3. Instance re-acquires lease — sets `scanning = true`
4. New scan loop starts
5. Old sleep finishes — old loop sees `scanning = true`, continues
6. Two loops now running simultaneously

`AbortController` is scoped to one leader session. When the lease is lost, `ac.abort()` is called. The `sleep()` function listens on the signal and resolves immediately. The old loop exits before the next tick. The new session gets a fresh controller with no shared state from the previous session.

### 13. Why deterministic alert_id and idempotency are still needed

Leader election reduces duplicate Kafka publishes but does not eliminate them:

- Brief window after Redis restart where both instances see the key absent and both attempt `SET NX` — only one succeeds, but they may both be mid-attempt
- Redis Cluster or Sentinel primary failover can make the key invisible to the new primary, allowing a second leader to form momentarily
- The previous leader's Kafka message may already be in-flight when the new leader takes over and publishes the same alert

The `alert_id` is deterministic (`{entity_id}:SIGNAL_LOSS:{dark_since_ms}`) and the API persists with `ON CONFLICT (alert_id) DO NOTHING`. Even if two Kafka messages arrive with the same alert, exactly one durable row is written. Leader election reduces duplicates; idempotency makes them safe.

---

## Failure modes

### Leader crashes mid-scan

The key expires after `LEADER_LEASE_TTL_MS`. A follower acquires it on its next retry (every `FOLLOWER_RETRY_INTERVAL_MS = 5s`). Maximum gap: 15s + 5s = 20s with no scan.

Any alert the crashed leader partially processed is safe: the Kafka publish either completed (alert is in the topic, API picks it up) or did not (the next leader's scan will re-detect the dark entity and publish again with the same deterministic `alert_id`).

### Leader loses Redis connectivity briefly

The renewal Lua script fails or times out. The evaluator treats any renewal error as lease loss and stops scanning. When Redis reconnects, the key may still be held by this instance (if the TTL has not expired) or by a new leader. The evaluator does not re-acquire automatically — it reverts to follower and competes normally.

### Two instances both believe they are leader (split-brain)

Possible briefly after a Redis restart where both instances see the key as absent. Both attempt `SET NX PX` but Redis's atomic NX ensures only one succeeds. There is no window where two instances hold the key simultaneously under a single Redis node.

On Redis Cluster or Sentinel with async replication, a leader elected on a primary that fails before replication may be invisible to the new primary. A second instance could acquire the key. This is the known limitation of Redis-based locks. The deterministic `alert_id` absorbs the consequence.

---

## What the evaluator must do on lease loss

- Stop the scan loop immediately.
- Stop publishing to Kafka.
- Do not write `alert-state:*` keys.
- Revert to follower polling.

An instance that continues scanning after losing the lease is a bug, not a race condition. The renewal check on every interval is the enforcement point.

---

## ASCII flow

```text
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

## Map mental model to code

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

## Redis lab

Run these against a live Sentinel stack (`make up`). Start `evaluator.ts` first, then run in a second terminal.

```bash
# 1. Confirm the leader key exists and holds a UUID
docker exec sentinel-redis redis-cli GET alert-evaluator:leader

# 2. Check TTL — run twice 6s apart; it should reset, not decay to zero
docker exec sentinel-redis redis-cli PTTL alert-evaluator:leader
sleep 6
docker exec sentinel-redis redis-cli PTTL alert-evaluator:leader

# 3. Start a second instance; confirm it logs "follower"
#    and that GET still shows only the first instance's UUID

# 4. Simulate lease loss without killing the process
docker exec sentinel-redis redis-cli SET alert-evaluator:leader fake-other-instance PX 15000
# Watch: "lease lost — aborting leader session", then "running as follower"

# 5. Speed up reacquisition
docker exec sentinel-redis redis-cli DEL alert-evaluator:leader
# Confirm the instance re-acquires and logs a scan tick

# 6. Simulate a crash
pkill -9 -f "src/evaluator.ts"
# Watch PTTL drain; confirm follower acquires within 20s

# 7. Confirm graceful release
node_modules/.bin/tsx src/evaluator.ts &
kill %1    # SIGTERM
docker exec sentinel-redis redis-cli EXISTS alert-evaluator:leader
# Expected: 0
```

---

## Safe manual tweak

In `evaluator.ts`, temporarily change `FOLLOWER_RETRY_INTERVAL_MS` from `5_000` to `2_000`. Kill the leader with `pkill`. Observe the follower acquires faster. Verify log timestamps confirm the shorter interval. Restore to `5_000` when done.

---

## Retention questions

1. Two instances start at exactly the same millisecond and both execute `SET alert-evaluator:leader {id} PX 15000 NX`. What happens and why is only one the leader?
2. The leader's renewal timer fires but the Lua script returns 0. What does the code do next, and why does it not simply retry?
3. A restarted instance executes `leader.release()` during shutdown. The key currently belongs to a different instance. What does the Lua release script do and why?
4. Why is `PEXPIRE` alone not safe for renewal? What is the specific failure scenario?
5. Walk through the exact sequence of events that causes two concurrent scan loops when using a shared boolean flag.
6. Why is `AbortController` scoped per leader session rather than per instance?
7. Name one scenario where two Kafka publishes of the same alert can occur despite correct leader election.
8. What is the maximum leader gap in seconds after a crash, given `LEADER_LEASE_TTL_MS = 15000` and `FOLLOWER_RETRY_INTERVAL_MS = 5000`? Derive the answer.
9. What does `runScan()` currently do in CP1? What will it do in CP2?
10. If Redis becomes completely unavailable, what does a running leader instance do? Is this the correct behavior?

---

## CP1 completion checklist

- [ ] I can explain why multiple evaluator instances exist (availability, not parallelism)
- [ ] I can describe what `SET key value PX 15000 NX` does and why `NX` is what makes election correct
- [ ] I can explain why plain `PEXPIRE` is unsafe for renewal and what Lua atomicity solves
- [ ] I can trace the exact sequence of events from leader crash to follower acquiring the lease
- [ ] I can explain the shared-boolean bug and why `AbortController` fixes it
- [ ] I can explain fail-closed: what triggers it, what stops, and why stopping is safer than continuing
- [ ] I can name one scenario where duplicate Kafka publishes occur despite correct leader election, and explain why deterministic `alert_id` absorbs it
- [ ] I can run the Redis lab commands from memory and interpret the output
- [ ] I can state what CP1 delivered and what CP2 adds
