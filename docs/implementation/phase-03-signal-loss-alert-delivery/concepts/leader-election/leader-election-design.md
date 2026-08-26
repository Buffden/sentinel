# Leader Election Design

---

## Why only one active evaluator

The Alert Evaluator scans all `entity:live:*` hashes on a fixed interval and emits alerts to
Kafka. If two instances run simultaneously, both scan the same entities and both publish the
same alert. The deterministic `alert_id` and idempotent DB write at the API absorb the
duplicate at persistence time, but the double Kafka publish and double WebSocket delivery are
wasteful and harder to reason about in failure analysis.

Leader election gives one instance the exclusive right to scan. All other instances stand by
as followers. If the leader crashes, a follower takes over automatically within one lease TTL.

This is not a correctness requirement — it is an efficiency and clarity requirement. The
system remains correct without leader election because of deterministic `alert_id` plus
`ON CONFLICT (alert_id) DO NOTHING`. Leader election reduces unnecessary work.

---

## The Redis lease approach

A single Redis key `alert-evaluator:leader` holds the active instance's ID.

The key has a short TTL (`LEADER_LEASE_TTL_MS = 15s`). The leader renews it every
`LEADER_RENEWAL_INTERVAL_MS = 5s`. If the leader crashes, the key expires after 15s and
a follower acquires it. The takeover window is at most one TTL.

### Why Redis and not ZooKeeper or etcd

The rest of the system already depends on Redis for live state, pub/sub, and episode state.
Adding a separate coordination service for one lease key is not justified at this scale.
Redis `SET NX PX` provides a correct single-instance distributed lock for a single-leader
scenario. Its limitations (no linearizable reads, replication lag on Sentinel/Cluster failover)
are acceptable because the deterministic `alert_id` is the correctness backstop.

---

## Acquire

```
SET alert-evaluator:leader {instance_id} PX 15000 NX
```

`NX` — only set if the key does not exist. Returns `OK` on success, `null` if another
instance holds the key. The operation is atomic in Redis's single-threaded command model.

---

## Renew

Renewal must not extend another instance's key. A plain `PEXPIRE` would reset the TTL
regardless of who owns the key. A Lua script checks ownership first:

```lua
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('PEXPIRE', KEYS[1], ARGV[2])
else
  return 0
end
```

Returns `1` if renewed (still owner), `0` if not (lease lost or expired). The evaluator
stops scanning immediately on a `0` return and reverts to follower mode.

---

## Release

Release must not delete another instance's key after a restart or network partition:

```lua
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
else
  return 0
end
```

Called during graceful shutdown. Allows a follower to acquire the lease immediately rather
than waiting for the TTL to expire.

---

## Failure modes

### Leader crashes mid-scan

The key expires after `LEADER_LEASE_TTL_MS`. A follower acquires it on its next retry
(every `FOLLOWER_RETRY_INTERVAL_MS = 5s`). Maximum gap: 15s + 5s = 20s with no scan.

Any alert the crashed leader partially processed is safe: the Kafka publish either completed
(alert is in the topic, API picks it up) or did not (the next leader's scan will re-detect
the dark entity and publish again with the same deterministic `alert_id`).

### Leader loses Redis connectivity briefly

The renewal Lua script fails or times out. The evaluator treats any renewal error as a lease
loss and stops scanning. When Redis reconnects, the key may still be held by this instance
(if the TTL has not expired) or by a new leader. The evaluator does not re-acquire
automatically after a connectivity loss — it reverts to follower and competes normally.

### Two instances both believe they are leader (split-brain)

Possible briefly after a Redis restart where both instances see the key as absent. Both
acquire via `SET NX PX` but Redis's atomic NX ensures only one succeeds. The other gets
`null` and starts as a follower. There is no window where two instances hold the key
simultaneously under a single Redis node.

On Redis Cluster or Sentinel with async replication, a leader elected on a primary that
then fails before replication may be invisible to the new primary. A second instance could
then acquire the key. This is the known limitation of Redis-based locks. The deterministic
`alert_id` absorbs the consequence.

---

## What the evaluator must do on lease loss

- Stop the scan loop immediately.
- Stop publishing to Kafka.
- Do not write `alert-state:*` keys.
- Revert to follower polling.

An instance that continues scanning after losing the lease is a bug, not a race condition.
The renewal check on every interval is the enforcement point.
