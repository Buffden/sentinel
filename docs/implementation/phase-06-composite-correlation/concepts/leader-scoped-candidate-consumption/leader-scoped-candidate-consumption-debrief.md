# Leader-Scoped Candidate Consumption Debrief

Commit: `9e67ad7` — "fix(alert-evaluator): scope candidate consumption to leader lease"

---

## Setup

```bash
make up
cd services/alert-evaluator
```

---

## Experiment 1: automated suite against real Redpanda

New tests join a **disposable** test group (`test-alert-evaluator-session-{uuid}`), never the production `alert-evaluator` group, and poll `admin.describeGroups()` since group-state changes are asynchronous:

```text
Test Files  3 passed (3)
     Tests  21 passed (21)
```

| Test | Proves |
| --- | --- |
| joining a session makes this instance the sole real Kafka group member | Real `JoinGroup`, not a mock |
| stopping the session actually leaves the real Kafka group | Real `LeaveGroup` |
| `stop()` is idempotent under concurrent callers | `Promise.all([session.stop(), session.stop()])` converges on one disconnect, no error |

---

## Experiment 2: real two-process failover against the live dev stack

Two real `tsx src/evaluator.ts` processes, tracked by PID (not shell job number — this repo's own Phase 05 exit-verification already documented job-control pitfalls with multiple backgrounded processes).

**Instance A starts, becomes leader:**

```text
{ instanceId: '0d62d971-...' } kafka producer connected
{ instanceId: '0d62d971-...' } joined candidate consumer group
{ instanceId: '0d62d971-...' } acquired leader lease — starting scan loop
```

```bash
$ docker exec sentinel-redpanda rpk group describe alert-evaluator
MEMBERS      1
MEMBER-ID    alert-evaluator-d5871e76-74a7-4185-9d8b-597bbe28756c
```

**Instance B starts — must become a follower and must not join:**

```text
{ instanceId: '78aefbde-...' } kafka producer connected
{ instanceId: '78aefbde-...' } running as follower — waiting for leader lease
```

```bash
$ docker exec sentinel-redpanda rpk group describe alert-evaluator
MEMBERS      1
MEMBER-ID    alert-evaluator-d5871e76-74a7-4185-9d8b-597bbe28756c   # unchanged — still only A
```

**`SIGTERM` on A — graceful leave:**

```text
{ instanceId: '0d62d971-...' } shutting down
{ instanceId: '0d62d971-...' } left candidate consumer group
```

Process confirmed fully exited (not just the `npm exec` wrapper) before checking group state.

**B takes over:**

```text
{ instanceId: '78aefbde-...' } joined candidate consumer group
{ instanceId: '78aefbde-...' } acquired leader lease — starting scan loop
```

```bash
$ docker exec sentinel-redpanda rpk group describe alert-evaluator
MEMBERS      1
MEMBER-ID    alert-evaluator-3b3932c7-31d0-43f8-ba87-e63c01ec35a1   # new member-id — clean handoff
```

**`SIGTERM` on B — final state:**

```bash
$ pgrep -f "tsx src/evaluator.ts"
(no output — no lingering processes)
$ docker exec sentinel-redpanda rpk group describe alert-evaluator
STATE        Empty
MEMBERS      0
```

| Check | Expected | Observed |
| --- | --- | --- |
| Leader alone shows as the sole real group member | 1 member | PASS |
| Follower never appears in the group | still 1 member, same member-id | PASS |
| Graceful shutdown leaves the group before exiting | `left candidate consumer group` logged, then exit | PASS |
| Failover produces a new sole member, not zero-then-one with observed overlap | new member-id, still exactly 1 | PASS |
| Final state after both instances stopped | `Empty` / 0 members, no lingering processes | PASS |

---

## Engineering debrief

**Data flow:** on lease acquisition, `runLeaderSession` calls `startCandidateConsumerSession(config.GROUP_ID)`, which creates a fresh `Consumer`, joins the group, and starts the scan loop. The lease-loss callback aborts the scan loop and calls `session.stop()` in the same synchronous step — not waiting for the scan loop's next tick to notice.

**Trade-off:** this does not claim mathematically perfect zero-overlap fencing during failover — see the concept note's "honest guarantee" section. Kafka's own rebalance, not this code, is what fences an in-flight message; deterministic alert identity and idempotent persistence remain the actual correctness backstop for that bounded overlap, same as everywhere else in Sentinel.

**Failure behaviour:** `session.stop()` is memoized so the lease-loss callback, the session's own teardown, and `shutdown()` cannot race a second `disconnect()` call. The real two-process test above never observed a window with two group members simultaneously — but the design does not depend on that being provably impossible, only bounded and absorbed downstream.

## Manual inspection commands

```bash
docker exec sentinel-redpanda rpk group describe alert-evaluator
pgrep -f "tsx src/evaluator.ts"   # confirm no lingering processes after shutdown
```

## Knowledge-check questions

1. What specific line in ADR-005 did the original Phase 05 implementation violate, and why did the original comment's reasoning ("Kafka partitioning is sufficient") not hold once composite correlation entered the picture?
2. Why does the real two-process experiment prove more than the automated `describeGroups()` polling tests alone?
3. What would you observe in `rpk group describe` if `session.stop()` were *not* idempotent and two teardown paths raced?

## Optional manual tweak

Run the two-process experiment yourself, but insert a deliberate delay before B's `SIGTERM`-triggered leave, and watch `rpk group describe` mid-transition to build intuition for the failover window this checkpoint deliberately does not eliminate.

## Next

Pre-CP2B (already resolved, `4952e95`) and CP2 (`1efa70c`) build directly on this: composite eligibility resolution assumes single-writer semantics over `alert-state`/`recent-loss` during normal operation, with deterministic identity as the backstop during the bounded failover window described above.

---

## Key observations

| Concept | Observed |
| --- | --- |
| Automated suite | 21/21 PASS, including real `JoinGroup`/`LeaveGroup` against a disposable test group |
| Real leader join | `MEMBERS 1`, real member-id |
| Real follower non-participation | Group unchanged while follower alive |
| Real failover handoff | New member-id, still exactly 1 member, no observed overlap |
| Final clean state | `Empty` / 0 members, no lingering processes |
