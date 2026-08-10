# ADR-005: Leader Election for Alert Evaluator

**Status:** Accepted
**Date:** 2026-08-05

---

## Context

The alert evaluator is a stateful worker that consumes Kafka topics (`deviation.candidates`, `proximity.candidates`), runs a scheduled Redis scan for signal loss, reads from Neo4j for composite alert context, reads/writes Redis state keys (`alert-state`, `deviation-state`), and emits alerts. If multiple instances of the alert evaluator run simultaneously, they may each independently evaluate the same event and emit duplicate alerts  - or worse, emit conflicting alerts for the same entity at the same time.

The system must prevent duplicate alert emission without reducing the evaluator to a single non-redundant process.

---

## Decision

Use leader election to ensure only one alert evaluator instance is the active writer at any time. Follower instances remain running and ready to take over if the leader fails.

---

## Reasoning

**Single-writer guarantee without a single point of failure.** A single evaluator instance is simple but fragile  - if it crashes, no alerts are emitted until it restarts. Leader election gives the safety of single-writer semantics with the resilience of multiple instances.

**Duplicate alert prevention is not solvable at the application level without coordination.** Naive dedup (e.g. checking a "has this alert been emitted" flag in Redis before writing) introduces a check-then-act race condition. Two instances checking simultaneously both see "not emitted" and both write. Leader election removes the race by ensuring only one instance ever evaluates at a time.

**Kafka consumer group is not sufficient here.** Consumer group partitioning handles message-level parallelism, but the alert evaluator's state (the entity graph, the correlation window) is not partition-local  - it spans the entire graph. A consumer group partition assignment does not prevent two instances from evaluating the same cross-entity correlation.

---

## Implementation approach

Use Redis-based leader election (e.g. Redlock or a simple `SET NX PX` lease with TTL). The active leader holds a key `alert-evaluator:leader` with a short TTL and renews it on each heartbeat. Followers poll for the key; if it expires (leader crashed), one follower acquires it and becomes the new leader.

Redis is already in the stack  - no additional infrastructure component is required.

---

## Alternatives Considered

### Application-level dedup via Redis flag (rejected)
- Race condition between check and write  - two instances can both pass the check simultaneously
- Adds complexity without solving the root problem
- Not defensible under interview scrutiny

### Zookeeper / etcd for distributed coordination (rejected)
- More robust leader election primitives (ZAB, Raft), but adds a new infrastructure component
- Overkill for a single-service coordination problem when Redis is already present
- Operational cost is high; Redis SET NX with TTL is sufficient for this use case

### Single evaluator instance, no election (rejected)
- Simple, but a single point of failure
- Unacceptable if the goal is to demonstrate understanding of distributed coordination

---

## Consequences

- The leader election lease duration determines the maximum time between leader failure and failover — must be tuned based on acceptable alert latency
- The leader must handle the case where it loses the lease mid-evaluation and stops emitting before completing a batch
- Redis dependency for coordination means Redis failure also disables alert evaluation — acceptable given Redis is already a core dependency
- **Kafka consumer lifecycle:** only the current lease holder creates and joins the `alert-evaluator` consumer group. Followers must not join — an idle member that does not poll causes the group to rebalance on heartbeat timeout, disrupting the leader's consumption. On lease acquisition: create the consumer, subscribe, start polling. On lease loss:
  1. Stop accepting new evaluation work immediately
  2. Stop / pause Kafka consumption
  3. Wait for or cancel the current in-flight evaluation (document the policy — e.g. "best-effort cancel; never retry if lease is lost")
  4. Close the Kafka consumer and leave the consumer group
  5. Return to follower polling loop (attempt NX acquire on each poll)
- **Lease renewal:** use Lua compare-and-expire rather than `SET XX PX`. The correct script: `if GET('alert-evaluator:leader') == instance_id then PEXPIRE('alert-evaluator:leader', LEADER_TTL_MS)`. The `SET XX PX` command is not safe for renewal: it sets the value unconditionally (overwriting whoever holds the key) as long as the key exists. A slow old leader whose lease has expired and been acquired by a new leader could run `SET XX PX` and overwrite the new leader's identity with its own, effectively stealing the lease. The Lua GET-then-PEXPIRE pattern checks ownership before extending.
- **Lease release on clean shutdown:** compare before deleting. Use a Lua script or a conditional delete: if `GET alert-evaluator:leader == instance_id then DEL`. This prevents a slow-shutting instance from deleting a lease that a new leader has already acquired.
- Deterministic `alert_id` format (`{entity_id}:{alert_type}:{window_start_ms}`) remains the correctness backstop: if a lease race ever causes two instances to evaluate the same event, the `ON CONFLICT DO NOTHING` in the `alerts` table prevents duplicate records.
- **POC-05 must validate:** (1) messages arriving during a leadership transition are not lost or double-processed; (2) leader crash after processing but before offset commit; (3) lease loss mid-evaluation; (4) exactly one active Kafka consumer group member at all times; (5) ownership-safe renewal and release.
