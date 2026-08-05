# ADR-005: Leader Election for Alert Evaluator

**Status:** Accepted
**Date:** 2026-08-05

---

## Context

The alert evaluator is a stateful worker that reads from Neo4j and Redis, evaluates composite anomaly rules, and emits alerts. If multiple instances of the alert evaluator run simultaneously, they may each independently evaluate the same event and emit duplicate alerts  - or worse, emit conflicting alerts for the same entity at the same time.

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

- The leader election lease duration determines the maximum time between leader failure and failover  - must be tuned based on acceptable alert latency
- The leader must handle the case where it loses the lease mid-evaluation and stops emitting before completing a batch
- Redis dependency for coordination means Redis failure also disables alert evaluation  - acceptable given Redis is already a core dependency
