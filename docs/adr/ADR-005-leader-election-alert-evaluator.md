# ADR-005: Leader Election for Alert Evaluator

**Status:** Accepted
**Date:** 2026-08-05

---

## Context

The Alert Evaluator combines scheduled signal-loss scans with Kafka-driven deviation/proximity facts and owns shared anomaly episode state. Multiple simultaneously active evaluator instances could race on the same state and emit conflicting logical outcomes.

Kafka consumer groups alone do not solve this because scheduled signal-loss work is not partition-assigned and composite state spans entities rather than one Kafka partition.

---

## Decision

Use a Redis lease so only one Alert Evaluator instance is active at a time.

The lease key is:

```text
alert-evaluator:leader
```

with value = `instance_id`.

Acquire with `SET NX PX`.

Renew with an ownership-safe Lua compare-and-expire operation:

```text
if GET(key) == instance_id
then PEXPIRE(key, LEADER_TTL_MS)
```

Release with compare-before-DEL.

---

## Kafka Consumer Lifecycle

Only the current lease holder joins and polls the `alert-evaluator` Kafka consumer group.

On lease acquisition:

1. create/join the consumer;
2. subscribe to `deviation.candidates` and `proximity.candidates`;
3. start polling;
4. start/continue scheduled signal-loss evaluation.

On lease loss:

1. stop accepting new evaluation work;
2. pause/stop Kafka polling;
3. finish or safely abandon in-flight work according to the implementation policy;
4. close/leave the consumer group;
5. return to follower lease polling.

Followers must not remain idle members of the Kafka group because that creates unnecessary rebalances and does not provide useful standby behavior.

---

## What Leader Election Guarantees

Leader election provides a **single active evaluator** under normal lease operation. It prevents two healthy evaluator instances from intentionally processing the shared anomaly state concurrently.

It does **not** create exactly-once transport.

Kafka may redeliver after crashes or offset replay, and lease edge cases can theoretically cause the same logical alert to be attempted more than once. Correctness therefore also relies on:

- deterministic alert IDs (ADR-007);
- Redis replay/episode guards;
- idempotent API persistence (`alerts.alert_id` primary key).

The system guarantee is duplicate-free **durable alert state for the same logical identity**, not exactly-once message delivery.

---

## Why Ownership-Safe Renewal Matters

`SET XX PX` is unsafe for renewal because it can overwrite a lease value after ownership has changed.

Example:

1. evaluator A's lease expires;
2. evaluator B acquires the key;
3. delayed evaluator A runs an unconditional `SET XX PX`;
4. A overwrites B's value and effectively steals the lease.

Comparing the stored value before `PEXPIRE` prevents this stale-owner renewal.

The same reasoning applies to clean shutdown: a stale instance must not delete a lease now owned by another evaluator.

---

## Alternatives Considered

### Application-level check-then-act dedup (rejected)

Two instances can both observe "not emitted" before either writes. This does not coordinate scheduled/shared rule evaluation.

### Kafka consumer group only (rejected)

Does not coordinate scheduled signal-loss scans and does not create one global owner for cross-entity rule state.

### ZooKeeper / etcd (rejected)

Provides stronger dedicated coordination primitives but adds infrastructure that is unnecessary for this single-service v1 coordination problem.

### Single evaluator with no standby (rejected)

Simpler, but creates an avoidable service availability gap on process failure.

---

## Consequences

- Redis availability is required for alert evaluation leadership.
- Lease TTL/heartbeat cadence affects failover latency and must be validated experimentally.
- Implementation tests must cover stale renewal, stale release, leader crash, follower takeover, and Kafka consumer handoff.
- The API's deterministic durable dedup remains the final backstop if a logical alert is attempted more than once.
