# ADR-005: Leader Election for Alert Evaluator

**Status:** Accepted
**Date:** 2026-08-05

---

## Context

The Alert Evaluator combines scheduled Redis scanning with Kafka-driven rule inputs and maintains stateful anomaly episodes. Running multiple fully active evaluators would create concurrent writers over the same rule state and could emit duplicate/conflicting logical alerts.

Kafka consumer groups alone do not coordinate the scheduled signal-loss scan, so Sentinel needs one active evaluator at a time while retaining failover capacity.

---

## Decision

Use a Redis lease at `alert-evaluator:leader` so exactly one evaluator instance is the active rule evaluator under normal lease ownership.

Followers remain alive but do not join/poll the `alert-evaluator` Kafka consumer group until they acquire the lease.

---

## Lease Protocol

### Acquire

```text
SET alert-evaluator:leader {instance_id} NX PX {LEADER_TTL_MS}
```

### Renew

Renew only if the caller still owns the lease. Use an atomic compare-and-expire Lua script:

```text
if GET(key) == instance_id then
  PEXPIRE(key, LEADER_TTL_MS)
end
```

Do not use unconditional `SET XX PX` for renewal because a stale former leader could overwrite a new owner's value.

### Release

Delete only if the current value still matches the releasing instance. A slow shutdown must never delete another instance's newly acquired lease.

---

## Kafka Consumer Lifecycle

Only the lease holder creates/joins the `alert-evaluator` consumer group.

On acquisition:

1. create Kafka consumer;
2. subscribe to `deviation.candidates` and `proximity.candidates`;
3. start polling/evaluation.

On lease loss:

1. stop accepting new evaluation work;
2. stop/pause Kafka polling;
3. finish or cancel in-flight work according to an explicit implementation policy;
4. close/leave the consumer group;
5. return to follower acquire loop.

---

## What Leader Election Guarantees

Leader election prevents **concurrent active Alert Evaluators** from independently mutating the same anomaly-rule state under normal operation.

It does **not** create exactly-once Kafka transport and is not the sole duplicate-protection mechanism.

The correctness backstop remains deterministic alert identity plus idempotent durable persistence in the API/TimescaleDB path. If a failover race or Kafka replay republishes the same logical alert, the database converges on one durable row.

---

## Alternatives Considered

### Application-level check-then-set dedup only — rejected

Without single-writer coordination, independent rule evaluations can race before a durable dedup boundary is reached.

### ZooKeeper/etcd — rejected

Strong coordination primitives but unnecessary new infrastructure for this portfolio-scale single-service election problem.

### One evaluator instance with no election — rejected

Simple but creates an avoidable availability gap when the evaluator process fails.

---

## Consequences

- Redis failure disables evaluator leadership; acceptable because Redis is already a core live-state dependency.
- Failover latency is bounded by lease TTL/poll timing.
- Ownership-safe renewal/release must be tested under stale-leader scenarios.
- Exactly one evaluator should normally be active, but durable alert idempotency must still be correct if duplicate logical publication occurs.
- Production-hardening tests should include leader crash, stale-owner renewal attempt, clean shutdown, and replay around failover.
