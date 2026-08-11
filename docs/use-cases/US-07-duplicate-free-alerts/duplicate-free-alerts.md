# US-07: Replay-Safe Alert Emission and Leader Failover

**Actor:** Operator / System
**Status:** Defined

---

## Story

As an operator, I want one durable incident per logical anomaly even when the Alert Evaluator fails over or Kafka redelivers events, so that retries and coordination races do not create duplicate durable alerts.

---

## Acceptance Criteria

- Under normal operation, only one Alert Evaluator instance is active because of the Redis leader lease.
- Followers do not join/poll the evaluator Kafka consumer group until leadership is acquired.
- A leader crash causes takeover after lease expiry without requiring manual intervention.
- Lease renewal/release are ownership-safe; a stale former leader cannot extend or delete a new leader's lease.
- Kafka may redeliver/replay the same logical alert input; Sentinel does not claim exactly-once transport.
- The same logical alert always receives the same deterministic `alert_id`.
- API persistence with `ON CONFLICT DO NOTHING` produces one durable alert row for that identity even if the logical alert is published more than once.
- WebSocket lifecycle notifications may be duplicated; clients deduplicate/converge by alert identity and lifecycle state.

---

## Flow Diagrams

### Leader Election

![Leader Election](../../../diagrams/docs/use-cases/US-07-duplicate-free-alerts/leader-election.svg)

Multiple evaluator instances compete for one Redis lease. Only the owner becomes the active rule evaluator and Kafka consumer.

### Failover

![Failover](../../../diagrams/docs/use-cases/US-07-duplicate-free-alerts/failover.svg)

When leadership is lost, the old evaluator stops accepting work and leaves the consumer group. A follower acquires the expired lease and resumes processing. Duplicate logical publication around the failover boundary remains safe because downstream durable identity is deterministic.

### Race Condition Without Election

![Race Condition Without Election](../../../diagrams/docs/use-cases/US-07-duplicate-free-alerts/race-condition-without-election.svg)

Leader election prevents two fully active stateful evaluators from independently mutating the same rule state. Durable idempotency remains necessary because coordination does not eliminate Kafka replay.

---

## Architectural Justification

Justifies: [ADR-005 - Leader Election for Alert Evaluator](../../adr/ADR-005-leader-election-alert-evaluator.md), [ADR-007 - Deterministic Idempotency Identity](../../adr/ADR-007-idempotency-key-schema.md)

The design intentionally combines two different protections:

1. leader election for single active rule ownership;
2. deterministic durable identity for replay/failover convergence.

Neither is described as exactly-once transport.
