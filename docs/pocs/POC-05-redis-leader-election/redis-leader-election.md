# POC-05: Redis Leader Election

**Branch:** `poc/redis-leader-election`
**Status:** Not started

---

## Risk

ADR-005 commits to a specific Redis pattern (`SET NX PX` lease with TTL renewal) for alert evaluator coordination. Additionally, US-03 requires Redis-based alert state tracking to prevent re-emission, and US-01 relies on Redis TTL for automatic entity expiry from the map. All three Redis patterns must be validated together.

---

## Goal

Prove that leader election, alert state management, and entity TTL expiry all work correctly under realistic conditions.

---

## Validate

- Two Node.js processes both attempt to acquire the `alert-evaluator:leader` key simultaneously - exactly one wins (US-07)
- When the leader process is killed, the TTL expires and the follower acquires the lease within one TTL window (US-07)
- Lease renewal works: a live leader keeps the key alive across multiple TTL windows
- Alert state key `alert-state:{entity_id}` (value = `dark_since_ms`) prevents re-emission across multiple evaluation cycles while the entity stays dark, and is correctly deleted by the position consumer (not the evaluator) when the entity comes back online (US-03)
- Entity live key `entity:live:{entity_id}` expires automatically after **24h** (safety-net TTL — not used for alert detection or dashboard cleanup; validates that ghost keys do not accumulate permanently)
- Race condition test: 10 simultaneous lease acquisition attempts result in exactly 1 winner (US-07)

---

## Done When

- Race condition test passes: 10 simultaneous acquisition attempts result in exactly 1 winner
- Failover test passes: leader killed, follower takes over within configured TTL
- TTL value is chosen and documented with reasoning (trade-off: shorter TTL = faster failover but more renewal overhead)
- Alert suppression test passes: simulated evaluation loop emits exactly 1 alert for a signal loss event that spans 10 evaluation cycles
- Entity expiry test passes: key disappears automatically after the configured TTL with no delete call

---

## ADR Coverage

[ADR-005 - Leader Election for Alert Evaluator](../../adr/ADR-005-leader-election-alert-evaluator.md)

## Use Case Coverage

- [US-01](../../use-cases/US-01-live-entity-tracking/live-entity-tracking.md) - entity expiry
- [US-03](../../use-cases/US-03-signal-loss-alert/signal-loss-alert.md) - alert suppression
- [US-07](../../use-cases/US-07-duplicate-free-alerts/duplicate-free-alerts.md) - duplicate-free alerts
