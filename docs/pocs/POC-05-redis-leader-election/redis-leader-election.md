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

- Two Node.js processes both attempt to acquire the `alert-evaluator:leader` key simultaneously — exactly one wins (US-07)
- When the leader process is killed, the TTL expires and the follower acquires the lease within one TTL window (US-07)
- Lease renewal works: a live leader keeps the key alive across multiple TTL windows using compare-and-renew (`SET XX PX`)
- Lease renewal correctly fails when the key has been acquired by a new leader (prevents a lagging slow leader from extending a stale lease)
- Lease release on clean shutdown uses compare-before-DEL (Lua script): does NOT delete the key if a new leader has already acquired it
- **Leadership transition scenario:** messages arrive on `proximity.candidates` during the gap between the old leader stopping consumption and the new leader starting; verify the new leader processes them and emits exactly one alert per event, not zero or two
- **Crash-after-process scenario:** leader processes an event and publishes to Kafka `alerts` but crashes before committing the Kafka offset; new leader re-processes the event; verify `ON CONFLICT DO NOTHING` prevents duplicate alert insertion
- **Lease loss mid-evaluation:** leader starts evaluating a signal loss batch, lease expires before the batch completes; verify the leader stops and the new leader re-evaluates without duplicate alerts
- Alert state key `alert-state:{entity_id}` (hash: `dark_since_ms`, `signal_loss_alert_id`) prevents re-emission across evaluation cycles; deleted by position consumer (not evaluator) when entity resumes (US-03)
- Entity live key `entity:live:{entity_id}` expires automatically after **24h** (safety-net TTL — not used for alert detection; validates ghost keys do not accumulate permanently)
- Race condition test: 10 simultaneous lease acquisition attempts result in exactly 1 winner

---

## Done When

- Race condition test passes: 10 simultaneous acquisition attempts result in exactly 1 winner
- Failover test passes: leader killed, follower takes over within configured TTL
- Compare-before-DEL test passes: slow-shutting leader does NOT delete the new leader's lease
- Compare-and-renew test passes: slow leader fails renewal after failover; does NOT extend the new leader's lease
- Leadership transition test: no event lost, no duplicate alert during leader changeover
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
