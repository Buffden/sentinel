# US-03: Signal Loss Alert

**Actor:** Operator
**Status:** Defined

---

## Story

As an operator, I want to receive an alert when an entity's transponder goes dark beyond a configurable time threshold so that I can investigate a potential signal loss event.

---

## Acceptance Criteria

- The system detects when an entity has not broadcast a position update within `SIGNAL_LOSS_THRESHOLD_MS`
- An alert is emitted with the entity ID, last known position, and time since last update
- The threshold is configurable per entity type (ADS-B and AIS have different expected broadcast intervals)
- The alert is not re-emitted on every evaluation cycle once it has been raised - only once per loss event

---

## Flow Diagrams

### Detection

![Detection](../../../diagrams/docs/use-cases/US-03-signal-loss-alert/detection.svg)

The alert evaluator runs on a fixed schedule (every 10s), scans all `entity:live:*` keys in Redis, and checks the `last_seen_ms` field in each hash. Any entity where `now() - last_seen_ms > SIGNAL_LOSS_THRESHOLD_MS` is flagged. The evaluator then fetches the last known position from TimescaleDB and emits an alert to Kafka. Detection is driven by `last_seen_ms`, not by Redis TTL expiry. The key carries a 24h TTL (safety-net only) — it is deliberately longer than `SIGNAL_LOSS_THRESHOLD_MS` so the key cannot expire between scan cycles before the evaluator has a chance to inspect `last_seen_ms`. Dashboard ghost cleanup is separate and client-side (US-01).

### Alert Delivery

![Alert Delivery](../../../diagrams/docs/use-cases/US-03-signal-loss-alert/alert-delivery.svg)

The API instance that consumes the alert from Kafka writes it to the `alerts` table in TimescaleDB (idempotent on replay), then publishes it to the `alert-events` Redis pub/sub channel. All API instances subscribe to `alert-events` and fan out to scope-matched WebSocket connections. This mirrors the `position-updates` pattern and solves the fan-out problem: the Kafka consumer group `api` delivers each alert to exactly one instance, but WebSocket connections are local per instance — without the Redis broadcast, users on non-consuming instances would never receive the push.

### Alert Suppression

![Alert Suppression](../../../diagrams/docs/use-cases/US-03-signal-loss-alert/alert-suppression.svg)

Once an alert is raised for an entity, the alert evaluator writes an `alert-state:{entity_id}` key to Redis (no TTL). Subsequent evaluation cycles check for this key first and skip re-emission if it is present. When the entity comes back online, the position consumer explicitly deletes `alert-state:{entity_id}` on its next write, clearing the suppression. This is distinct from the durable dedup in the `alerts` table (TimescaleDB), which handles Kafka replay idempotency — not in-loop suppression.

---

## Architectural Justification

Justifies: [ADR-002 - TimescaleDB for Position History](../../adr/ADR-002-timescaledb-over-cassandra.md)

Signal loss detection requires comparing the latest known position timestamp against the current time across all tracked entities. This is a time-range query on historical position data - the exact access pattern TimescaleDB hypertables are optimised for. Redis holds the most recent position per entity (US-01), enabling the alert evaluator to check last-seen timestamps without hitting TimescaleDB on every evaluation cycle.
