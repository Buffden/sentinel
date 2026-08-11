# Phase 07 — Alert Lifecycle

## Goal

Implement operator alert state and real-time delivery to all connected dashboard instances.

Implement:

- durable alert lifecycle in TimescaleDB (`NEW` → `ACKNOWLEDGED` → `RESOLVED` → `SUPERSEDED`)
- API Kafka consumer: `alerts` topic → TimescaleDB INSERT ON CONFLICT DO NOTHING → `alert-events` pub/sub → offset commit
- `PATCH /alerts/:alert_id` — acknowledge and resolve; publishes `ALERT_STATUS_CHANGED` to `alert-events`
- composite supersession: atomic transaction — INSERT COMPOSITE + UPDATE referenced alert to `SUPERSEDED`
- `alert-events` Redis pub/sub fan-out to all API instances
- WebSocket delivery to scope-matched connections on every API instance

---

## Learning Goals

- Kafka consumer groups and single-consumer guarantee
- why the API cannot push directly from the consuming instance to all WebSocket clients
- Redis pub/sub as a cross-instance fan-out mechanism
- atomic DB transactions for supersession
- idempotent Kafka replay via `ON CONFLICT DO NOTHING` + pub/sub re-publish after no-op inserts

---

## Key Experiment: Multi-Instance Fan-Out

Run two API instances.

Connect a WebSocket client to each.

Trigger an alert.

Confirm both clients receive the alert even though only one instance consumed it from Kafka.

This is the core distributed behavior this phase must demonstrate.

---

## Kafka Commit Ordering

The correct order is:

1. write to TimescaleDB (`INSERT ON CONFLICT DO NOTHING`)
2. publish to `alert-events` pub/sub (always — even on a no-op insert, ensures WebSocket delivery after replay)
3. commit Kafka offset

Committing before publishing means a crash between commit and publish permanently loses the WebSocket push. The dashboard deduplicates by `alert_id`, so a replayed pub/sub event is harmless.

Test this deliberately: crash the API instance after step 1 but before step 3. Confirm the event is re-published and delivered on restart.

---

## Exit Criteria

- alert status transitions work and persist correctly in TimescaleDB
- composite supersession atomically marks the referenced alert as `SUPERSEDED`
- all WebSocket clients on all instances receive alert events regardless of which instance consumed from Kafka
- Kafka replay does not produce duplicate rows or duplicate WebSocket deliveries (dashboard deduplicates by `alert_id`)
