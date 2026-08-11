# Phase 08 — Alert Lifecycle + Distributed Fan-Out

## Goal

Complete operator alert state transitions and make real-time alert delivery correct across multiple API instances.

## Alert State Model

`RESOLVED` and `SUPERSEDED` are terminal outcomes; `SUPERSEDED` is not a step that occurs after `RESOLVED`.

```text
NEW → ACKNOWLEDGED → RESOLVED
 │         │
 └─────────┴────→ SUPERSEDED
```

A COMPOSITE may supersede an active individual alert directly when correlation determines the composite represents the higher-level incident.

## What to Build

- `PATCH /alerts/:alert_id` for acknowledge/resolve
- audit fields and timestamps
- atomic COMPOSITE insert + referenced-alert `SUPERSEDED` update
- Redis `alert-events` pub/sub
- all API instances subscribe and fan out to their local WebSocket connections
- client deduplication by `alert_id` and event type/version semantics as needed

## Delivery Semantics

Kafka-to-database processing creates **idempotent durable effects**, but WebSocket delivery is **at least once**, not exactly once.

Correct consumed-alert ordering:

1. persist idempotently in TimescaleDB
2. publish `alert-events`
3. commit Kafka offset

A crash before offset commit can cause replay and another WebSocket push. This is acceptable; clients deduplicate. The system must never claim replay guarantees zero duplicate WebSocket deliveries.

## Key Experiment

Run two API instances with one WebSocket client on each. Trigger an alert and verify both receive it even though only one API group member consumed Kafka. Then crash after DB persistence but before offset commit and verify no duplicate durable row while duplicate push remains harmless.

## Exit Criteria

Status transitions persist correctly, supersession is atomic, all API instances can notify local clients, Kafka replay produces no duplicate durable alert rows, and WebSocket semantics are explicitly at-least-once.
