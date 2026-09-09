# Phase 10 — Production Hardening + Failure Lab

## Goal

Standardize observability across the complete system and verify behavior under failure, replay, and representative load.

Observability has existed throughout earlier phases. This phase **completes and validates it system-wide**.

**No mandatory feature-UI checkpoint.** Every other user-facing phase in this roadmap (07–09) must reach a dedicated frontend checkpoint before it's considered done, per `IMPLEMENTATION_WORKFLOW.md`'s vertical-slice rule — this phase is the explicit exception, because it adds no new operator-facing capability. Its own "vertical slice" is end-to-end diagnosability and failure evidence, not a UI: the exit criteria below already express that (the developer can diagnose the system from operational signals, not "an operator can click a new button").

## Observability Completion

Standardize:

- structured JSON logs with service, level, timestamp, and useful correlation/entity/event identifiers
- meaningful state-transition logs
- error counters
- Kafka consumer lag visibility
- dependency-aware health/readiness endpoints
- operational commands/runbook notes

Avoid enterprise-observability overengineering; the goal is practical diagnosability.

## Failure Lab

Deliberately test Position Consumer crashes/replay, Redis failure/recovery, TimescaleDB outage, Neo4j outage and retry, Alert Evaluator leader failover, offset reset/replay, duplicate delivery, and multi-instance API failure/reconnect behavior.

## Load / Capacity Experiments

Use the load generator to measure ingestion throughput, consumer lag under burst, TimescaleDB write/query latency, H3 candidate density/correlation cost, Redis memory/key growth, and WebSocket fan-out behavior.

Tune implementation choices only from observed evidence.

## Exit Criteria

The developer can diagnose Sentinel from operational signals without reading code, explain important crash/replay scenarios, demonstrate idempotent durable effects, describe degradation boundaries, and defend measured bottlenecks/trade-offs.
