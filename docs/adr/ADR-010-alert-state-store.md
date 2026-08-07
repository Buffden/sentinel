# ADR-010: Alert State Store for Lifecycle Management

**Status:** Accepted
**Date:** 2026-08-07

---

## Context

The alert evaluator publishes alert events to the `alerts` Kafka topic. Currently, alerts are stateless from the system's perspective - an operator sees them on the dashboard but cannot acknowledge, resolve, or suppress them. To support alert lifecycle management (US-13), alert state must be persisted somewhere.

Requirements:
- Store per-alert state: `NEW`, `ACKNOWLEDGED`, `RESOLVED`
- State must survive restarts - losing alert history when Redis restarts is not acceptable
- State must be queryable: filter by status, entity, time range, alert type
- Writes are low-volume relative to position pings (thousands of alerts vs millions of pings)
- The same alert must not create duplicate records under Kafka replay

---

## Decision

Store alert lifecycle state in a regular PostgreSQL table on the existing TimescaleDB instance. Not a hypertable - alerts are not a high-volume time-series write and do not need chunk-based sharding.

Alert records are written by the API layer when it consumes from the `alerts` Kafka topic. Alert state transitions (acknowledge, resolve) are applied via API endpoints (`PATCH /alerts/:alert_id`).

The `alert_id` is a deterministic key of the form `{entity_id}:{alert_type}:{window_start_ms}`, derived from the alert event. This makes the initial insert idempotent under Kafka replay (`INSERT ... ON CONFLICT DO NOTHING`).

---

## Reasoning

**Durability is required.** Redis holds alert state only until restart (without AOF/RDB persistence). Operators must be able to see and update alert state across restarts. A relational table survives restarts by definition.

**Queryability matters.** An operator viewing an entity's alert history needs filtering by status, time range, and alert type. Redis has no native secondary index - filtering would require scanning all keys or maintaining separate index sets. SQL handles this natively.

**No new infrastructure.** TimescaleDB is already running as a PostgreSQL-compatible instance. Adding an `alerts` table to the same instance reuses existing connections, backup procedures, and operational tooling. Adding a dedicated relational database (e.g. RDS PostgreSQL) for alert state alone is not justified at this scale.

**Low write volume.** Alerts are rare relative to position pings. The case for a time-series store or write-optimised store does not apply here. A plain table with a primary key and a few indexes is sufficient.

**Deterministic alert_id enables idempotent writes.** `{entity_id}:{alert_type}:{window_start_ms}` is derived from the source alert event, so replaying the `alerts` Kafka topic produces the same alert_id and the duplicate insert is a no-op.

---

## Alternatives Considered

### Redis (rejected)
- Fast and already in the stack, but volatile without persistence configuration
- Alert history would be lost on restart unless RDB or AOF is enabled - this adds operational complexity and still provides weaker durability guarantees than a proper database
- No native secondary indexes - querying "all ACKNOWLEDGED alerts for entity X" requires application-level filtering or extra Redis data structures
- Redis is the right store for live entity state (high frequency, acceptable to lose on restart); it is the wrong store for mutable, durable, queryable records

### Dedicated PostgreSQL instance (rejected)
- Correct choice if alert volume were high enough to justify isolation
- Adds a new infrastructure component for a low-volume, low-complexity table
- TimescaleDB is PostgreSQL - the existing instance handles this without any schema migration tooling changes

### Kafka as store - read alert state from topic (rejected)
- Replaying the topic gives the sequence of events but not the current state without building a projection
- State transitions (acknowledge, resolve) would require compacted topics or a CQRS projection layer
- Adds significant complexity for a problem that a two-column SQL update solves cleanly

### Separate alert-state microservice (rejected)
- Each service owns exactly one concern; alert state management is a natural API layer concern since the API is already the entry point for operator interactions
- A dedicated service adds a deployment unit, a network hop, and a failure domain for no architectural gain at this scale

---

## Consequences

- The API layer consumes from the `alerts` Kafka topic and writes initial alert records with status `NEW` to the `alerts` table
- Alert state transitions are applied via `PATCH /alerts/:alert_id` endpoints - the API updates the table directly
- The `alert_id` format `{entity_id}:{alert_type}:{window_start_ms}` must be included in every alert event published by the alert evaluator
- A new `alerts` table must be added to the TimescaleDB schema alongside `position_history` and `route_baseline`
- Alert history is queryable via the API without re-reading Kafka - the table is the source of truth for current alert state
