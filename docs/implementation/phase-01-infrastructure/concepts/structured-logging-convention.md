# Structured Logging Convention

Established in Phase 01 Checkpoint 6. Every Sentinel service must follow this contract from its first commit.

This document defines the log event shape, level semantics, contextual fields, error format, and safety rules. It does not prescribe a logging library or a central log backend.

---

## Goals

- Machine-parseable: every line is valid JSON.
- Human-understandable: fields are obvious without a schema reference.
- Consistent: the same field names carry the same meaning in every service.
- Traceable: Kafka context and Sentinel deterministic identities make it possible to follow an event through the pipeline without a distributed tracing system.
- Safe: secrets never appear in logs; PII is minimized and logged only deliberately when necessary.
- Container-native: logs go to stdout and are collected by the container runtime.

---

## 1. Mandatory fields

Every log event must contain at least these four fields:

| Field | Type | Description |
| --- | --- | --- |
| `timestamp` | string | UTC ISO-8601 with millisecond precision: `2026-08-14T22:00:00.000Z` |
| `level` | string | One of: `debug`, `info`, `warn`, `error` |
| `service` | string | Canonical service name (see section 8) |
| `message` | string | Short stable human-readable description of the event |

Example minimum log line:

```json
{"timestamp":"2026-08-14T22:00:00.000Z","level":"info","service":"position-consumer","message":"position persisted"}
```

---

## 2. Log level semantics

| Level | When to use |
| --- | --- |
| `debug` | Detailed diagnostic state: internal variable values, loop iterations, low-level protocol details. Not emitted in production by default. |
| `info` | Expected lifecycle and meaningful operational events: service started, Kafka message processed, alert emitted, migration applied. |
| `warn` | Unexpected but recoverable conditions: stale telemetry ignored, retry attempted, DLQ record produced, non-fatal configuration gap. Processing continues. |
| `error` | Operation failed or requires intervention: database write failed, Kafka publish failed after retries, schema validation rejected a record unrecoverably. |

Do not use `fatal`. Process-level failures may reach stderr before structured logging is available. Explicit process-level error handling will be defined when services are implemented.

---

## 3. Message field rule

`message` is a short stable human-readable event name. It should be consistent across invocations so that log search and alerting rules can match it reliably.

Good:

```text
position persisted
kafka message rejected
neo4j write failed
alert superseded
stale position ignored
deviation candidate published
lease acquired
lease lost
```

Bad: embedding structured values in the message string.

```text
// Wrong -- can't search for this reliably; values vary
"persisted entity abc123 from topic adsb.raw at offset 42"
```

```json
// Right -- message is stable; values are fields
{
  "message": "position persisted",
  "entity_id": "abc123",
  "topic": "adsb.raw",
  "offset": 42
}
```

Structured fields can be parsed and filtered reliably by JSON-aware tooling. Whether they are indexed depends on the eventual logging backend. A string that concatenates values cannot be parsed reliably by any tool. A stable message string also makes it easy to define metric counters or alert rules that do not break when an entity ID changes.

---

## 4. Optional contextual fields

Context fields are included when relevant to the event. Omit a field entirely when the context does not apply. Do not substitute `null`, `"unknown"`, or `"N/A"` for missing context.

### Kafka context

Include when the log relates to a Kafka record being processed:

| Field | Type | Description |
| --- | --- | --- |
| `topic` | string | Kafka topic name |
| `partition` | number | Partition index |
| `offset` | number | Record offset within the partition |
| `consumer_group` | string | Consumer group name |

`topic + partition + offset` uniquely identifies a specific record in the broker's log. This is the most precise pointer to the source event that caused a processing outcome.

`offset` is NOT a globally unique event ID. Offsets are per-partition. Two different topics can both have a record at offset 42 with no relationship between them.

### Entity context

| Field | Type | Description |
| --- | --- | --- |
| `entity_id` | string | ICAO hex, MMSI, or synthetic entity ID |
| `entity_type` | string | `aircraft` or `vessel` |

### Alert context

| Field | Type | Description |
| --- | --- | --- |
| `alert_id` | string | Deterministic alert identity (see DATA_MODEL.md) |
| `alert_type` | string | `SIGNAL_LOSS`, `ROUTE_DEVIATION`, `UNSCHEDULED_PROXIMITY`, `COMPOSITE` |

### Proximity / correlation context

| Field | Type | Description |
| --- | --- | --- |
| `pair_key` | string | `min(a,b):max(a,b)` canonical pair identity |
| `idempotency_key` | string | `{pair_key}:{episode_start_ms}` episode identity |
| `episode_start_ms` | number | Source event time for episode start |

### Signal-loss context

| Field | Type | Description |
| --- | --- | --- |
| `dark_since_ms` | number | Source event time when entity went dark |

### Route-deviation context

| Field | Type | Description |
| --- | --- | --- |
| `route_id` | string | Reference route identity |

### API / operator context

| Field | Type | Description |
| --- | --- | --- |
| `user_id` | string | UUID from `users.user_id`; prefer over email |

---

## 5. Error shape

When logging at `error` level, include a structured `error` field rather than embedding the error message in the `message` string.

```json
{
  "timestamp": "2026-08-14T22:00:02.000Z",
  "level": "error",
  "service": "correlation-worker",
  "message": "neo4j write failed",
  "pair_key": "abc123:def456",
  "idempotency_key": "abc123:def456:1700000000000",
  "error": {
    "name": "Neo4jError",
    "message": "connection unavailable",
    "code": "ServiceUnavailable"
  }
}
```

Fields within `error`:

| Field | Required | Description |
| --- | --- | --- |
| `name` | yes | Error class name |
| `message` | yes | Error message string |
| `code` | no | Provider/library error code if available |
| `stack` | no | Stack trace — debug environments only; omit in production |

Stack traces in production add noise and may expose internal file paths. The convention is: include `stack` at `debug` level or during active local troubleshooting; omit it at `error` level in production unless the service is configured explicitly to include it.

---

## 6. Secrets and PII rules

### Never log

```text
Passwords
NEO4J_AUTH (or any parsed credential from it)
Database connection strings containing credentials
JWTs (application or Google ID tokens)
OAuth access tokens or refresh tokens
Authorization header values
Cookie values or session tokens
API keys
Raw secret values from environment variables
```

### PII

User `email` is PII. It must not appear as a default log context field. Use `user_id` (UUID) for operator context in logs. If email is ever logged for a specific debugging purpose, it must be deliberate, targeted, and not committed as a permanent log statement.

### Raw provider payloads

DLQ records persist `raw_payload` per DATA_MODEL.md. Log files must not duplicate full raw provider payloads at `info`/`warn`/`error` level. When a record is rejected, log the identifying metadata and rejection reason:

```json
{
  "level": "warn",
  "service": "position-consumer",
  "message": "record sent to dlq",
  "topic": "adsb.raw",
  "partition": 0,
  "offset": 182,
  "rejection_reason": "missing required field: lat"
}
```

Not:

```json
{
  "level": "warn",
  "message": "record sent to dlq",
  "raw_payload": "...entire provider response..."
}
```

---

## 7. Format and runtime conventions

### JSON Lines

Each log event is one complete JSON object on a single line (JSON Lines / NDJSON). No pretty-printed multiline JSON at runtime. Container log collectors treat each line as one log event; multiline JSON breaks that expectation.

### stdout / stderr

All application log events go to **stdout**.

**stderr** is reserved for process-level output that the runtime captures before the logger is initialized: uncaught exceptions, unhandled rejections, startup failures that abort before the logger is ready. Do not write application business-logic logs to stderr.

### No local log files

Services must not write log files inside containers. The container runtime collects stdout. Future production infrastructure (CloudWatch, Loki, etc.) ingests from the container log stream. Services are not aware of the destination.

---

## 8. Canonical service names

These are the values the `service` field must use, derived from ARCHITECTURE.md:

| Service | `service` value |
| --- | --- |
| Ingestion Poller | `ingestion-poller` |
| Position Consumer | `position-consumer` |
| Deviation Detector | `deviation-detector` |
| Correlation Worker | `correlation-worker` |
| Alert Evaluator | `alert-evaluator` |
| API | `api` |

---

## 9. Representative examples

### Normal Kafka processing

```json
{"timestamp":"2026-08-14T22:00:00.000Z","level":"info","service":"position-consumer","message":"position persisted","entity_id":"A1B2C3","entity_type":"aircraft","topic":"adsb.raw","partition":0,"offset":18442}
```

### Stale telemetry ignored

```json
{"timestamp":"2026-08-14T22:00:01.000Z","level":"warn","service":"position-consumer","message":"stale position ignored","entity_id":"A1B2C3","incoming_timestamp_ms":1700000000000,"current_timestamp_ms":1700000005000,"topic":"adsb.raw","partition":0,"offset":18443}
```

### Record sent to DLQ

```json
{"timestamp":"2026-08-14T22:00:01.500Z","level":"warn","service":"position-consumer","message":"record sent to dlq","topic":"adsb.raw","partition":0,"offset":18444,"rejection_reason":"missing required field: lat"}
```

### Neo4j write failed

```json
{"timestamp":"2026-08-14T22:00:02.000Z","level":"error","service":"correlation-worker","message":"neo4j write failed","pair_key":"A1B2C3:D4E5F6","idempotency_key":"A1B2C3:D4E5F6:1700000000000","error":{"name":"Neo4jError","message":"connection unavailable","code":"ServiceUnavailable"}}
```

### Proximity candidate published

```json
{"timestamp":"2026-08-14T22:00:03.000Z","level":"info","service":"correlation-worker","message":"proximity candidate published","pair_key":"A1B2C3:D4E5F6","idempotency_key":"A1B2C3:D4E5F6:1700000000000","episode_start_ms":1700000000000}
```

### Signal loss detected

```json
{"timestamp":"2026-08-14T22:00:04.000Z","level":"info","service":"alert-evaluator","message":"signal loss alert emitted","entity_id":"A1B2C3","entity_type":"aircraft","alert_id":"A1B2C3:SIGNAL_LOSS:1699999500000","dark_since_ms":1699999500000}
```

### Alert acknowledged

```json
{"timestamp":"2026-08-14T22:00:05.000Z","level":"info","service":"api","message":"alert acknowledged","alert_id":"A1B2C3:SIGNAL_LOSS:1699999500000","user_id":"550e8400-e29b-41d4-a716-446655440000"}
```

### Alert Evaluator lease acquired

```json
{"timestamp":"2026-08-14T22:00:06.000Z","level":"info","service":"alert-evaluator","message":"leader lease acquired"}
```

---

## 10. Deferred

The following observability concerns are intentionally not decided in Phase 01:

**Logging library:** Pino and Winston are both reasonable choices for Node.js. Selection deferred to Phase 02 when the first service is implemented. The choice affects initialization code, not this field contract.

**Universal request/trace/correlation ID:** Sentinel's existing deterministic identities (`entity_id`, `alert_id`, `pair_key`, `idempotency_key`) plus Kafka `topic+partition+offset` are sufficient to trace events through the pipeline for all documented flows. A HTTP `request_id` belongs on the API layer (Phase 03) where HTTP requests exist. A distributed `trace_id` requires a tracing backend and sampling strategy; deferred to Phase 10.

**instance_id field:** Relevant for debugging which Alert Evaluator instance holds the lease. Not mandatory on every log. The Alert Evaluator will introduce this as optional context when it is implemented (Phase 03).

**environment and version fields:** Useful in multi-environment production deployments. Deferred until deployment environments are configured.

**Central log backend:** CloudWatch, Loki, Elasticsearch, or other aggregation destination. The convention is backend-independent. Deferred to Phase 10.

**Log sampling and rate limiting:** High-throughput paths (every position normalized, every H3 cell lookup) may need `debug`-level sampling in production. Deferred to Phase 10 when load is understood.

**Metrics and error counters:** Phase 10 adds counters for important failure paths. Phase 01 establishes the log events they will be derived from.

**HTTP request logging middleware:** Express request/response logging belongs in Phase 03 when the API is built.
