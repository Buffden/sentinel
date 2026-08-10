# Phase 09 — Observability + Failure Injection

## Goal

All services emit structured logs, expose health endpoints, and publish metrics so that the system's behaviour is visible under normal operation and diagnosable under failure. Failure injection scenarios validate the documented failure modes.

## Dependencies

- Phase 08 (entity investigation — all core services running)

---

## Tasks

### Structured Logging

All services emit JSON-formatted logs. Log entries include the following fields where applicable:

```json
{
  "service":         "string",
  "event_id":        "string",
  "entity_id":       "string",
  "pair_key":        "string",
  "alert_id":        "string",
  "kafka_topic":     "string",
  "partition":       "number",
  "offset":          "number",
  "correlation_id":  "string",
  "level":           "info | warn | error",
  "message":         "string",
  "ts":              "ISO8601"
}
```

Fields omitted when not applicable. `correlation_id` threads a request or event through multiple services where possible.

### Health Endpoints

Each service exposes:

- `GET /health/live` — returns 200 immediately if the process is running
- `GET /health/ready` — returns 200 only when all dependencies relevant to that service are reachable

| Service | Readiness checks |
|---|---|
| Ingestion Poller | Kafka broker reachable |
| Position Consumer | Kafka consumer group joined; TimescaleDB writable; Redis writable |
| Correlation Worker | Kafka consumer group joined; Redis readable; Neo4j writable |
| Deviation Detector | Kafka consumer group joined; TimescaleDB readable |
| Alert Evaluator | Redis readable/writable; leader lease status (leader: Kafka group joined) |
| API | TimescaleDB readable; Redis readable; Neo4j readable |

### Metrics

Services expose metrics in Prometheus text format at `GET /metrics`. Priority metrics:

**Ingestion Poller:**
- `opensky_requests_total{status}` — HTTP status of each OpenSky poll
- `opensky_credits_remaining` — gauge from response header
- `raw_events_published_total{topic}` — events forwarded to Kafka
- `states_without_position_total` — state vectors where `time_position == null` (not malformed — logged, not DLQ'd)

**Position Consumer:**
- `normalization_failures_total{reason}` — events routed to DLQ by rejection reason
- `dlq_events_total{topic}` — events on each DLQ topic
- `position_history_inserts_total` — TimescaleDB inserts
- `redis_live_state_writes_total` — Redis hash writes
- `timestamp_guard_rejections_total` — events skipped by the timestamp guard

**Correlation Worker:**
- `proximity_episode_started_total` — new proximity episodes detected
- `proximity_episode_refreshed_total` — episode TTL refreshed (entity pair still close)
- `stale_geo_candidates_rejected_total` — candidates discarded by `last_seen_ms` recheck
- `neo4j_write_failures_total` — failed MERGE operations
- `kafka_publish_failures_total{topic}` — failed Kafka publishes after retry

**Deviation Detector:**
- `deviation_candidates_total{status}` — OUT_OF_RANGE and IN_RANGE events emitted

**Alert Evaluator:**
- `signal_loss_scan_duration_ms` — histogram of scan cycle duration
- `signal_loss_alerts_total` — SIGNAL_LOSS alerts emitted
- `deviation_alerts_total` — ROUTE_DEVIATION alerts emitted
- `proximity_alerts_total{type}` — UNSCHEDULED_PROXIMITY and COMPOSITE alerts emitted
- `alerts_superseded_total` — SIGNAL_LOSS alerts superseded by COMPOSITE
- `leader_changes_total` — leader election transitions
- `leader_lease_renewal_failures_total` — failed compare-and-renew attempts
- `kafka_consumer_lag{topic,partition}` — lag of alert-evaluator consumer group

**API:**
- `websocket_connections` — gauge of currently connected WebSocket clients
- `websocket_events_delivered_total{type}` — events pushed to clients
- `websocket_events_filtered_total{reason}` — events suppressed by scope filter
- `alerts_created_total{type}` — alerts written to TimescaleDB by type

### Failure Injection Scenarios

Validate the documented failure modes from each ADR. Do not claim a mode is handled without a passing test scenario.

| Scenario | Expected behaviour |
|---|---|
| Kafka temporarily unavailable | Ingestion Poller buffers in producer; Position Consumer stops committing offsets; services log `kafka_publish_failures_total` increase; no data loss on Kafka recovery |
| Neo4j unavailable during proximity processing | Correlation Worker skips Neo4j write + skips Kafka publish; logs failure; proximity candidate is NOT emitted; re-detection expected on next ping |
| Redis restart | Leader election re-acquires within TTL; `entity:live:*` re-warms from Kafka replay or next position pings; alert evaluator does not emit duplicate alerts on re-warm |
| Alert Evaluator leader crash | TTL expires; follower acquires within one TTL window; no alert gap > 1 TTL; no duplicate from reprocessed events (deterministic alert_id + ON CONFLICT DO NOTHING) |
| API instance crash with WebSocket clients | Clients reconnect to any remaining instance; WebSocket state restored from `user_workspaces` on reconnect |
| Duplicate Kafka delivery | TimescaleDB `ON CONFLICT DO NOTHING` silently drops duplicate; Neo4j `MERGE` is a no-op; Redis timestamp guard prevents state regression |
| Out-of-order replay | Timestamp guard prevents older events from regressing Redis live state; TimescaleDB idempotent |
| Stale H3 membership | `ZRANGEBYSCORE` score filter excludes old members; `last_seen_ms` recheck on `entity:live:*` fetch discards any remaining stale candidates |
| OpenSky 429 response | Ingestion Poller backs off per `Retry-After` header; logs `opensky_requests_total{status=429}`; no crash or tight retry loop |
| Lease loss mid-evaluation | Leader stops evaluation immediately on failed compare-and-renew; in-progress alert may be emitted or dropped; deterministic alert_id ensures no duplicate if emitted then replayed |

## Done When

- All services start with health endpoints responding correctly
- `GET /health/ready` returns 503 when a dependency is unavailable (tested by stopping Redis, Kafka, TimescaleDB in turn)
- All priority metrics emit non-zero values during a test pipeline run
- All failure injection scenarios pass with documented behaviour matching the expected column above
- A Prometheus scrape config is documented in `docs/DEVELOPMENT.md` (Grafana optional)
