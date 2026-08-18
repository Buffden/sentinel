# Health and Readiness Convention

Established in Phase 01 Checkpoint 7. Every Sentinel service must follow this contract from its first implementation.

This document defines what "live" and "ready" mean, which dependency states affect each, how health is exposed over HTTP, how graceful shutdown interacts with health, and what is deferred to later phases.

---

## Goals

- Make process health observable without a service mesh or external monitoring agent.
- Prevent unnecessary process restarts when a downstream service is temporarily unavailable.
- Allow orchestrators and load balancers to stop routing work to a service that is starting up, shutting down, or unable to process.
- Align with the CP6 logging convention so health transitions emit structured log events.

---

## 1. The two probes

Sentinel uses two distinct probes per service. They must never be conflated.

### Liveness

**Question:** Is this process alive and able to continue running?

**Answer:** Yes if the HTTP health server responds. No other check is performed.

A transient Kafka broker outage, a momentarily unreachable TimescaleDB, or a Redis connection drop does NOT make a process "not live." Restarting a process because a downstream service is unavailable destroys in-flight work, loses active consumer-group membership and triggers a partition rebalance (committed offsets remain stored in Kafka, but the reconnect cost is real), and forces cold reconnection to every dependency. Retrying at the application level is always preferable.

### Readiness

**Question:** Is this process ready to accept or process work?

**Answer:** Readiness reflects the service's current known ability to perform its core work. It is derived from:

- whether startup has completed;
- the current state of required dependency clients (connected, reconnecting, or known-failed);
- any known failures or recoveries observed since startup;
- whether a graceful shutdown has been initiated.

Readiness is maintained as internal state and updated as these factors change. It is NOT computed by querying dependencies on every `/health/ready` request — that would add latency and create a dependency on the health check path itself.

---

## 2. HTTP endpoints

Every Sentinel service exposes:

```text
GET /health/live
GET /health/ready
```

These are the only unauthenticated endpoints on every service. The API additionally has `/auth/google` (per ADR-011).

### Response shape

**Healthy — HTTP 200:**

```json
{"status": "ok"}
```

**Not ready — HTTP 503:**

```json
{"status": "not ready", "reason": "timescaledb unreachable"}
```

The `reason` field is present on 503 responses only. It should identify the failed dependency or state, not expose internal details or secrets.

**Not live** is not a meaningful state: if the process cannot respond to the liveness endpoint, it is already dead from the orchestrator's perspective. There is no 503 body for liveness — the endpoint either responds 200 or does not respond.

---

## 3. Non-HTTP workers

Any Sentinel service that does not have a business HTTP server must start a **minimal dedicated health HTTP server** on a configurable port to expose `/health/live` and `/health/ready`. This includes the Ingestion Poller and all Kafka consumer services (Position Consumer, Deviation Detector, Correlation Worker, Alert Evaluator).

Environment variable convention:

```text
HEALTH_PORT=<port>
```

The health server has no authentication. It serves only the two health endpoints and nothing else.

This approach:

- is compatible with Docker Compose `healthcheck` directives;
- costs essentially nothing (a single-route HTTP server);
- enables the same health contract across all services regardless of type.

---

## 4. Readiness by service

Readiness requires all of the following to be true simultaneously:

| Service | Readiness requires |
| --- | --- |
| Ingestion Poller | Kafka producer connected. Upstream ADS-B/AIS provider unavailability does not make the process not-ready; it remains available to retry fetching while Kafka is the critical downstream dependency. |
| Position Consumer | Kafka consumer group assigned (adsb.raw, ais.raw), TimescaleDB reachable, Redis reachable |
| Deviation Detector | Kafka consumer group assigned (position.normalized), TimescaleDB reachable |
| Correlation Worker | Kafka consumer group assigned (position.normalized), Redis reachable, Neo4j reachable |
| Alert Evaluator | Redis reachable, TimescaleDB reachable, Kafka client initialized, lease acquire loop running |
| API | HTTP server bound, Kafka consumer group assigned (alerts), TimescaleDB reachable, Redis reachable. Neo4j unavailability degrades investigation/evidence reads but does not make the API not-ready; core alert persistence, auth, and live-state functionality do not require Neo4j. |

"Connected" means the dependency client has successfully established a connection and the service has not observed a subsequent unrecovered failure. A dependency that was connected at startup but has since entered a known-failed state flips readiness to false. A dependency that reconnects after a transient outage may flip readiness back to true. The exact reconnection and readiness-recovery logic is an implementation choice per service.

### Alert Evaluator note

A standby Alert Evaluator (one that has not yet acquired the leader lease) is **live and ready** once its required dependencies are reachable and the lease acquire loop is running. Per ARCHITECTURE.md, the Alert Evaluator uses Redis (live state, lease, episode state), TimescaleDB (last-known position for signal-loss payload), and Kafka (candidate consumption and alert publication when active). A standby instance does not join the Kafka consumer group until it acquires the lease, but it must have its Kafka client initialized and its store connections established so it can take over without delay.

Whether the instance currently holds the lease is operational state, not health state. Exposing lease ownership via the health endpoint would conflate two different concerns and complicate load-balancer configuration.

---

## 5. Startup sequence and readiness

During startup:

```text
process starts
  -> /health/live returns 200 immediately
  -> /health/ready returns 503

attempt required dependency connections (with implementation-defined backoff)
  -> connection succeeds: update internal readiness state, continue retrying others
  -> all required connections established: /health/ready returns 200

if a dependency remains unreachable
  -> /health/live continues to return 200
  -> /health/ready continues to return 503
  -> service retries/reconnects with implementation-defined backoff
  -> /health/ready returns 200 once the dependency is reachable
```

Transient dependency unavailability during startup is not fatal. The service stays live and not-ready, retrying with backoff, until dependencies are reachable.

**Fatal startup conditions** are limited to non-recoverable configuration or environment errors:

- required environment variable missing or unparseable;
- health HTTP server fails to bind its port;
- invalid static configuration that cannot be corrected without a redeploy.

In these cases the service logs at `error` level and exits non-zero, because retrying cannot resolve the problem.

---

## 6. Graceful shutdown

On `SIGTERM`:

```text
SIGTERM received
  -> /health/ready returns 503 immediately
  -> log: {"level": "info", "service": "...", "message": "shutdown initiated"}
  -> finish in-flight work (bounded drain window)
  -> close dependency connections
  -> exit 0
```

`/health/live` continues to return 200 throughout the drain window so the orchestrator does not forcibly kill the process while it is flushing work. Liveness only becomes irrelevant after the process exits.

The drain window duration is an implementation choice per service. Phase 1 does not set specific values.

---

## 7. What must NOT cause a process crash

These conditions must be handled at the application level without exiting:

- Transient Kafka broker unavailability (consumer/producer retry)
- Individual record processing failure (DLQ or skip with logging)
- Transient TimescaleDB write failure (retry)
- Transient Redis command failure (retry for writes; tolerate for reads where contract allows)
- Transient Neo4j write failure (retry)
- Transient Neo4j read failure for non-KNOWN_ASSOCIATE queries (retry)
- DLQ produce failure: log at error; the source Kafka offset must NOT be committed until the DLQ record is durably produced or the failure is explicitly handled — silently skipping a failed DLQ publish loses the record entirely. Exact retry/pause/offset mechanics are deferred to the service that owns DLQ production.

A process that exits on any transient downstream failure would be continuously restarted by Docker/orchestration, losing active consumer-group membership, triggering a rebalance, and forcing cold reconnection to every dependency.

### KNOWN_ASSOCIATE read failure: fail-closed

A failed Neo4j KNOWN_ASSOCIATE read in the Correlation Worker must NOT be treated as "no relationship found." The Correlation Worker cannot safely classify a pair as unscheduled if it cannot determine their relationship status. Treating an error as "no associate" would risk publishing proximity candidates for pairs that are actually known associates, violating the filtering contract in ARCHITECTURE.md.

The Correlation Worker must fail-closed on KNOWN_ASSOCIATE read errors: do not publish a `proximity.candidates` event for the affected pair until the read succeeds. Exact retry/pause behavior is deferred to Phase 05 when the Correlation Worker is implemented.

---

## 8. Health log events

Health transitions must emit structured log events following the CP6 convention.

**Service ready:**

```json
{"timestamp": "...", "level": "info", "service": "position-consumer", "message": "service ready"}
```

**Shutdown initiated:**

```json
{"timestamp": "...", "level": "info", "service": "position-consumer", "message": "shutdown initiated"}
```

**Dependency not yet reachable (retrying):**

```json
{"timestamp": "...", "level": "warn", "service": "position-consumer", "message": "dependency not yet reachable", "reason": "timescaledb connection refused", "attempt": 3}
```

**Non-recoverable startup failure (service exits):**

```json
{"timestamp": "...", "level": "error", "service": "position-consumer", "message": "startup failed", "error": {"name": "ConfigurationError", "message": "required environment variable POSTGRES_PASSWORD is not set"}}
```

The `attempt` field is optional context; include it when it adds debugging value.

---

## 9. Docker Compose healthcheck pattern

When a service is added to `docker-compose.yml`, its healthcheck should target the liveness endpoint:

```yaml
healthcheck:
  test: ["CMD-SHELL", "curl -sf http://localhost:$${HEALTH_PORT}/health/live || exit 1"]
  interval: <implementation choice>
  timeout: <implementation choice>
  retries: <implementation choice>
  start_period: <implementation choice>
```

Specific `interval`, `timeout`, `retries`, and `start_period` values are deferred to implementation; they depend on how quickly each service starts and reconnects.

Use the liveness endpoint for Docker Compose healthchecks. Readiness is for load balancers and orchestrators that route work; liveness indicates whether the process is responsive. Docker Compose reports an `unhealthy` status when the liveness check fails, but does not automatically restart the container unless a restart policy is configured separately.

`curl -sf` returns non-zero on HTTP error or connection failure. No host-side `curl` is required; use `CMD-SHELL` with tooling available inside the container image.

---

## 10. Deferred

**Ongoing dependency polling:** Whether readiness should continuously re-check dependency connectivity after startup is not decided in Phase 1. Phase 10 may add periodic dependency health checks if operational evidence shows they are needed.

**Kubernetes liveness/readiness/startup probes:** Sentinel runs locally with Docker Compose. Kubernetes probe configuration (separate startup probe, probe failure thresholds, period/timeout tuning) is a Phase 10 production concern.

**Centralized health aggregation:** No service registry or aggregated health endpoint exists in Phase 1. Inspecting individual service health requires checking each container's health endpoint directly.

**Circuit breakers:** Automatic dependency circuit-breaking (open/half-open/closed states) belongs in Phase 10 if needed after load testing.

**Metrics from health state:** Error counters for readiness failures belong in Phase 10 observability standardization.

**`/health/ready` detail level:** Returning per-dependency status in the ready response body (e.g., `{"timescaledb": "ok", "redis": "unreachable"}`) may be useful for debugging but is not required in Phase 1. The `reason` field on 503 is sufficient.
