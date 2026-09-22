# Phase 10 — Production Hardening + Failure Lab

## Goal

Standardize observability across the complete system and verify behavior under failure, replay, and representative load.

Observability has existed throughout earlier phases. This phase **completes and validates it system-wide**.

**No mandatory feature-UI checkpoint.** Every other user-facing phase in this roadmap (07–09) must reach a dedicated frontend checkpoint before it's considered done, per `IMPLEMENTATION_WORKFLOW.md`'s vertical-slice rule — this phase is the explicit exception, because it adds no new operator-facing capability. Its own "vertical slice" is end-to-end diagnosability and failure evidence, not a UI: the exit criteria below already express that (the developer can diagnose the system from operational signals, not "an operator can click a new button").

## Starting State (verified against the code on 2026-09-22)

This section records what earlier phases actually left in place, checked directly in the code rather than assumed, so the checkpoints below start from real ground.

**Already in place**

- The Position Consumer routes malformed records to `adsb.dlq` and `ais.dlq`, and a failed DLQ publish blocks the offset commit rather than silently dropping the record.
- The Alert Evaluator runs leader election with a Redis lease, so leader failover can be tested as it is.
- The ingestion poller, Position Consumer and Correlation Worker already write one JSON object per log line.
- Docker Compose runs Redpanda, TimescaleDB, Redis and Neo4j with container health checks.

**Not in place yet**

- **The ingestion poller runs out of OpenSky credits.** Its default bounding box (latitude 49 to 61, longitude -8 to 10) is 216 square degrees, which costs 3 credits per call, and it polls every 10 seconds. That is about 26,000 credits a day against a budget of 400 (anonymous) or 4,000 (logged in). The poller treats a `429` like any other failure: it logs a warning and tries again on the next cycle, ignoring OpenSky's rate-limit headers. A config comment also describes the limit incorrectly. The full analysis is in ADR-020 (Proposed).
- **A provider outage looks like every aircraft going dark.** The Alert Evaluator declares signal loss after 5 minutes without a position. Nothing tells it that the provider, not the aircraft, stopped reporting. An exhausted OpenSky budget should therefore produce a signal-loss alert for every tracked aircraft. This is expected from reading the code but has not been observed on purpose yet.
- **Logs are not consistent across services.** The poller and Position Consumer include timestamp, level, service and message. The API writes JSON but without a timestamp or service name, and uses a different field name for the message. The Alert Evaluator passes an object and a string to the console directly, so some of its lines are not JSON at all.
- **Health checks do not check anything.** The API's `/healthz` always answers "ok", even if Postgres, Redis, Neo4j or its Kafka consumer is down. The other application services have no health endpoint.
- **There is no view of Kafka consumer lag** other than running `rpk` by hand.
- **There is no load generator.** The load experiments below assume one. The synthetic generator planned in Phase 04 was never built, because that phase stopped after its first checkpoint.
- **The application services are not in Docker Compose.** Only the infrastructure is. They run by hand.

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

Deliberately test Position Consumer crashes/replay, Redis failure/recovery, TimescaleDB outage, Neo4j outage and retry, Alert Evaluator leader failover, offset reset/replay, duplicate delivery, multi-instance API failure/reconnect behavior, and **ingestion provider outage or credit exhaustion** (see Starting State above and ADR-020).

## Load / Capacity Experiments

Use a load generator to measure ingestion throughput, consumer lag under burst, TimescaleDB write/query latency, H3 candidate density/correlation cost, Redis memory/key growth, and WebSocket fan-out behavior.

Tune implementation choices only from observed evidence.

## Proposed Checkpoint Sequence

Every checkpoint below is **Pending**. This is a proposed order, not a set of decisions. Each one follows the full implementation sequence in `CLAUDE.md` (teach-back, direct experiment, implementation, a real failure boundary, docs), and the scope of each is confirmed before it starts.

The order puts the failure already hit in real use first, then the observability that later lab experiments need in order to be diagnosed at all, then the lab itself, then load.

| # | Checkpoint | Smallest observable result | Status |
| --- | --- | --- | --- |
| 1 | OpenSky credit budget | The poller logs its remaining credit balance each cycle, stays inside the daily budget over a full day, and when deliberately pushed past it, waits the time OpenSky asks for instead of retrying every cycle | Pending |
| 2 | Provider outage versus aircraft dark | First, stop the poller for more than 5 minutes and observe what the Alert Evaluator does. Then decide how Sentinel should tell the two cases apart | Pending, needs a design decision |
| 3 | Consistent structured logs | Every service's log lines parse as JSON with the same core fields, and one alert can be followed from ingestion to WebSocket by searching logs for its identifiers | Pending |
| 4 | Dependency-aware health | Stopping Redis, Postgres or Neo4j makes the affected service report unhealthy, and starting it again makes it report healthy | Pending |
| 5 | Consumer lag visibility | Pausing a consumer and watching its lag grow, then shrink after it restarts, using a documented command | Pending |
| 6 onward | Failure lab runs | One checkpoint per failure listed above, each with its own debrief showing observed behavior | Pending |
| Later | Load generator and capacity runs | Depends on the load generator decision below | Pending |

### Checkpoint 1 detail: OpenSky credit budget

This is the first checkpoint because the failure has already happened in real use.

- **Ownership:** the ingestion poller alone. Nothing downstream changes.
- **What changes:** confirm the poller is logged in, shrink the default box to 25 square degrees or less (1 credit per call), set the interval to fit the daily budget, read `X-Rate-Limit-Remaining` on every response, and on a `429` wait for `X-Rate-Limit-Retry-After-Seconds` instead of retrying on the next cycle. Correct the config comment about anonymous limits.
- **Failure boundary to exercise:** run anonymously with a short interval to exhaust the 400 credit budget on purpose, then confirm the poller backs off for the time OpenSky asks and logs why.
- **Manual inspection:** the poller's own logs for credit balance and back-off, and `rpk` to confirm messages stop arriving on `adsb.raw` while it waits and resume afterwards.
- **Not in scope:** anything about signal loss. The poller backing off still leaves every aircraft silent, which is exactly what checkpoint 2 is for.

### Checkpoint 2 detail: provider outage versus aircraft dark

This is an architectural discovery under `CLAUDE.md`, not an implementation choice. The checkpoint starts with evidence (observing the mass signal-loss case on purpose) and a design discussion. It does not start with code. Candidate directions to compare at that point, none chosen:

- The poller reports its own health somewhere the Alert Evaluator can read, which gives the poller a new write path it does not have today.
- The Position Consumer tracks when it last received any position from each source, since it already writes Redis state.
- The Alert Evaluator infers an outage itself when too many entities go silent at once, which needs no new write path but relies on a threshold.

Whichever is chosen changes the signal-loss contract, so it needs an ADR and updates to `ARCHITECTURE.md` and the signal-loss use case before implementation.

## Decisions Needed

- **ADR-020:** accept the parts that apply now (OpenSky stays the only live source and gets tuned; provider health is treated as its own problem) before checkpoint 1 is implemented. The FlightAware route question stays with Phase 04.
- **Checkpoint 1 settings:** which region the smaller box covers, and what poll interval to use. These are implementation choices, made with the developer at that checkpoint.
- **Health endpoints for workers:** the Alert Evaluator, Correlation Worker and Position Consumer have no HTTP server. Options include adding a small health port to each, or relying on logs and container health checks. This is an implementation choice for checkpoint 4.
- **Load generator:** build a minimal one inside this phase for load experiments, or first build the Phase 04 synthetic generator and reuse it. The load experiments cannot start until this is decided.
- **Running the application services in Docker Compose:** several failure experiments (killing and restarting a service, running two API instances) are easier to reproduce if the services run as containers. Whether to do that here is open.
- **AWS deployment:** the fixed stack names Docker Compose to AWS as the deployment target, but no phase plan currently covers the deployment itself. Decide whether it belongs in this phase or stays out of scope.

## Exit Criteria

The developer can diagnose Sentinel from operational signals without reading code, explain important crash/replay scenarios, demonstrate idempotent durable effects, describe degradation boundaries, and defend measured bottlenecks/trade-offs.
