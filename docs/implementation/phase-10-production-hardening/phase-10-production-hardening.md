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

- **The ingestion poller runs out of OpenSky credits.** It polls every 10 seconds by default, which is 8,640 calls a day. OpenSky's daily budget is 400 credits anonymous or 4,000 logged in, and each call costs 1 to 4 credits depending on the size of the bounding box. The code's default box (latitude 49 to 61, longitude -8 to 10) is 216 square degrees, 3 credits per call, so about 26,000 credits a day. The local, uncommitted `.env` sets a small San Francisco Bay Area box (1 credit per call) and OpenSky login credentials, which still needs 8,640 credits a day against 4,000. Nothing in the poller's start script loads `.env` automatically, so which of these two configurations actually runs depends on how the poller is started, and has to be confirmed first. The poller treats a `429` like any other failure: it logs a warning and tries again on the next cycle, ignoring OpenSky's rate-limit headers. A config comment also describes the limit incorrectly. The full analysis is in ADR-020.
- **A provider outage looks like every aircraft going dark.** The Alert Evaluator declares signal loss after a fixed silence (5 minutes by default; the local `.env` sets 15). Nothing tells it that the provider, not the aircraft, stopped reporting. An exhausted OpenSky budget should therefore produce a signal-loss alert for every tracked aircraft. This is expected from reading the code but has not been observed on purpose yet.
- **A record in an unsupported compression stops ingestion silently.** Observed on 2026-09-23: one record produced with `rpk topic produce` (which compresses with snappy by default) made the Position Consumer crash on every fetch, because kafkajs has no snappy codec installed. The group sat at 0 members with the lag stuck at 1, and nothing after that offset could be processed. The failure happens before a message is handed to the consumer, so it never reaches the DLQ, and the consumer's Kafka client runs at `logLevel: 0`, so the crash is never logged. Today's pollers do not compress, so this is latent, but any producer using snappy, lz4 or zstd would trigger it.
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

Deliberately test Position Consumer crashes/replay, Redis failure/recovery, TimescaleDB outage, Neo4j outage and retry, Alert Evaluator leader failover, offset reset/replay, duplicate delivery, multi-instance API failure/reconnect behavior, **ingestion provider outage or credit exhaustion** (see Starting State above and ADR-020), and **a raw record in a compression codec the consumers cannot decode** (the poison-pill case in Starting State: it must become visible in logs and must not silently stop a partition).

## Load / Capacity Experiments

Use a load generator to measure ingestion throughput, consumer lag under burst, TimescaleDB write/query latency, H3 candidate density/correlation cost, Redis memory/key growth, and WebSocket fan-out behavior.

Tune implementation choices only from observed evidence.

## Checkpoint Sequence

A measured provider comparison (Pre-CP1) changed this phase's order. ADR-020 now makes adsb.fi the primary regional live source and OpenSky the fallback, with exactly one authoritative live provider at a time, explicit failover, and no merging of positions from both. The order follows from that: build the new primary, make the fallback production-safe, then connect them through provider health, and only then move on to system-wide observability, the failure lab and load.

Every checkpoint after CP1 is **Pending**. Each one follows the full implementation sequence in `CLAUDE.md` (teach-back, direct experiment, implementation, a real failure boundary, docs), and the scope of each is confirmed before it starts.

| # | Checkpoint | Smallest observable result | Status |
| --- | --- | --- | --- |
| Pre-CP1 | Provider experiment and ADR-020 decision | A 15 minute side-by-side OpenSky and adsb.fi measurement over SF Bay, a deliberate `429` on both, and ADR-020 decided from the evidence. See [concepts/provider-experiment/README.md](concepts/provider-experiment/README.md) | Done |
| CP1 | adsb.fi regional primary ingestion | adsb.fi positions flow end to end through the ADR-021 envelope on `adsb.raw` and land in the canonical model with provider `adsbfi`, verified in TimescaleDB and Redis. Real `429`s from adsb.fi make the poller back off with jitter, never faster than its 2 second interval, and it recovers to normal polling. See [concepts/adsbfi-primary-ingestion/](concepts/adsbfi-primary-ingestion/) | Done |
| CP2 | Harden OpenSky as the fallback | The OpenSky poller runs at a budget-safe interval over a 1-credit box, logs its credit balance each cycle, and honours the retry time. A deliberately exhausted budget produces one clear pause and one clear resume | Pending |
| CP3 | Provider health, failover and failback | Design first, with its own ADR. Then cutting off adsb.fi makes OpenSky take over, and adsb.fi takes back over when it has recovered steadily. Both switches are logged, the switch does not bounce during an unstable recovery, and aircraft both providers see raise no false signal-loss alert | Pending, needs design and an ADR |
| Investigation | Proximity pairs dominated by ground traffic | Measure how many proximity candidates in the real pipeline involve aircraft that are not clearly airborne. Any filter is a separate decision | Pending, investigation only |
| CP4 | Consistent structured logs | Every service's log lines parse as JSON with the same core fields, and one alert can be followed from ingestion to WebSocket by searching logs for its identifiers | Pending |
| CP5 | Dependency-aware health | Stopping Redis, Postgres or Neo4j makes the affected service report unhealthy, and starting it again makes it report healthy | Pending |
| CP6 | Consumer lag visibility | Pausing a consumer and watching its lag grow, then shrink after it restarts, using a documented command | Pending |
| CP7 onward | Failure lab runs | One checkpoint per failure listed above, each with its own debrief showing observed behavior | Pending |
| Later | Load generator and capacity runs | Depends on the load generator decision below | Pending |

### CP1 detail: adsb.fi regional primary ingestion

- **Ownership:** a new adsb.fi poller that publishes raw positions to Kafka, and a new raw mapping in the Position Consumer that produces canonical positions with provider `adsbfi`. The OpenSky poller does not run alongside it, because only one provider is authoritative at a time.
- **Raw topic, decided in ADR-021:** both providers share `adsb.raw`, each message wrapped as `{ provider, payload }`, and the consumer classifies a record before normalizing it. Old bare OpenSky records stay replayable through a narrow legacy shape check. adsb.fi tracks without an ICAO address are skipped by the poller with a logged count.
- **Settings chosen:** a 48 NM circle containing the SF Bay box, polled every 2 seconds, with backoff from 2 seconds up to 60 seconds.
- **Requirements from ADR-020:** query a circle and filter to the monitored area (adsb.fi has no box query); send a descriptive user agent; after a `429`, back off with bounded exponential backoff and jitter, since adsb.fi gives no retry time.
- **Failure boundary to exercise:** deliberately exceed one request a second and confirm the poller backs off and recovers without flooding the logs.
- **Not in scope:** failover to OpenSky (CP3), and any change to the correlation worker.

### CP2 detail: harden OpenSky as the fallback

OpenSky is hardened before it becomes the automated fallback, so CP3 tests failover into a production-safe poller rather than one that still burns its budget at a 10 second interval.

- **Ownership:** the existing OpenSky poller alone.
- **What changes:** confirm which configuration actually runs (the startup log reports whether the poller is logged in), make the default box 25 square degrees or less (1 credit per call), set a budget-safe interval (25 seconds was chosen earlier: 3,456 of 4,000 daily credits), read `X-Rate-Limit-Remaining` on every response, and on a `429` wait for `X-Rate-Limit-Retry-After-Seconds` instead of retrying every cycle. Correct the config comment about anonymous limits, and make the poller's start script load its `.env`.
- **Requirements from ADR-020:** handle a `429` that carries a retry time but no balance, and a refusal that arrives before the balance reaches zero. Pauses can last hours, so the pause and resume must each be logged once, clearly.
- **Failure boundary to exercise:** exhaust the anonymous 400 credit budget on purpose and confirm the poller pauses for the time OpenSky asks, logs why, and resumes cleanly.
- **Not in scope:** signal loss and failover, both CP3.

### CP3 detail: provider health, failover and failback

This is an architectural discovery under `CLAUDE.md`, not an implementation choice. It starts with evidence (cutting the primary off on purpose and watching what the Alert Evaluator does) and a design discussion, not with code. Candidate directions for detecting provider health, none chosen:

- Each poller reports its own health somewhere the Alert Evaluator can read, which gives pollers a new write path they do not have today.
- The Position Consumer tracks when it last received any position from each provider, since it already writes Redis state.
- The Alert Evaluator infers an outage itself when too many entities go silent at once, which needs no new write path but relies on a threshold.

The ADR for this checkpoint must decide:

- **Failover:** what makes Sentinel switch from adsb.fi to OpenSky.
- **Failback, with hysteresis:** what makes it switch back, and how long adsb.fi must stay healthy first, so an unstable recovery cannot make Sentinel bounce between the two.
- **Signal loss during a switch:** the two providers do not see identical aircraft, so a switch changes which aircraft are visible. How signal loss treats aircraft the new provider cannot see is part of the design.

It changes the signal-loss contract, so it also needs updates to `ARCHITECTURE.md` and the signal-loss use case before implementation.

### Investigation: proximity pairs dominated by ground traffic

The provider experiment found that most close proximity pairs involved airport surface traffic (see its README). The correlation worker has no on-ground filter. This item measures the effect in the real pipeline and brings the evidence back for a separate decision. It does not change the correlation worker, and it is not part of the provider decision.

## Decisions Needed

- **Health endpoints for workers:** the Alert Evaluator, Correlation Worker and Position Consumer have no HTTP server. Options include adding a small health port to each, or relying on logs and container health checks. This is an implementation choice for CP5.
- **Load generator:** build a minimal one inside this phase for load experiments, or first build the Phase 04 synthetic generator and reuse it. The load experiments cannot start until this is decided.
- **Running the application services in Docker Compose:** several failure experiments (killing and restarting a service, running two API instances) are easier to reproduce if the services run as containers. Whether to do that here is open.
- **AWS deployment:** the fixed stack names Docker Compose to AWS as the deployment target, but no phase plan currently covers the deployment itself. Decide whether it belongs in this phase or stays out of scope.

## Exit Criteria

The developer can diagnose Sentinel from operational signals without reading code, explain important crash/replay scenarios, demonstrate idempotent durable effects, describe degradation boundaries, and defend measured bottlenecks/trade-offs.
