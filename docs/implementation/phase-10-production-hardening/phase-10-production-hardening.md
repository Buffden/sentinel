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
- The ingestion poller and Position Consumer already write one JSON object per log line. (This line originally also named the Correlation Worker. The CP4 audit found that it passes an object and a string to the console, like the Alert Evaluator, so its lines are not JSON.)
- Docker Compose runs Redpanda, TimescaleDB, Redis and Neo4j with container health checks.

**Not in place yet**

- **The ingestion poller runs out of OpenSky credits.** It polls every 10 seconds by default, which is 8,640 calls a day. OpenSky's daily budget is 400 credits anonymous or 4,000 logged in, and each call costs 1 to 4 credits depending on the size of the bounding box. The code's default box (latitude 49 to 61, longitude -8 to 10) is 216 square degrees, 3 credits per call, so about 26,000 credits a day. The local, uncommitted `.env` sets a small San Francisco Bay Area box (1 credit per call) and OpenSky login credentials, which still needs 8,640 credits a day against 4,000. Nothing in the poller's start script loads `.env` automatically, so which of these two configurations actually runs depends on how the poller is started, and has to be confirmed first. The poller treats a `429` like any other failure: it logs a warning and tries again on the next cycle, ignoring OpenSky's rate-limit headers. A config comment also describes the limit incorrectly. The full analysis is in ADR-020. **Addressed by CP2:** the defaults, `.env` loading and `429` handling now keep the poller inside its budget.
- **A provider outage looks like every aircraft going dark.** The Alert Evaluator declares signal loss after a fixed silence (5 minutes by default; the local `.env` sets 15). Nothing tells it that the provider, not the aircraft, stopped reporting. An exhausted OpenSky budget should therefore produce a signal-loss alert for every tracked aircraft. This is expected from reading the code but has not been observed on purpose yet.
- **A record in an unsupported compression stops ingestion silently.** Observed on 2026-09-23: one record produced with `rpk topic produce` (which compresses with snappy by default) made the Position Consumer crash on every fetch, because kafkajs has no snappy codec installed. The group sat at 0 members with the lag stuck at 1, and nothing after that offset could be processed. The failure happens before a message is handed to the consumer, so it never reaches the DLQ, and the consumer's Kafka client runs at `logLevel: 0`, so the crash is never logged. Today's pollers do not compress, so this is latent, but any producer using snappy, lz4 or zstd would trigger it. **Now visible in logs (CP4, 2026-09-28):** the record is still in `adsb.raw` near offset 434901. The Position Consumer's experiment entry point, reading from the beginning in its own `kafka-experiment` group, reached it and logged kafkajs's `Crash: KafkaJSNotImplemented: Snappy compression not implemented` with its stack. The partition still stops, and the process exits with status 0, so fixing the behavior stays in the failure lab.
- **A consumer that has stopped for good can exit with status 0.** Observed on 2026-09-28 during CP4. After kafkajs crashed on the snappy record and did not restart the consumer, the Position Consumer's experiment process exited with status 0, which tells a supervisor it succeeded. The main consumer entry point was not tested this way, so whether it also exits 0, or stays running with no consumer, is still unknown. This is tracked separately from the poison-pill fix: any fatal consumer failure must end the process with a non-zero status, or stay visibly unhealthy to health checks (CP5). Belongs to the failure lab.
- **The Correlation Worker's integration test writes to the live candidate topic.** Observed on 2026-09-28 during CP4. The worker's integration test publishes candidates for `test-worker-…` pairs to the real `proximity.candidates` topic. The next time the Alert Evaluator ran, it turned the six candidates from that day's two test runs into `UNSCHEDULED_PROXIMITY` alerts on the real `alerts` topic. The Alert Evaluator's own integration tests do the same on purpose: they publish to the real `alerts` topic, as their header warns. When the API next ran, it persisted 139 test alerts from its backlog. The dev database then held 217 alert rows whose `alert_id` starts with `test-`, some left by earlier phases' test runs. They were not deleted. Tests should publish to their own topics, or to nothing live. Not fixed in CP4.
- **Logs are not consistent across services.** The poller and Position Consumer include timestamp, level, service and message. The API writes JSON but without a timestamp or service name, and uses a different field name for the message. The Alert Evaluator and Correlation Worker pass an object and a string to the console directly, so some of their lines are not JSON at all. **Being addressed by CP4.**
- **Health checks do not check anything.** The API's `/healthz` always answers "ok", even if Postgres, Redis, Neo4j or its Kafka consumer is down. The other application services have no health endpoint.
- **There is no view of Kafka consumer lag** other than running `rpk` by hand.
- **There is no load generator.** The load experiments below assume one. The synthetic generator planned in Phase 04 was never built, because that phase stopped after its first checkpoint.
- **The application services are not in Docker Compose.** Only the infrastructure is. They run by hand.
- **kafkajs prints a non-JSON warning at connect.** Observed on 2026-09-23 during CP2: kafkajs 2.2.4 on Node 23 prints a `TimeoutNegativeWarning` to stderr during `producer.connect()`. A bare kafkajs connect script reproduces it, so it comes from the library, not from Sentinel code. Node replaces the negative delay with 1 ms and connecting works normally. It was not fixed in CP2. It matters for CP4, because these lines are not JSON. Only the ingestion poller was checked. See the CP2 debrief.

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

CP3a to CP3f and CP4 are **Done**. Every later checkpoint is **Pending**. Each one follows the full implementation sequence in `CLAUDE.md` (teach-back, direct experiment, implementation, a real failure boundary, docs), and the scope of each is confirmed before it starts.

| # | Checkpoint | Smallest observable result | Status |
| --- | --- | --- | --- |
| Pre-CP1 | Provider experiment and ADR-020 decision | A 15 minute side-by-side OpenSky and adsb.fi measurement over SF Bay, a deliberate `429` on both, and ADR-020 decided from the evidence. See [concepts/provider-experiment/README.md](concepts/provider-experiment/README.md) | Done |
| CP1 | adsb.fi regional primary ingestion | adsb.fi positions flow end to end through the ADR-021 envelope on `adsb.raw` and land in the canonical model with provider `adsbfi`, verified in TimescaleDB and Redis. Real `429`s from adsb.fi make the poller back off with jitter, never faster than its 2 second interval, and it recovers to normal polling. See [concepts/adsbfi-primary-ingestion/](concepts/adsbfi-primary-ingestion/) | Done |
| CP2 | Harden OpenSky as the fallback | The OpenSky poller runs at a budget-safe interval over a 1-credit box, logs its credit balance each cycle, and honours the retry time. A deliberately exhausted budget produces one clear pause and one clear resume. Implementation and live validation are done. The real `429` pause was validated live, using OpenSky's real retry time of about 8 hours. The resume was validated by unit tests only and was not observed live, because that retry window was too long to wait through. The missing-header fallback was also validated by unit tests only. See [concepts/opensky-fallback-hardening/](concepts/opensky-fallback-hardening/) | Done |
| CP3 | Provider health, failover and failback | Design first, with its own ADR. Then cutting off adsb.fi makes OpenSky take over, and adsb.fi takes back over when it has recovered steadily. Both switches are logged, the switch does not bounce during an unstable recovery, and aircraft both providers see raise no false signal-loss alert | Done through CP3f; see the six sub-checkpoints and runtime evidence below |
| CP3a | Coordinator lease and heartbeat | One coordinator process runs the adsb.fi adapter only while it holds `{live-provider}:lease`, and `heartbeat_ms` advances every 5 s. A second coordinator stays idle while the lease is held, and takes over once the first is killed and its lease expires. See [concepts/coordinator-lease/](concepts/coordinator-lease/) | Done |
| CP3b | adsb.fi authority and coverage timeline | The `{live-provider}:authority` hash and `{live-provider}:coverage` sorted set open, extend and close adsb.fi coverage segments with the ADR-022 reasons, and `timeline_version` changes only when a segment opens, closes or authority is committed. See [concepts/authority-coverage-timeline/](concepts/authority-coverage-timeline/) | Done |
| CP3c | Evaluator observed silence | The Alert Evaluator counts only the owning provider's coverage toward the signal-loss threshold. Repeating the outage experiment raises no mass `SIGNAL_LOSS` wave, and an evaluator restart after downtime raises no restart burst. See [concepts/observed-silence/](concepts/observed-silence/) | Done |
| CP3d | Provider health state machines | The `{live-provider}:health:*` hashes move through `HEALTHY`, `DEGRADED`, `UNAVAILABLE` and `RECOVERING` at the ADR-022 thresholds, driven by real request outcomes. See [concepts/provider-health/](concepts/provider-health/) | Done |
| CP3e | Failover to OpenSky | With injected provider responses against real Redis and Redpanda, an adsb.fi outage closes coverage and relinquishes to `none`; a successful OpenSky candidate publish COMMITs authority and CREDITs coverage; a failed candidate Kafka delivery leaves authority unset/`none`; and restart restoration keeps stored OpenSky authority conservatively. See [concepts/provider-failover/](concepts/provider-failover/) | Done |
| CP3f | Failback hysteresis and restart restoration | adsb.fi takes authority back only when `HEALTHY` and OpenSky has been authoritative for at least 5 min. Real Redis + Redpanda verification proves the minimum window, serialized OpenSky → adsb.fi HANDOVER, `handover_attempt` coverage, recovery gating, conservative restart, and backed-off retries after any failed attempt without starving OpenSky or exceeding adsb.fi's request limit. See [concepts/provider-failover/](concepts/provider-failover/) | Done |
| Investigation | Proximity pairs dominated by ground traffic | Measure how many proximity candidates in the real pipeline involve aircraft that are not clearly airborne. Any filter is a separate decision | Pending, investigation only |
| CP4 | Consistent structured logs | Every service's log lines parse as JSON with the same core fields, and one alert can be followed from ingestion to WebSocket by searching logs for its identifiers | Done: all five services migrated and validated live, and one real alert traced from ingestion to a connected WebSocket client. Known limitations are listed in the CP4 detail below See the CP4 detail below |
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

It changes the signal-loss contract, so `ARCHITECTURE.md`, `DATA_MODEL.md` and the signal-loss use case change with the implementation.

**Status (2026-09-23).** The outage experiment is done ([concepts/provider-outage-experiment/](concepts/provider-outage-experiment/README.md)): cutting adsb.fi off made all 78 airborne aircraft raise `SIGNAL_LOSS` in one scan. ADR-022 is accepted. It covers:

- one ingestion coordinator that owns provider health and authority;
- failover only after a successful OpenSky cycle;
- failback only with adsb.fi `HEALTHY` plus 5 min on OpenSky;
- a Redis coverage timeline;
- signal loss measured as silence observed by the aircraft's owning provider.

Of the candidate directions above, the first was chosen, in the form of one coordinator rather than separate pollers. The Position Consumer and mass-silence options were rejected. CP3a through CP3f are implemented.

### CP3 sub-checkpoints

ADR-022 is implemented in six sub-checkpoints, CP3a through CP3f. Each one kept strictly to its own scope rather than pulling later production-hardening work forward.

The evaluator change (CP3c) comes before any provider switching. Coverage plus observed silence fixes the mass-alert wave from the outage experiment using adsb.fi alone. That proves the signal-loss fix end to end before health and switching add complexity.

- **CP3a, lease and heartbeat.** Scope is a single coordinator process, its `{live-provider}:lease`, and the `heartbeat_ms` renewal. A second coordinator taking over after the lease expires is expected. The lease only guards against running two instances at once. It is not fencing: as ADR-022 section 8 states, a coordinator paused past its lease can still complete Kafka sends. Not in scope: health state machines, the coverage timeline, failover, and any evaluator change.
- **CP3b, adsb.fi authority and coverage timeline.** Scope is the `authority` hash and the `coverage` sorted set for adsb.fi only: opening, extending and closing segments, and the lease-checked atomic write scripts. Not in scope: health-driven switching and OpenSky authority.
- **CP3c, evaluator observed silence.** Scope is the Alert Evaluator reading the coverage timeline once per scan and measuring silence as the owning provider's coverage. The evidence is a repeat of the outage experiment. Not in scope: provider switching, which does not exist yet.
- **CP3d, provider health state machines.** Scope is the per-provider health states, transitions, check rates and the `health:*` hashes. Not in scope: acting on health to change authority.
- **CP3e, failover to OpenSky.** Scope is loss of authority, selection rounds, committing OpenSky authority after a successful active cycle, and the part of restart restoration that CP3e's own states need: a stored authority whose provider restores `UNAVAILABLE` becomes `none` before polling, and a stored `none` or `opensky` is resumed. Not in scope: failback, and restoring a failback in progress.
- **CP3f, failback hysteresis and handover restoration.** Scope is voluntary failback with the 5 min minimum, the handover sequence, and restoring a failback that was in progress when a coordinator stopped.

### CP4 detail: consistent structured logs

CP4 is **Done** (2026-09-28). All five backend services pass the checks below, and one alert has been traced from ingestion to WebSocket delivery. Services were migrated one at a time, each validated live before the next began.

| Service | Status |
| --- | --- |
| Ingestion poller | Migrated, validated live |
| Position Consumer | Migrated, validated live |
| Correlation Worker | Migrated, validated live |
| Alert Evaluator | Migrated, validated live |
| API | Migrated, validated live |
| End-to-end alert trace | Passed on 2026-09-28, see below |

**The log contract.** Every service writes one JSON object per line to stdout using pino, so stderr stays empty in normal operation.

| Field | Rule |
| --- | --- |
| `ts` | ISO-8601 UTC with milliseconds. Processing time only; source event time stays in its own fields such as `timestamp_ms` and `dark_since_ms` |
| `level` | One of `debug`, `info`, `warn`, `error`, `fatal`, as a word |
| `service` | The service's name, for example `ingestion-poller` or `position-consumer` |
| `msg` | A short lower-case description of the event |
| `instance_id` | The process's identity where it has one. The poller uses a random id per process. The Position Consumer uses the same host and process id it writes into DLQ records |
| `err` | Always this key, always an object with `type`, `message` and `stack`. `type` comes from the error's name, so a Node warning shows as `TimeoutNegativeWarning` rather than `Error`. A thrown or rejected value that is not an Error, such as a string, `null`, `undefined` or a plain object, becomes type `NonError` with the value as its message and an empty stack |
| Context | snake_case: `entity_id`, `alert_id`, `alert_type`, `pair_key`, `provider`, `topic`, `partition`, `offset` |

Time fields name their unit. A key ending in `_ms` or `_seconds` holds a number or null, and a key ending in `_at` holds an ISO timestamp. A time name without a unit, such as `retry_time` or `duration`, breaks the contract. A key that isn't a time, such as `timeline_version`, has no time rule.

**Third-party and Node output.** kafkajs logs at WARN and above go through the service's logger. They are tagged `component: kafkajs`, their camelCase keys are renamed to snake_case (`retryTime` becomes `retry_time_ms`), and their error text and stack are rebuilt as `err`. These logs were previously switched off entirely. Node's process warnings are recorded as `warn` lines by a `warning` handler. Uncaught exceptions and unhandled rejections are logged as `fatal`, and the process then exits with status 1. The logger writes synchronously, so the fatal line reaches stdout before the process exits.

**Launching.** Launchers pass `--no-warnings` to Node. That switches off Node's plain-text warning printer, and the JSON handler keeps each warning recorded. With and without the flag, the poller and the Position Consumer each produced one plain-text warning and one matching JSON warning, so nothing is lost. Start services with `npm run -s` followed by the script name, for example `npm run -s coordinate` or `npm run -s consumer`. The API's `start` script does not load its `.env` file the way the poller's does, so export that file into the shell before running `npm run -s start`. Without `-s`, npm prints its own two-line banner to stdout. `-s` also hides npm's own diagnostics, so if a service fails to launch, run the same command again without `-s` to see npm's error. There is deliberately no repository-wide `.npmrc` setting, because that would also hide npm's warnings during installs. The Correlation Worker installs with pnpm, not npm. It has only a pnpm lockfile and no CI job, and npm crashed while reading its pnpm-built `node_modules`.

**Where the process handlers are installed.** The handlers are in a small module that each entry file imports first, so they exist before any dependency is evaluated. Tests must never load them, because a crash handler that exits the process would kill the test worker. When a service's main module is also imported by integration tests (the Position Consumer, Correlation Worker and Alert Evaluator), the service starts from a separate entry file, `main.ts`. The main module exports a start function and no longer checks whether it was run directly.

**Linking the ingestion hop.** The poller logs each published batch with its topic, partition and base offset, next to the number of records published. The Position Consumer logs topic, partition and offset on every record. `adsb.raw` has a single partition today, so a batch covers exactly its base offset up to base offset plus the published count minus one. The live check confirmed this for a 154-record batch: all 154 offsets appeared as `position persisted` lines with provider `adsbfi`. This arithmetic holds only for a single partition. With more partitions, a batch-wide published count cannot say how many records went to each partition, so the link would need per-partition counts.

**Linking the correlation hop.** The Correlation Worker logs `proximity candidate published` for each candidate it sends. The line carries:
- the pair key and both aircraft ids;
- the episode start;
- the triggering position's `entity_id` and source `timestamp_ms`;
- the `proximity.candidates` topic, partition and base offset from the send result.

The triggering position joins to the Position Consumer's `position persisted` line by `entity_id` and `timestamp_ms`. That pair is exact for the logical position, since it is the same identity `position_history` is idempotent on. It is not exact for the raw record. adsb.fi repeats an aircraft's last position until the aircraft reports a new one, and one traced candidate's position appeared in 11 consecutive batches with the same source timestamp. Naming the exact raw record would need the source Kafka offset carried through the worker's message handler, and the Position Consumer's `position.normalized` publish offset logged. Neither is done.

**Linking the alert hop.** The Alert Evaluator logs each alert it emits with:
- `alert_id` and `alert_type`;
- the entity ids and, for proximity and composite alerts, the pair key;
- the `alerts` topic, partition and base offset from the send result.

A proximity alert joins to the worker's candidate line by `pair_key` and `episode_start_ms`, the two parts its `alert_id` is built from. The live check followed one candidate at `proximity.candidates` offset 9923 to its alert at `alerts` offset 16370. The evaluator's integration tests assert the signal-loss fields. The live scans raised no signal-loss alert.

**Linking the API hop.** The API's sink logs `alert sinked` with the `alerts` topic, partition and offset it consumed, the `alert_id`, and `published_alert_ids`. That last field lists every row it republished to `alert-events`, which for a composite alert includes the alerts it supersedes. Each of those rows then produces one `alert fan-out` line on every API instance, with the `alert_id`, its status, `open_connections` and `sends_attempted`. `sends_attempted` counts the frames handed to the WebSocket library for open connections that passed the scope filter. It does not confirm that a frame was written to the socket or received by a browser. The API's Kafka client previously used kafkajs's default logger at INFO, which printed kafkajs's own JSON shape to stdout. It now logs WARN and above through the contract like the other services, so kafkajs's INFO lines, such as consumer group joins, are no longer printed.

**The end-to-end trace (2026-09-28).** All five services ran together on fresh adsb.fi data, with a demo WebSocket client connected before any new alert was created. One `UNSCHEDULED_PROXIMITY` alert was followed from the client back to its poller batch, using only log lines:

| Hop | Time (UTC) | What links it to the next hop |
| --- | --- | --- |
| Poller batch | 22:03:13.517 | `adsb.raw` partition 0, base offset 521304, 173 records |
| Position Consumer | 22:03:13.678 | position `aa6b9f` at source time 1790632988809, from `adsb.raw` offset 521318, the only raw copy of that position |
| Correlation Worker | 22:03:14.220 | pair `3c4b32:aa6b9f`, episode start 1790632988809, sent to `proximity.candidates` offset 10827 |
| Alert Evaluator | 22:03:14.224 | `alert_id` `3c4b32:aa6b9f:UNSCHEDULED_PROXIMITY:1790632988809`, sent to `alerts` offset 17274 |
| API sink | 22:03:14.227 | consumed `alerts` offset 17274, persisted, republished that `alert_id` |
| API fan-out | 22:03:14.227 | status `NEW`, 1 open connection, 1 send attempted |
| WebSocket client | 22:03:14.227 | received the frame for that `alert_id` with status `NEW` |

The trace took about 710 ms from the poller's publish to the client's receipt. In this case the triggering position had a single raw copy, so every hop was exact. In general the consumer-to-worker join is exact for the logical position but not for the raw record, as explained above. In the same run, every `sends_attempted: 1` fan-out line matched a frame the client recorded (1,302 of each). The 1,568 older alerts the API processed before the client connected all logged `sends_attempted: 0`. Across the five services the run produced about 11,000 log lines. All of them passed the validator, and every stderr was empty.

**Non-Error rejections.** A review found that `Promise.reject('connection lost')` logged `err` as a bare string. The process still exited 1. The `err` serializer now normalizes any value, and the crash handlers serialize explicitly, because pino drops an `undefined` value before its serializers run. Each service was checked in child processes with a rejected string, `null`, `undefined`, plain object and circular object, a thrown string, a thrown `TypeError`, and a structured kafkajs error. All 40 cases produced a full `err` object, and every crash case exited 1.

**Known limitations, kept after Done.**
- No signal-loss alert was raised during CP4's live runs, so the signal-loss log path has only integration-test evidence.
- The consumer-to-worker join is exact for the logical position, not the raw record.
- Test suites that publish to shared topics or channels were not rerun after the last changes, and five API test files were never run during CP4:
  - the Correlation Worker's `worker.integration`;
  - the Alert Evaluator's `evaluator.integration`;
  - the API's `alertSink`, `wsServer`, `multiInstanceLifecycle`, `alertLifecycle` and `alerts`.

  The live trace exercised their code paths but does not replace their assertions. They stay unrun until they publish to isolated topics.
- The API process has no SIGTERM handler. It is killed with exit status 143 and logs no shutdown line. This predates CP4.


**How each service is checked.** A validator parses every line of captured stdout and stderr and checks the field types, allowed levels, `err` shape, naming and time-field rules. Any line that isn't JSON fails, whatever its cause. Each service is also checked with one failure that goes through its new error paths:
- For the poller, a broker address that refuses connections.
- For the API, only the six test files that do not publish to shared topics or Redis channels were run. `alertSink`, `wsServer`, `multiInstanceLifecycle`, `alertLifecycle` and `alerts` publish to the real `alerts` topic or the `alert-events` and `position-updates` channels. They stay unrun until they are isolated, and their paths were checked by the live trace instead.
- For the Correlation Worker, a broker address that refuses connections. kafkajs's retries and the final error are logged, and the process exits 1.
  Before its live check, the worker's group was 83,550 records behind, mostly days-old telemetry. It was moved to the end of `position.normalized` so old positions could not reach the live alert path as new proximity candidates. The Neo4j evidence for encounters in that skipped range was never recorded.
- For the Position Consumer, an unreachable TimescaleDB while real records were waiting. kafkajs's retries and consumer restarts are now visible, with the failing record's topic, partition and offset. The committed offset stayed put, and the 618 held-back records were all processed once the database was back.

### Investigation: proximity pairs dominated by ground traffic

The provider experiment found that most close proximity pairs involved airport surface traffic (see its README). The correlation worker has no on-ground filter. This item measures the effect in the real pipeline and brings the evidence back for a separate decision. It does not change the correlation worker, and it is not part of the provider decision.

## Decisions Needed

- **Health endpoints for workers:** the Alert Evaluator, Correlation Worker and Position Consumer have no HTTP server. Options include adding a small health port to each, or relying on logs and container health checks. This is an implementation choice for CP5.
- **Load generator:** build a minimal one inside this phase for load experiments, or first build the Phase 04 synthetic generator and reuse it. The load experiments cannot start until this is decided.
- **Running the application services in Docker Compose:** several failure experiments (killing and restarting a service, running two API instances) are easier to reproduce if the services run as containers. Whether to do that here is open.
- **AWS deployment:** the fixed stack names Docker Compose to AWS as the deployment target, but no phase plan currently covers the deployment itself. Decide whether it belongs in this phase or stays out of scope.

## Exit Criteria

The developer can diagnose Sentinel from operational signals without reading code, explain important crash/replay scenarios, demonstrate idempotent durable effects, describe degradation boundaries, and defend measured bottlenecks/trade-offs.
