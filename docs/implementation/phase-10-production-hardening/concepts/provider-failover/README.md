# Provider Failover and Failback Runtime Verification

CP3e and CP3f are defined by ADR-022 and the architecture/data-model docs. This file keeps only the runtime evidence needed to close those checkpoints; it does not duplicate that design.

## Environment

Verification ran in GitHub Actions on 2026-09-26 using:

- Redis 7.2.4, with the production lease, health and coverage Lua scripts;
- Redpanda v24.1.2, with KafkaJS using the same producer path as the service;
- the production `Coordinator`, `CoordinatorLease`, `CoverageTimeline` and `ProviderHealthStore`;
- injected adsb.fi and OpenSky responses.

No provider HTTP call was made by these tests, so the run used **zero live OpenSky credits**. Successful delivery paths used the real Redpanda broker. The Kafka-failure boundary used a deliberately disconnected KafkaJS producer so the failure happens at the publish dependency without taking the shared CI broker down.

The permanent test is `services/ingestion-poller/src/failover.integration.test.ts`.

## Observed boundaries

### 1. First authority and failover

Starting from no authority and no health state:

1. the first valid adsb.fi response published and COMMIT created epoch 1;
2. coverage remained closed because that response only seeded freshness;
3. the next advancing adsb.fi response CREDITed coverage as a separate revision;
4. adsb.fi failure closed coverage, health reached `UNAVAILABLE`, and RELINQUISH wrote `provider=none` while keeping epoch 1;
5. a healthy OpenSky candidate published to Kafka, COMMIT created epoch 2, and CREDIT opened OpenSky coverage.

The test observes `timeline_version=2` after adsb.fi's first coverage open, `4` at the intermediate `none`, and `6` after OpenSky COMMIT + CREDIT. That is the intended separation: authority and coverage are different facts and therefore different revisions.

### 2. Recovery before the failback window

While OpenSky held authority, adsb.fi recovered through `RECOVERING` to `HEALTHY`. Before the minimum OpenSky authority window elapsed it remained a standby check and produced no Kafka records.

The CP3e scenario then made OpenSky `UNAVAILABLE`, which relinquished to `none`; adsb.fi returned through the normal failover path as epoch 3. CP3f separately verifies the voluntary handover path below.

### 3. Candidate Kafka failure

With authority still uninitialized, adsb.fi unavailable and OpenSky returning valid data, the OpenSky candidate's Kafka delivery was forced to fail.

Redis showed OpenSky health success (`RECOVERING`, no health failure timestamp), while `provider` and `epoch` remained absent and the Kafka topic high watermark stayed 0. Provider health therefore does not absorb a broker failure, and authority is never granted before successful publication.

### 4. Restart restoration

The test preloaded OpenSky authority at epoch 7 with an open coverage segment and stored healthy provider state, then started a new coordinator.

Before its first publish, acquisition closed the predecessor's segment as `coordinator_down`. The epoch remained 7, OpenSky remained authoritative, and only OpenSky records reached Kafka. A restart therefore does not create an implicit failback or extend coverage across downtime.

### 5. Publication serialization

Every successful delivery in the failover run used the real Kafka producer. The test keeps a publish in flight briefly to let the two provider loops contend and records the maximum simultaneous calls. The observed maximum is **1**.

At shutdown, the real topic high watermark equals the number of successful deliveries recorded by the test, so the publication assertions are tied to broker-accepted records rather than log messages alone.


### 6. Voluntary failback hysteresis

A stored OpenSky authority at epoch 4 was started with healthy provider records and a scaled 1.5 s minimum in the injected runtime. adsb.fi reached `HEALTHY` quickly, but authority remained OpenSky before the minimum expired.

On the first eligible standby check after the threshold, the coordinator let the current OpenSky request finish, closed OpenSky coverage as `handover_attempt`, fetched adsb.fi again, published that cycle to the real broker, and HANDOVER changed authority directly from OpenSky to adsb.fi at epoch 5. CREDIT then opened adsb.fi coverage. The test observed no intermediate `none` authority and maximum concurrent publication remained 1.

The production minimum is a fixed **5 minutes**; only the injected test dependency is scaled.

### 7. Restart during an interrupted failback

The restart test seeds the exact durable state left before HANDOVER: OpenSky is still authority, coverage is already closed, and a prior `handover_attempt` segment exists.

A new coordinator resumes OpenSky first. Stored provider health is restored conservatively, so it does not assume the interrupted handover completed. Once adsb.fi is eligible again, a new handover succeeds and increments epoch 9 → 10. No separate "handover in progress" Redis field is required.

## Automated evidence

PR #167, CI run `36216036300`:

```text
src/failover.integration.test.ts  (3 tests)  2399 ms
  adsb.fi -> none -> OpenSky -> adsb.fi       1065 ms
  candidate Kafka failure                       792 ms
  stored OpenSky restart                        530 ms

Test Files  12 passed (12)
Tests       203 passed (203)
Duration    5.40 s
```

The full repository CI also passed for the CP3e closure commit. CP3f's focused ingestion run `36218659075` then passed the expanded suite:

```text
src/failover.integration.test.ts  5 passed
  failback minimum + real Kafka HANDOVER       passed
  interrupted-failback restart                 passed

Test Files  12 passed (12)
Tests       209 passed (209)
```

## Limits of this verification

These runs deliberately do not call live adsb.fi or OpenSky. Provider HTTP behavior and real OpenSky rate-limit handling were established in earlier checkpoints; CP3e/CP3f verify coordinator authority, failover and failback behavior around those adapters.

The Kafka failure is a disconnected KafkaJS producer, not a stopped Redpanda process. Earlier CP3d evidence covers a physical Redpanda outage and establishes that broker failure does not become provider-health failure. Here the assertion is specifically that a failed **candidate delivery** cannot grant authority.

The Redis lease is still a duplicate-instance guard, not Kafka fencing. The known stale-send window in ADR-022 remains unchanged.

## Result

CP3e and CP3f are complete. Sentinel can fail over through `none` when the active provider is unavailable, and can later fail back voluntarily from OpenSky to a proven-healthy adsb.fi only after the five-minute minimum. Both paths publish before changing authority, keep coverage separate, serialize publication, and restore conservatively after interruption.

### CP3f repair

A review after the initial CP3f run found three problems in failback, none of which the earlier CI run could see:

- A failed handover was retried on every 10 second adsb.fi standby check, and each failure restarted OpenSky immediately. At that rate OpenSky would use about 8,640 requests a day against a 4,000 credit budget.
- The committing adsb.fi fetch could start milliseconds after the standby fetch, breaking adsb.fi's one request per second limit.
- The handover path did not check authority again after its Kafka publish, before calling HANDOVER.

The repair backs off every failed attempt (60 seconds, doubling to 15 minutes), resumes OpenSky one normal interval after a failed attempt, waits until one second after the standby request before the committing fetch, and rechecks authority, lease and term after the publish. A second review then found that backing off only publish failures was not enough: a failed committing fetch moves adsb.fi to `DEGRADED`, one standby success returns it to `HEALTHY`, and each new attempt cancelled OpenSky's resumed cycle, so OpenSky was starved and nothing was published. Backing off every failed attempt closes that, and because the argument depends on the shortest retry being longer than OpenSky's active interval, the coordinator now refuses to start unless both `SELECTION_RETRY_BASE_MS` and `SELECTION_RETRY_MAX_MS` exceed `OPENSKY_ACTIVE_INTERVAL_MS`.

**Automated evidence.** The full ingestion-poller suite, including every integration file against local Redis and Redpanda, passed 220 of 220 on 2026-09-27. Unit fixtures cover repeated handover-only publish failures, alternating successful standby checks with failed committing fetches, a `stale_clock` refusal, lease loss during the spacing wait and during the handover publish, and the startup timing rule. The starvation and lease-loss fixtures were each confirmed to fail with the fix removed.

**Runtime evidence.** Two manual runs used the real coordinator, the real Redis scripts on the `{live-provider}` keys and real Redpanda publishes, with production timings and injected provider responses, so no live OpenSky credits were spent. OpenSky was seeded as the authority for more than five minutes with both providers healthy.

| Run | Injected failure | Failback attempt gaps | OpenSky | adsb.fi request spacing | Authority |
| --- | --- | --- | --- | --- | --- |
| 440 s | adsb.fi handover publish rejected, OpenSky publishes normally | 61, 121, 241 s | 16 requests, 16 publishes, 25 s rhythm | at least 1,001 ms | `opensky`, epoch 1 throughout |
| 300 s | committing adsb.fi fetch returns HTTP 503 | 61, 121 s | 11 requests, 11 publishes, coverage reopened after every attempt | at least 1,003 ms | `opensky`, epoch 1 throughout |

In both runs OpenSky's gap was longer than 25 seconds only right after a failed attempt (36 s and 46 s), because its next cycle is scheduled one interval after the attempt ends. `handover_attempt` segments appeared once per attempt instead of once per standby check. The startup rule was checked with real environment values: a retry base of 10 s, or a maximum of 10 s or 25 s, stops the coordinator with an error naming both settings, while the defaults load.

To inspect the same boundary, run the coordinator with a handover-only publish failure (not a stopped Redpanda, which would also break OpenSky's deliveries) and watch:

```bash
docker exec sentinel-redis redis-cli HGETALL '{live-provider}:authority'
docker exec sentinel-redis redis-cli ZRANGE '{live-provider}:coverage' 0 -1 WITHSCORES
```

OpenSky should stay authoritative with the same epoch, with `handover_attempt` segments at the backoff pace.
