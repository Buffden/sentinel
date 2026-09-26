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
