# Provider Health State Machines Debrief

Evidence from verifying CP3d on 2026-09-25, against local Redis 7.2.4 and Redpanda, with the coordinator polling the live adsb.fi API. The design is in [provider-health.md](provider-health.md).

---

## Before any code: checking the model by hand

Four experiments ran before implementation, spending one anonymous OpenSky credit and none from the account.

**The health hash on scratch keys.** A lease-checked script that deletes and rewrites the whole hash was played through every transition on `{lab}:*` keys, with times as offsets from T0 = 1790400000000:

| Step | `state` | `state_since_ms` |
| --- | --- | --- |
| success at T0 | HEALTHY | 1790400000000 |
| timeout at +10 s | DEGRADED | 1790400010000 |
| `http_503` at +25 s | DEGRADED | 1790400010000, unchanged |
| deadline timer running late at +71.5 s | UNAVAILABLE | 1790400070000, the deadline itself |
| success at +90 s | RECOVERING | 1790400090000, streak from the same time |
| success at +210 s | HEALTHY | 1790400210000, streak cleared |

A wrong token and an empty token both returned `lease_mismatch` and left the hash byte-identical. Six stored records were then restored at a new acquisition by a second token, following the restore table, and the old token's write after the takeover was refused.

**adsb.fi failure shapes.** Three real requests and two local checks gave the `last_error` classes:

| Case | Observed | Class |
| --- | --- | --- |
| valid | 200, keys `ac, msg, now, total, ctime, ptime` | success |
| 1 ms client timeout | `TimeoutError`: "The operation was aborted due to timeout" | `timeout` |
| bad path on the same host | HTTP 400, empty `text/html` body | `http_400` |
| DNS failure (no adsb.fi traffic) | `TypeError` "fetch failed", cause `ENOTFOUND` | `network:ENOTFOUND` |
| body without `now` | `adsb.fi response "now" is not epoch milliseconds: undefined` | `validation: …` |

**Why the deadline needs a timer.** The real adsb.fi backoff was simulated 100,000 times from the moment a provider enters `DEGRADED`, measuring how late the first request after the 60 s deadline finishes:

| Failure speed | p50 | p90 | p99 | max |
| --- | --- | --- | --- | --- |
| fast (~200 ms) | 19.3 s | 40.9 s | 53.4 s | 60.0 s |
| 8 s timeout | 15.8 s | 31.1 s | 59.2 s | 67.9 s |

The backoff ceiling reaches 60 s by the sixth failure. Relying on request outcomes alone could leave a provider "under observation" for up to about two minutes.

**OpenSky.** One anonymous request at 19:45:35 UTC: HTTP 200 with `x-rate-limit-remaining: 399`, `time` 1790365527 (an integer in epoch **seconds**, not milliseconds), and `states` a list of 150 vectors. No `429` was produced. The CP2 capture was reused instead: a real `429` carries `x-rate-limit-retry-after-seconds` (31952, then 31939, a countdown to one fixed refill moment) and no remaining-credits header. That capture was anonymous; an authenticated `429` has not been observed.

**Kafka after a provider success.** A scratch script ran the production adsb.fi fetch into a KafkaJS producer and stopped Redpanda between two cycles:

```text
+1080ms  provider success evidence (178 messages)
+1081ms  publish start
+21256ms publish FAILED  KafkaJSNonRetriableError "Connection error"
```

Provider success comes strictly first. `docker stop` surfaces a publish failure in about 20 s, where `docker pause` took 192.8 s in CP3b.

---

## Tests

The pure state-machine tests were written first. The suite went from 103 tests before CP3d to **176/176** at the end of implementation (148 unit, 28 integration). The two corrections afterwards (removing an experiment-only switch, and the zero-length close fix) brought it to **180/180**:

- **`providerHealth.test.ts`, 34 tests:** every transition; 59.9 s against 60 s; repeated failures not moving the clock; 119.9 s against 120 s; the streak reset on failure; `429` with and without a retry time from every state; unknown success and failure; first-deployment `HEALTHY`; the deadline guard in both orders; every restore case; the exact field names; error classes; OpenSky check rates.
- **`coordinator.test.ts`, 27 new tests (26 to 53)**, among them: health restored before the first request and recorded before publishing; first deployment staying `HEALTHY` when the publish fails; a Kafka failure adding no health failure; seeded and unconfirmed responses as success; frozen as failure plus a coverage close; the deadline firing with no request finishing; a deadline callback queued behind a success not overwriting `HEALTHY`; an `UNAVAILABLE` adsb.fi still polling, publishing and crediting; refused or timed-out health writes and a failed health read failing closed; OpenSky checks never publishing, their cadence, pause and resume, the 30 s recheck, token failure, kept credits and every restore case; a zero-length close keeping the lease.
- **Adapter tests, 8:** adsb.fi failure classes and the unchanged legacy wrapper; OpenSky validation and the check's outcomes.
- **Real Redis, 8:** exact fields, whole-record replacement, wrong and empty tokens, the acquisition snapshot, a real Coordinator going `HEALTHY` → `DEGRADED` → `UNAVAILABLE` → `RECOVERING` → `HEALTHY` with authority unmoved, and the three close cases (with length, no length, backwards).

**Checks that the tests can fail.** Turning a Kafka failure into a health failure failed two tests; removing the per-provider serialization failed the timer race test. Both files were restored byte for byte. Two older tests had encoded the zero-length bug (one expected the member `adsbfi|1000|1000|coordinator_shutdown`) and were corrected to give their segments length.

```text
$ npx tsc --noEmit                        (no output, exit 0)
$ npx prettier --check src/*.ts           All matched files use Prettier code style!
$ npx vitest run
 Test Files  10 passed (10)
      Tests  180 passed (180)
```

The integration files passed five runs in a row together, including the `CLIENT PAUSE` test.

---

## The runtime experiments

Each run used the real coordinator against the live adsb.fi API, without the service's `.env`. A read-only watcher recorded the health hashes on every state change. Timings were scaled through the new settings.

### A. Rollout onto an existing authority

Redis held the initialized adsb.fi authority from CP3b and CP3c and no health hash. The recovery window was 20 s.

```text
provider health restored  adsbfi=unknown opensky=unknown first_deployment=false
RECOVERING state_since_ms=1790368723027 success_streak_since_ms=1790368723027 consecutive_failures=0
HEALTHY    state_since_ms=1790368743962 success_streak_since_ms=
```

The first valid response gave `RECOVERING`, not `HEALTHY`, and `HEALTHY` followed after a 20,935 ms streak. That first response only seeded adsb.fi's freshness, so coverage opened on the next cycle; health counted it as a success.

### B. The `DEGRADED` deadline

Stored health was `HEALTHY` from A. A new acquisition ran with a 1 ms fetch timeout and a 15 s deadline:

```text
20:39:28.236 HEALTHY -> DEGRADED  state_since_ms=1790368768235   (restore)
20:39:28.255 .. 20:39:41.060      five timeouts; next request backed off 14.2 s
20:39:43.240 DEGRADED -> UNAVAILABLE state_since_ms=1790368783235 (= 1790368768235 + 15000)
UNAVAILABLE last_failure_ms=1790368781060 consecutive_failures=5 last_error=timeout
```

The repeated failures did not move `state_since_ms`, and the timer made the provider `UNAVAILABLE` at the logical deadline while the next request was still about 14 s away.

### C. Recovery

With normal access and a 20 s window, the stored `UNAVAILABLE` was kept unchanged, the first success gave `RECOVERING` at 1790368820875, and `HEALTHY` followed at 1790368841925 after 21,050 ms of continuous success.

### D. Kafka does not touch health

With adsb.fi `HEALTHY`, Redpanda was stopped at 20:40:45.730:

| Snapshot | `state` | `consecutive_failures` | `last_failure_ms` | `last_success_ms` |
| --- | --- | --- | --- | --- |
| before the stop | HEALTHY | 0 | 1790368781060 (from B) | 1790368844153 |
| after the first publish error | HEALTHY | 0 | 1790368781060 | 1790368846475 |
| after the second | HEALTHY | 0 | 1790368781060 | 1790368869518 |

Each provider success was recorded before its publish failed, 20.6 s and 25.2 s later. Coverage, separately, closed as `failure` at 1790368844164 and reopened when Redpanda returned. The coordinator's own backoff counted the two Kafka failures; the health counter did not.

### E. OpenSky

One run allowed checks, anonymously, with a one-hour recovering interval so that exactly one check happened:

```text
opensky health check outcome=ok state=RECOVERING credits_remaining=398 next_check_in_ms=3600000
state=RECOVERING state_since_ms=1790368927808 last_probe_ms=1790368927808 paused_until_ms= credits_remaining=398
```

Exactly one live check, nothing published, OpenSky `RECOVERING` with 398 credits left.

### Authority never moved

Through every run and every health change, including adsb.fi being `UNAVAILABLE`, the authority kept `provider=adsbfi`, `epoch=1`, `authority_since_ms=1790350153688`. Only CP3b's own fields changed as coverage opened and closed (`timeline_version` 8 to 16).

### The zero-length coverage fix

Runs C and E each ended with coverage opened by one cycle and closed by the shutdown before another, which wrote `adsbfi|1790368897027|1790368897027|coordinator_shutdown` and `adsbfi|1790368929885|1790368929885|coordinator_shutdown`. After the fix, those two exact members were removed from local Redis with `ZREM` (scores 1790368897027 and 1790368929885), leaving the six segments with length untouched. A targeted run then opened coverage and was stopped 100 ms later:

```text
before: timeline_version=16 members=6
coverage opened  active_success_ms=1790369598755 timeline_version=17
coverage closed with no length: no segment written  reason=coordinator_shutdown timeline_version=18
after:  timeline_version=18 members=6 coverage_open_since_ms='' zero-length members=0
```

The timeline closed and took its revision, and no member was written.

---

## Not tested live

- **An OpenSky `429`.** Reproducing one means spending a whole daily budget. Covered by the reused CP2 capture and by tests driven with its real retry value.
- **A health write failing in a running coordinator.** Covered by the refused and timed-out write tests; the Redis-uncertainty rule is the same one CP3b proved live.
- **A backwards coverage segment.** It cannot be produced by the credit script; covered by the integration test on a hand-written hash.

---

## Deferred and out of scope

- **`TimeoutNegativeWarning`.** It appeared in the Kafka experiment script as well as in the coordinator, whenever a KafkaJS producer connected under Node 23. That it comes from KafkaJS's connect path is an inference, not traced. Harmless (Node clamps it to 1 ms) and deferred.
- **Acting on health.** Authority becoming `none`, selection rounds, failover and failback are CP3e and CP3f, and adsb.fi's 10 s standby check arrives with them.
- **The experiments' health records.** `{live-provider}:health:adsbfi` and `{live-provider}:health:opensky` were left in Redis as valid state; the next coordinator start restores them.

---

## Engineering debrief

**Data flow.** Each adsb.fi cycle fetches, validates and runs the freshness check, then records a health outcome with one lease-checked write, and only then publishes and credits coverage. OpenSky checks fetch and validate on their own timer and write only OpenSky's health. A per-provider timer turns an expired `DEGRADED` into `UNAVAILABLE`. At every acquisition, stored health is read in one transaction and restored before any request.

**Trade-off.** Health is conservative by design: a restart loses recovery progress, unknown health must prove itself for two minutes, and a stored record that cannot be read is treated as unknown. The cost is a slower return to `HEALTHY`, and a few extra OpenSky credits after each restart. In return, no downtime, restart or broker outage can make a provider look healthier than the evidence shows.

**Failure behaviour.** A provider failing without a single request finishing still became `UNAVAILABLE` on time. A broker outage left health untouched while coverage closed. A recovering provider needed its full window of continuous success. In every case authority stayed exactly where it was.

---

## Manual inspection commands

```bash
# Run a coordinator
cd services/ingestion-poller && npm run coordinate

# Provider health
docker exec sentinel-redis redis-cli HGETALL '{live-provider}:health:adsbfi'
docker exec sentinel-redis redis-cli HGETALL '{live-provider}:health:opensky'

# Authority must not move with health
docker exec sentinel-redis redis-cli HMGET '{live-provider}:authority' provider epoch authority_since_ms timeline_version

# Scaled timings for experiments
PROVIDER_DEGRADED_TIMEOUT_MS=15000 PROVIDER_RECOVERY_WINDOW_MS=20000 npx tsx src/coordinator.ts

# Force adsb.fi request failures
ADSBFI_FETCH_TIMEOUT_MS=1 npx tsx src/coordinator.ts

# A fast, reversible broker failure
docker stop sentinel-redpanda
docker start sentinel-redpanda
```

Health changes are logged as `provider health changed` with `from`, `to` and `state_since_ms`; each OpenSky check as `opensky health check` with its outcome, credits and next check.

## Knowledge-check questions

1. In run B, `UNAVAILABLE` was stamped 1790368783235 although the timer ran a few milliseconds later. Why stamp the deadline rather than the moment the timer ran?
2. In run D, `last_success_ms` kept advancing while every publish failed. Which ordering in the cycle makes that true, and what would CP3e do wrongly without it?
3. Run A gave `RECOVERING` on the first success, a true first deployment would give `HEALTHY`. What distinguishes the two, and when is it decided?
4. Why was the zero-length close fixed in the writer rather than by letting the evaluator accept equal start and end times?

## Optional manual tweak

Start the coordinator with `PROVIDER_DEGRADED_TIMEOUT_MS=10000 ADSBFI_FETCH_TIMEOUT_MS=1` for about 20 s, then without the fetch timeout. Predict the `state_since_ms` of each state before looking at the hash, then check that the restore at the second start used the `UNAVAILABLE` stored by the first.

## Next

CP3e: failover to OpenSky, where health starts to drive authority. It starts with its own teach-back and scope confirmation.
