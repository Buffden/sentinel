# Evaluator Observed Silence Debrief

Evidence from verifying CP3c on 2026-09-25, against local Redis 7.2.4 and Redpanda, with the coordinator polling the live adsb.fi API over SF Bay. The design is in [observed-silence.md](observed-silence.md).

---

## Before any code: checking the model by hand

Four experiments ran before the evaluator changed.

**What a returning pipeline looks like.** The stack had been down for hours, so live state had expired. Starting the Position Consumer replayed about 3,600 backlog messages from 18 to 20 hours earlier and rebuilt 460 live aircraft, all owned by adsb.fi and all stale: 206 airborne and 254 on the ground, none gated. The replay also refreshed their 24 h TTL. The old evaluator would have alerted all 206 airborne aircraft in its first scan. Under observed silence, the prediction was that the aircraft still absent once coverage returned (201 after two short coordinator runs) would alert together, after one threshold of real coverage. There were no OpenSky-owned aircraft, because no OpenSky poller ran.

**One observed-silence sum by hand.** The coordinator ran twice, with a gap between. The snapshot, taken at 1790350255164, held one closed segment and one open span. Aircraft `a163c2` was airborne and last seen at 1790350183918, during the first run:

| Interval | Overlap with `a163c2`'s silence |
| --- | --- |
| Closed `adsbfi|1790350153688|1790350184876|coordinator_shutdown` | 184876 - 183918 = 958 ms |
| Gap between the runs, 1790350184876 to 1790350218749 | not covered: 0 |
| Open span 1790350218749 to 1790350254149 | 35,400 ms |
| After the last credited success, 1790350254149 to scan time | not yet proven: 0 |

Observed silence was **36,358 ms** against a wall silence of **71,246 ms**. The 34,888 ms difference is exactly the 33,873 ms gap plus the 1,015 ms since the last credited cycle.

**The one-transaction read.** `MULTI`, `HGETALL` authority, `ZRANGE` coverage 0 -1, `EXEC`, as ioredis returns it:

```text
[
 [null, { heartbeat_ms: "1790350266278", provider: "adsbfi", epoch: "1",
          authority_since_ms: "1790350153688",
          coverage_open_since_ms: "1790350218749",
          last_active_success_ms: "1790350269811", timeline_version: "3" }],
 [null, ["adsbfi|1790350153688|1790350184876|coordinator_shutdown"]]
]
```

Each reply is an `[error, value]` pair, so the two halves have to be checked separately, and every value is a string. `redis-cli` flattens this into one list, so only the client's shape is meaningful.

**Source-to-delivery lag.** For the 96 aircraft refreshed during the second run, `last_active_success_ms` minus `last_seen_ms` was: min 2,161 ms, median **2,581 ms**, 75th percentile 4,507 ms, max **22,092 ms**. The freshest cycle alone ranged from 2,161 to 2,499 ms, and the tail is aircraft adsb.fi reports with older positions. A tracked aircraft therefore carries a few seconds of observed silence at each scan, against a 300 s threshold. No correction was added.

---

## Tests

The pure tests were written first and failed with the module missing. After implementation the evaluator suite stood at **114/114**.

- **`signalLossCoverage.test.ts`, 18 pure tests:** no coverage; a segment before, straddling and after `last_seen_ms`; two segments with a gap; provider isolation; the open span; an open span for another provider ignored; an open span that is not positive; ends cut at the scan time; overlapping and touching segments merged; malformed members skipped and counted; a missing provider; `last_seen_ms` older than all retained coverage; missing, heartbeat-only, and no-provider or no-epoch authority treated as uninitialized; `timeline_version` reported.
- **`evaluator.integration.test.ts`, 6 existing signal-loss tests migrated** to seed their own coverage under their own timeline keys and scan only their own entities.
- **7 new integration tests against real Redis and Kafka:** wall silence over the threshold but observed silence under it raises nothing; silence split across a closed segment and the open span alerts with the same deterministic `alert_id`; an uninitialized timeline raises nothing and warns exactly once with counts 2, 2, 2; a heartbeat-only record behaves the same; an OpenSky-owned aircraft on adsb.fi-only coverage raises nothing while an adsb.fi aircraft in the same scan does; an existing gate is left byte for byte unchanged; a broken authority read and then a broken coverage read each raise nothing and log the right failure.
- **2 regression tests for timeline failures:** with the authority read broken, and then the coverage read, one simulated leader tick runs the scan and two proximity candidates. The scan resolves without throwing, the dark aircraft gets no gate and no alert, a plain candidate still produces `UNSCHEDULED_PROXIMITY`, and a candidate whose aircraft already had an active loss still produces `COMPOSITE`.

The first migrated test also checks that the payload keys are unchanged and that the detection log carries `provider`, `observed_silence_ms` and `wall_silence_ms`, and the scan summary `timeline_version`.

**Checks that the tests can fail.** With the decision temporarily switched back to wall silence, the four tests that separate observed from wall silence failed. With a timeline failure temporarily made to throw instead of return, both regression tests failed. Each time the original file was restored.

```text
$ npx tsc --noEmit                        (no output, exit 0)
$ npx prettier --check src/*.ts           All matched files use Prettier code style!
$ npm run test:coverage
 Test Files  5 passed (5)
      Tests  114 passed (114)
```

---

## The runtime experiment

The Position Consumer, coordinator and evaluator ran as normal processes against the live adsb.fi API. No gates and no alerts were deleted or consumed at any point; every conclusion below comes from Redis key counts, the `alerts` topic high watermark, and the records on the topic.

**The threshold in use.** `npm run evaluator` starts the evaluator without an env file, `config.ts` does not load `.env`, and the running process's environment had no override. The threshold was therefore the code default, **300,000 ms**, with scans every **30 s**.

### Stage 0: startup with no coverage, then the returning-coverage wave

The evaluator started first, with the timeline closed and 237 airborne adsb.fi aircraft already past the threshold on wall-clock time. Its first scans reported `scanned: 548, alerted: 0`; gates stayed at 1287 and the high watermark at 15949.

The coordinator then started at 15:50:15 UTC. Its first cycle logged `freshness: seeded, coverage_credited: false`, and the second `fresh, coverage_credited: true`, opening coverage at 1790351418847. As real coverage built up, the returning-coverage wave arrived over five scans (198, 1, 3, 1 and 23 alerts):

| | Count |
| --- | --- |
| `SIGNAL_LOSS` records on `alerts` (high watermark 15949 to 16175) | 226, all adsb.fi |
| Backlog aircraft, wall silence 1 h to 31.5 h | 198 |
| Aircraft that went silent during the window | 28 |
| Observed silence at detection | 302,824 to 328,014 ms |

The backlog aircraft alerted about 160 s into this run rather than 300 s, because they already carried 140 s of covered silence from the earlier experiment's segments. With coverage healthy, the background rate was then about one real signal loss per scan.

### Stage 1: healthy coverage

| | Value |
| --- | --- |
| Authority | `adsbfi`, epoch 1, `timeline_version` 5 |
| Open span | 1790351418847 to 1790351866499 |
| Outage cohort | 46 airborne, ungated adsb.fi aircraft seen in the last 60 s |
| Gates | 1501 |
| `alerts` high watermark | 16178 |

### Stage 2: `kill -9` of the coordinator

The coordinator was killed at 15:57:48 UTC with coverage open. Its last credited success was 1790351866499, and the unclean stop left the segment open, as expected. Sampled every 60 s for 390 s:

```text
t=1790351882187 la=1790351866499 open=1790351418847 lease=0 HWM=16178 gates=1501
...
t=1790352310831 la=1790351866499 open=1790351418847 lease=0 HWM=16178 gates=1501
```

- `last_active_success_ms` stayed at 1790351866499, so the open span ended there throughout.
- The `alerts` high watermark stayed at **16178** and gates at **1501**.
- All **14** evaluator scans during the outage reported `alerted: 0`, including the organic one-per-scan trickle.
- The largest observed silence among absent cohort aircraft stayed at 62,506 ms.

**One aircraft, `abec03`.** Last seen at 1790351865978:

| Sample | Wall silence | Observed silence |
| --- | --- | --- |
| Outage, first | 17,255 ms | 521 ms |
| Outage, sixth (past the threshold) | 323,615 ms | 521 ms |
| Outage, last | 446,063 ms | 521 ms |
| 20 s after recovery | 494,344 ms | 20,705 ms |

The 521 ms is 1790351866499 minus 1790351865978, the covered time between its last position and the last credited success. After recovery it is 521 plus 20,184 ms of new coverage (1790352358663 minus 1790352338479). The outage contributed nothing.

### Stage 3: recovery

The coordinator restarted at 16:05:35 UTC, after **467,404 ms** down:

```text
"message":"lease acquired: now leader"
"message":"coverage closed","reason":"coordinator_down","segment":"adsbfi|1790351418847|1790351866499|coordinator_down","timeline_version":6
"message":"poll cycle complete",...,"freshness":"seeded","coverage_credited":false
"message":"poll cycle complete",...,"freshness":"fresh","coverage_credited":true
coverage_open_since_ms 1790352338479, timeline_version 7
```

The stale segment closed at the old last success, the first cycle seeded freshness without credit, and the second opened new coverage. The uncovered gap is 1790351866499 to 1790352338479, **471,980 ms**.

After 340 s of renewed coverage, the outage cohort, compared with each aircraft's `last_seen_ms` at the moment of the kill:

| Group | Aircraft | Alerts |
| --- | --- | --- |
| Reappeared | 26 | 0, no gate on any of them |
| Stayed absent | 20 | 20, each only after total observed silence crossed the threshold |

The 20 fired at 302,972 to 328,371 ms of observed silence, and each alert's `dark_since_ms` equals that aircraft's last position before the kill. They had carried 0.5 to 62.5 s of covered silence into the outage, so they needed between 243.6 and 302.5 s of renewed coverage. The invariant is a full threshold of total observed coverage since last seen, not a full threshold of renewed coverage.

Six more records landed in the same window, all legitimate. Five were aircraft already 184 to 285 s into covered silence at the kill, just outside the cohort's 60 s freshness filter; `a8b5ad` was 29 s from alerting when the coordinator died, stayed frozen for the whole outage, and alerted at 300,327 ms of observed silence. The sixth went dark after recovery.

### Stage 4: evaluator and pipeline downtime

The coordinator and evaluator were both stopped at 16:12:08 UTC. The coordinator closed its segment cleanly as `adsbfi|1790352338479|1790352722466|coordinator_shutdown` (version 8). They stayed down until every position was more than 300 s old by wall clock. Then only the evaluator restarted, with no new coverage:

| | Before restart | After three scans |
| --- | --- | --- |
| Ungated airborne adsb.fi aircraft past the threshold by wall clock | 44 | |
| Past the threshold by observed silence | 0 | |
| Scan results | | `alerted: 0`, three times |
| `alerts` high watermark | 16205 | 16205 |
| Gates | 1523 | 1523 |

This is the boundary that raised 124 alerts in the original outage experiment.

---

## Not tested live

- **An unreadable timeline in a running evaluator.** Covered by the integration tests, which give the authority or coverage key the wrong type so the read fails inside the real transaction.
- **Malformed or overlapping coverage members.** The coordinator never writes them; covered by the pure tests.
- **OpenSky-owned aircraft.** None existed during the runs, since the coordinator is the only publisher. Covered by the integration test.

---

## Deferred and out of scope

These were seen during CP3c and deliberately left alone:

- **Coordinator `TimeoutNegativeWarning`.** At startup the coordinator logs a negative timeout equal to minus the current epoch in milliseconds, apparently a first-cycle delay computed from a zero "last request" time. Node clamps it to 1 ms, so it is harmless. It is a CP3b coordinator quirk.
- **Historical `alert-state:*` gates.** Over a thousand gates from earlier runs have no expiry. The Position Consumer clears a gate when its aircraft reappears, which is why the count fell during the runs, but gates for aircraft that never return stay.
- **A stopped Position Consumer.** If the consumer stops while coverage keeps being credited, aircraft would look silent. That is a consumer failure, not a provider failure, and belongs to the failure lab (ADR-022 non-goals).
- **Evaluator `.env` loading.** The evaluator runs at the 300 s code default because it does not load its `.env`. CP4 configuration work (ADR-022 non-goals).
- **The experiment's real alerts.** 256 real `SIGNAL_LOSS` records from these runs are on the `alerts` topic (high watermark 15949 to 16205). They were left in place; the API will persist them if it consumes from its current offset.

---

## Engineering debrief

**Data flow.** Each scan takes one scan time and reads the authority hash and every coverage member in one transaction. It builds each provider's cleaned intervals once. Then, for every airborne aircraft with an accepted position, it sums the overlap between its provider's intervals and the window since `last_seen_ms`. Only when that reaches the threshold does the old path run: gate check, gate write, Kafka publish.

**Trade-off.** Silence is deliberately undercounted: gaps, the time since the last credited success, the first cycle after each acquisition, and any provider without a timeline all count as unobserved. The cost is that some genuine losses alert later, and OpenSky-owned aircraft cannot alert at all yet. In return an outage, a crash or evaluator downtime can no longer cause a signal-loss alert on its own. The wave of stale aircraft after a long downtime is delayed until real coverage has seen them missing, not removed.

**Failure behaviour.** A `kill -9` outage of 7.8 minutes raised nothing. Recovery closed the stale segment at the last real success and credited none of the gap. Aircraft that came back were never alerted. Aircraft that stayed away alerted only once their total covered silence crossed the threshold. An evaluator restart after downtime raised nothing.

---

## Manual inspection commands

```bash
# Run the pipeline
cd services/position-consumer && npm run consumer
cd services/ingestion-poller && npm run coordinate
cd services/alert-evaluator && npm run evaluator

# The snapshot the evaluator reads, as one transaction
printf 'MULTI\nHGETALL {live-provider}:authority\nZRANGE {live-provider}:coverage 0 -1\nEXEC\n' | docker exec -i sentinel-redis redis-cli

# One aircraft's inputs: compare its last_seen_ms with the segments above
docker exec sentinel-redis redis-cli HMGET entity:live:<icao24> provider on_ground last_seen_ms
docker exec sentinel-redis redis-cli EXISTS alert-state:<icao24>

# Alerts actually produced, not inferred from logs
docker exec sentinel-redpanda rpk topic describe alerts -p
docker exec sentinel-redis redis-cli --scan --pattern 'alert-state:*' | wc -l

# An unclean outage
pkill -9 -f src/coordinator.ts
```

In the evaluator log, each `signal loss detected` line carries `provider`, `observed_silence_ms` and `wall_silence_ms`, and each `scan complete` line carries `timeline_version`.

## Knowledge-check questions

1. During the outage `abec03`'s wall silence passed 300 s while its observed silence stayed at 521 ms. Which authority field kept it there, and what would have happened if the open span ended at the scan time instead?
2. The 20 absent cohort aircraft needed as little as 243.6 s of renewed coverage. Where did the rest of their 300 s come from, and why is counting it correct?
3. In Stage 0, 198 backlog aircraft alerted even though CP3c is meant to stop mass alerts. Why is that wave correct, and what distinguishes it from the one in the original outage experiment?
4. A timeline read fails. Why are proximity and composite alerts unaffected, and what single change to the scan would break that?

## Optional manual tweak

Start the evaluator with `SIGNAL_LOSS_THRESHOLD_MS=120000 npm run evaluator` while the coordinator runs, then stop the coordinator with SIGTERM for three minutes. Predict which aircraft alert while it is down (none should) and which alert within the first two minutes after it restarts, then check your prediction against the `observed_silence_ms` values in the detection log.

## Next

CP3d: per-provider health state machines. It starts with its own teach-back and scope confirmation.
