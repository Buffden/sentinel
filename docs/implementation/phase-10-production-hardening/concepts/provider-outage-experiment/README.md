# adsb.fi Outage Experiment

## Objective

Before designing provider health (Phase 10 CP3), we observed what Sentinel actually does when its primary provider stops delivering while the rest of the pipeline keeps running. The code predicted that every airborne aircraft would raise `SIGNAL_LOSS`. This experiment tested that prediction and recorded how the system recovered.

## Setup

The experiment ran on 2026-09-23 in the local Docker Compose stack.

**Infrastructure:** Redpanda, Redis and TimescaleDB.

**Services, each started with its default npm script:**
- the Position Consumer;
- the adsb.fi poller, with its defaults: a 2 s interval and the SF Bay box;
- the Alert Evaluator.

`npm run evaluator` does not load the evaluator's `.env`, so the signal-loss threshold was the code default of **5 min**, not the 15 min set in that file. The evaluator scans every 30 s.

**State before the experiment:**
- Redis held 116 `entity:live` keys, all more than an hour old, and 1,252 existing episode gates.
- The Position Consumer had 585 unread OpenSky records left over from CP2.

## Outage method

1. At 23:06:43 UTC the healthy adsb.fi poller was stopped with SIGINT. Its last successful cycle had finished at 23:06:42.665Z and published 124 aircraft.
2. The poller was immediately restarted with `ADSBFI_FETCH_TIMEOUT_MS=1`. Every request then timed out before adsb.fi could answer.

The outage was produced by configuration only, with no code change. Redpanda, Redis, the Position Consumer and the Alert Evaluator ran throughout.

## Baseline and cohort

**Evaluator baseline.** The evaluator's first scan raised 124 alerts: these are the restart burst described under Results. The next three scans raised none, while the number of live entities grew from 331 to 343.

**Traffic.** adsb.fi published 114 to 124 aircraft per cycle, and the consumer's lag was 0.

**The cohort.** The cohort is the aircraft adsb.fi had reported in the 60 s before the outage: 122 in all. After the cutoff, their final live state showed **78 airborne and 44 on the ground**.

## Results

**The poller stayed alive and quiet.**
- Over 7.7 minutes it logged 21 timed-out requests.
- Its backoff grew to about 50 s, and it never crashed.
- `adsb.raw` stayed at offset 449724 throughout.
- Nothing outside the poller's own logs showed that the provider had failed.

**Mass `SIGNAL_LOSS`.** The 78 airborne aircraft had last updated between 23:06:35 and 23:06:41, so they crossed the 5 min threshold at 23:11:35 to 23:11:41. That was just after the 23:11:31 scan, so **the 23:12:01 scan raised 78 alerts at once, 5 min 19 s after the last successful poll**. That group was 76 cohort aircraft plus 2 the cohort snapshot had missed.

Seven other alerts during the outage were ordinary:
- 5 adsb.fi aircraft that had gone quiet 1.5 to 3.5 min *before* the cutoff;
- 2 cohort aircraft that had also stopped updating before it.

**No duplicate alerts.** The scans after the wave raised none, because the episode gates stopped repeats. The `alerts` topic rose by exactly 85 (from 15791 to 15876), and Redis gates rose from 1,345 to 1,430.

**Grounded aircraft.** None of the 44 grounded cohort aircraft got a gate or an alert. They became invisible with no signal at all.

**Recovery.**
- adsb.fi was restarted normally at 23:14:28, and its first cycle arrived at 23:14:31.
- Within about 50 ms the Position Consumer began logging `signal loss episode cleared`. Each cleared episode moved to a `recent-loss` key with a 120 s TTL.
- **62 of the 78 cleared.** The other 16 were never reported again during observation, most likely because they had left the area during the 7.8 min gap. Their gates stayed with no TTL.
- The evaluator raised no new alerts after recovery.
- No recovery or resolution message was published: `alerts` stayed at 15876.

**Restart burst.** The evaluator's first scan at startup alerted 124 aircraft whose data was hours old, with `dark_since` values hours in the past.

## Conclusions for the architecture

1. **Silence assumed someone was watching.** The evaluator cannot tell "the provider stopped" from "the aircraft went silent". It needs explicit knowledge of when observation actually happened.
2. **Health cannot come from aircraft disappearing.** The poller holds all the evidence about the provider, but none of it leaves the process.
3. **Suppressing alerts only while the provider is down would not be enough.** At recovery, the 16 aircraft that never returned would alert at once, and a scan racing the consumer's first fresh writes could alert returning aircraft too.
4. **Startup after downtime is the same problem.** Time when nothing was observing must not count as silence.
5. **Grounded aircraft need no change.** They are already excluded.

These conclusions led to ADR-022 (Accepted): one ingestion coordinator that owns provider health and authority, a Redis coverage timeline, and signal loss measured as silence observed by the aircraft's owning provider.

## Limitations

- **One run** in one region, at one time of day.
- **The threshold was 5 min, not 15,** because the evaluator's `.env` was not loaded. The timings scale with the threshold; the outcome does not.
- **The outage was a client-side timeout,** which is not a real provider failure, and it required restarting the poller process.
- **The cohort snapshot was slow,** taken over about 30 s while adsb.fi was still updating. Final values were re-read after the cutoff, and 2 wave aircraft fell outside the cohort.
- **The API was not running,** so how alerts appear to an operator, and their lifecycle, were not observed.
- **Old Redis state and the OpenSky backlog were present.** They were identified by provider and age and kept separate from outage effects.
- **Recovery was watched for only about 1.5 min,** so what finally happened to the 16 remaining gates was not observed.
- **Evaluator logs have no timestamps.** Scan times are wall-clock stamps added as each log line arrived.

Raw logs and snapshots stayed in a local scratch area and are not versioned.

## See also

- [ADR-022](../../../../adr/ADR-022-live-provider-health-and-failover.md): the design derived from this experiment
- [ADR-020](../../../../adr/ADR-020-aviation-data-providers.md): the provider strategy that left health and failover open
