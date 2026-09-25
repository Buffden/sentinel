# Evaluator Observed Silence: Design and Learning Reference

---

## What this checkpoint does

The outage experiment showed the Alert Evaluator reading "no position for 5 minutes" as "the aircraft went dark", even when adsb.fi, not the aircraft, had stopped. It raised 78 false `SIGNAL_LOSS` alerts in one scan, and an evaluator restart after hours of downtime raised 124 more. CP3b made the coordinator write down when adsb.fi was actually watching. This checkpoint makes the evaluator use that record:

- each signal-loss scan reads the coverage timeline once, atomically;
- an aircraft's silence is counted only over time its own provider was proven to be covering;
- `SIGNAL_LOSS` fires only when that **observed silence** reaches the threshold.

Only the signal-loss scan changed. The alert itself, its identity, its gate and the proximity and composite paths are exactly as before. There is still no provider health, no OpenSky authority and no failover.

---

## In plain language

Imagine a night guard who writes down every hour they were actually at the window. When asked "has that car been gone for five hours?", the guard does not look at the clock. They add up only the hours they were at the window since they last saw the car. Hours spent away from the window prove nothing, so they do not count.

The coordinator's coverage timeline is that logbook, and the evaluator is the guard. If adsb.fi was down for 20 minutes, those 20 minutes are not in the logbook, so no aircraft gets 20 minutes closer to an alert because of them. Time already counted before the outage is still counted after it; the logbook just has a gap in the middle.

---

## Concepts

### Observed silence

For one aircraft, observed silence is the total length of its provider's coverage intervals that fall after its `last_seen_ms` and before the scan time. Each interval contributes the part of it that overlaps the window from `last_seen_ms` to the scan time, and nothing if it does not overlap at all.

```text
time ─────────────────────────────────────────────────────────────►
           last_seen                                        scan time
               │                                                │
coverage:  [=======]          [==================]      [=====]
               ├───┤          ├──────────────────┤      ├─────┤
               counted        counted                   counted
                   └─ gap ────┘                  └ gap ─┘
                   not counted                   not counted
```

Wall silence is simply scan time minus `last_seen_ms`. Observed silence can never exceed it, and it is lower by exactly the gaps.

### Which coverage counts: the owning provider

An aircraft belongs to the provider whose position was last accepted into `entity:live`, which the Position Consumer records in the `provider` field (ADR-022 section 5). Only that provider's coverage counts. An aircraft last reported by OpenSky gains no silence from adsb.fi coverage, because adsb.fi being up says nothing about whether OpenSky would have seen it.

A missing, unknown, or uncovered provider has no intervals, so its aircraft observe no silence and cannot raise `SIGNAL_LOSS`. Today OpenSky has no coverage timeline, so OpenSky-owned aircraft cannot signal-loss alert until OpenSky authority exists (CP3e). ADR-022 accepts that consequence.

### Where the intervals come from

The evaluator builds one view per scan from the two CP3b keys:

- **Closed segments** from `{live-provider}:coverage`, each member `provider|start|end|reason`.
- **The open span** from `{live-provider}:authority`: `coverage_open_since_ms` to `last_active_success_ms`, credited to the authority's provider. It ends at the last credited success, not at the scan time. Time since the most recent successful cycle is unobserved until a later cycle proves it, so a coordinator that has just died cannot make its aircraft look darker every scan.

Before summing, each provider's intervals are cleaned: intervals that do not parse or are not positive are dropped, ends are cut at the scan time, and intervals that overlap or touch are merged. The coordinator never writes overlaps, but a legacy or hand-edited timeline could, and counting the same millisecond twice would make an aircraft look darker than it is. Members that fail to parse are counted and reported in one warning per scan; they never crash the scan.

### One snapshot, one clock

At the start of each scan the evaluator takes one scan time and reads the authority hash and every coverage member in one `MULTI`/`EXEC`. Every aircraft in that scan is judged against the same snapshot and the same time. The single transaction matters: CP3b's close script moves the open span into the sorted set in one atomic step, and two separate reads could see that span twice or not at all. The reads are not per aircraft, so the cost is one round trip per scan however many aircraft there are.

### When the timeline cannot be trusted

- **Uninitialized.** There is no authority hash, or it holds only a heartbeat, or it lacks `provider` or `epoch`. None of the timeline is trusted, closed members included, so every aircraft observes zero silence. The scan still runs and logs one warning with how many entities were scanned, how many were eligible, and how many would have alerted on wall-clock silence alone.
- **Unreadable.** Either half of the transaction fails, or returns the wrong shape. Each half is checked separately. The scan logs one error and ends. Because the scan does nothing but signal loss, ending it disables only signal loss; proximity and composite run in the separate candidate consumer. The scan returns rather than throws, because a throw would end the leader session and take that consumer down with it.

In both cases there is deliberately no fallback to wall-clock silence. Falling back would bring back exactly the outage alerts this checkpoint removes.

### What did not change

- Grounded aircraft are still skipped, and entities without an accepted position are still ignored.
- `dark_since_ms` is still the aircraft's source-time `last_seen_ms`, and `alert_id` is still `{entity_id}:SIGNAL_LOSS:{dark_since_ms}`.
- The `alert-state` gate is still written before the Kafka publish, and an existing gate still suppresses the alert.
- The Kafka alert payload is unchanged. Observed silence, wall silence and provider appear only in the detection log line; `timeline_version` appears in the scan summary.

### Mixing two clocks

`last_seen_ms` is source event time; coverage segments are the coordinator's processing time. ADR-022 accepts that mix as an explicit exception. Positions reach the coordinator a few seconds after their source time (a median of about 2.6 s was measured), so a continuously tracked aircraft carries a few seconds of observed silence at each scan, against a threshold of minutes. No correction factor is applied.

### What this delays but does not remove

Aircraft that genuinely disappeared while nothing was watching, such as stale backlog entries after a long downtime, still alert once their provider has covered a full threshold of their absence. That returning-coverage wave is intended: after that much real coverage, "not seen" is real evidence. What disappears is the wave caused merely by wall time passing.

---

## Ownership

| Part | Owner | Reads | Writes |
| --- | --- | --- | --- |
| Coverage snapshot and observed silence | Alert Evaluator, signal-loss scan | `{live-provider}:authority`, `{live-provider}:coverage`, `entity:live:*` | Nothing new. The gate and alert writes are unchanged |
| Coverage timeline | Ingestion coordinator (CP3b) | | `{live-provider}:authority`, `{live-provider}:coverage` |
| Entity ownership | Position Consumer | | `provider` in `entity:live:*` |

The evaluator only reads the timeline. It never repairs, prunes or extends it.

---

## Failure modes

**adsb.fi or the coordinator stops, cleanly or not.** `last_active_success_ms` stops moving, so the open span stops growing and observed silence freezes. No aircraft alerts during the outage, however long it lasts.

**The coordinator comes back.** The new lease holder closes the old segment as `coordinator_down` at the old last success, seeds freshness on its first cycle without credit, and opens new coverage on the next fresh one. The outage gap is never covered, so it adds nothing. Aircraft that reappear refresh `last_seen_ms` and start from zero. Aircraft that stay absent alert once their total observed silence reaches the threshold, counting coverage from before and after the outage.

**The evaluator restarts after downtime.** Wall time has passed but coverage has not grown, so the first scans raise nothing.

**The timeline is missing, heartbeat-only or unreadable.** No signal-loss alerts that scan, one log line explaining why, and no effect on proximity or composite alerts.

**Malformed or overlapping coverage data.** Bad members are skipped and counted; overlaps are merged. Silence can be undercounted, never overcounted.

**An OpenSky-owned aircraft.** No OpenSky coverage exists yet, so it cannot signal-loss alert. Accepted in ADR-022 until OpenSky authority arrives.

---

## Map to code

| Concept | Where |
| --- | --- |
| Parsing members, the open span, cleaning intervals, observed silence | `buildCoverageSnapshot`, `observedSilenceMs`, `services/alert-evaluator/src/signalLossCoverage.ts` |
| One-transaction read and its failure handling | `readCoverageSnapshot`, `services/alert-evaluator/src/evaluator.ts` |
| The scan: one time, ownership, the observed-silence decision, the once-per-scan warnings, diagnostic log fields | `runScan`, `services/alert-evaluator/src/evaluator.ts` |
| Timeline keys and the test-isolation options | `PROVIDER_AUTHORITY_KEY`, `PROVIDER_COVERAGE_KEY`, `SignalLossScanOptions`, `services/alert-evaluator/src/evaluator.ts` |
| Tests | `signalLossCoverage.test.ts`, `evaluator.integration.test.ts` |
| Decision | ADR-022 section 5 (observed silence and ownership) and section 7 (timeline reads) |

`runScan` takes optional timeline keys and an entity key pattern, defaulting to the production keys and `entity:live:*`. Tests use their own keys and scan only their own entities, so they never judge, gate or alert real aircraft in a shared Redis. It is the same code path, not a test mode.

---

## Retention questions

1. An aircraft has 62 s of covered silence when the coordinator is killed, and the outage lasts 8 minutes. After recovery, how much renewed coverage does it need before it alerts at a 300 s threshold, and why is that correct?
2. Why does the open span end at `last_active_success_ms` rather than at the scan time?
3. Why is an uninitialized timeline treated as "no coverage at all", even when closed members exist?
4. Why is there no wall-clock fallback when the timeline cannot be read, and what would a fallback reintroduce?
5. A timeline read fails. Why does `runScan` return rather than throw, and what else would stop if it threw?
6. Why must authority and coverage be read in one transaction?
7. Why can an OpenSky-owned aircraft not raise `SIGNAL_LOSS` today, and which later checkpoint changes that?

---

## Completion checklist

- [ ] I can compute observed silence by hand from `last_seen_ms`, the closed members and the open span
- [ ] I can explain why the open span ends at the last success and what that means during an outage
- [ ] I can say which provider's coverage counts for an aircraft and where that comes from
- [ ] I can explain uninitialized versus unreadable timelines and what the evaluator does for each
- [ ] I can explain why a timeline failure cannot affect proximity or composite alerts
- [ ] I can explain why the returning-coverage wave is delayed rather than removed, and why that is intended
