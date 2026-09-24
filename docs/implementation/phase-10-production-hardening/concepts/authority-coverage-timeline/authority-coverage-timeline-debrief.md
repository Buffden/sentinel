# adsb.fi Authority and Coverage Timeline Debrief

Evidence from verifying CP3b on 2026-09-24, against local Redis 7.2.4 and Redpanda, with coordinators polling the live adsb.fi API. The design is in [authority-coverage-timeline.md](authority-coverage-timeline.md).

---

## Before any code: checking the model by hand

Five direct experiments ran before the ADR clarifications or any code.

**What CP3a leaves behind.** One coordinator ran for 12 s and was stopped with SIGINT. The lease was gone, and the authority hash was left holding only a heartbeat, with no expiry:

```text
HGETALL {live-provider}:authority  ->  heartbeat_ms 1790283444182
HLEN=1 HEXISTS provider=0 HEXISTS epoch=0 TTL=-1
```

That is exactly the state the bootstrap rule has to recognise as a first deployment, and since it never expires, every real restart will meet it until authority is first committed.

**The timeline on scratch keys.** Played by hand with `MULTI`/`EXEC` on `{lab}:*` keys:

- bootstrap wrote five fields and took `timeline_version` from absent to **1**;
- extending `last_active_success_ms` from 1000 to 3000 left the version at 1;
- closing as `failure` added `adsbfi|1000|3000|failure` with score 3000, cleared `coverage_open_since_ms`, and took the version to 2;
- adding the same member again returned 0, and `ZCARD` stayed 1;
- `ZRANGEBYSCORE 4000 +inf` returned only the segment ending at 9000, and `ZREMRANGEBYSCORE -inf 3000` pruned only the older one.

**A wrong token.** A lease-checked script run with a bad token and with an empty token returned 0, and the hash, set and lease were byte-for-byte unchanged. The same script with the right token did write, so the refusal came from the check.

**Does adsb.fi's `now` advance?** Three requests, 2 s apart:

| Request | `now` | Behind local time |
| --- | --- | --- |
| 1 | 1790283486000 | 599 ms |
| 2 | 1790283488000 | 1015 ms |
| 3 | 1790283490000 | 1585 ms |

It advances, but in whole seconds. Two cycles can legitimately see the same value, so freshness means strictly greater than the highest seen, with a 10 s tolerance rather than a check on every cycle.

**A 1 ms fetch timeout.** Six failed requests over 25 s, with backoff growing to 29.8 s. The `adsb.raw` high watermark stayed at 459868 before and after, so nothing was published.

These runs also showed that a restart with the timeout could never demonstrate a `failure` close: a clean shutdown or the acquisition close would already have closed the segment. The broker experiment below does that instead.

---

## Tests

The coordinator tests were written before the coordinator changed, and 16 of them failed against the unchanged code. The freshness and timeline modules were written just before their own tests. After implementation, the suite stood at **102/102**:

- **`adsbfiFreshness.test.ts`, 5 tests:** seeding, strictly-greater advances, repeated values inside the window, the 10 s frozen boundary, and a fresh tracker per acquisition.
- **`coverageTimeline.integration.test.ts`, 12 script tests against real Redis:** bootstrap from a heartbeat-only hash and from nothing; `provider` without `epoch` counts as uninitialized; extension without a new revision; the backward-clock guard on extending and on opening; close at `last_active_success_ms`, then idempotent no-ops; recovery keeping `epoch` and `authority_since_ms`; both restart reasons; a close on a pre-authority hash writes nothing; every write refused under a wrong or missing token; refusal when another provider holds authority; pruning with the boundary kept.
- **One end-to-end test** with a real `Coordinator`, lease and timeline: bootstrap, extension, a failed publish, recovery, a frozen feed that closes at the last advancing cycle and publishes nothing while frozen, reopening, and clean shutdown, versions 1 to 6 in order.
- **`coordinator.test.ts`, 16 new tests:** acquisition close before the first fetch; no polling when that close fails; seeding; credit after publish at the publish time; zero-message credit; repeated `now` publishes uncredited, then resumes; frozen failure and repeated closes; failed publish and failed fetch close as `failure`; nothing written after losing the lease mid-cycle; credit and close errors fail closed; freshness reset on reacquisition; shutdown closes while renewal still runs.

**A timeline timeout against real Redis.** One gap remained: proving a real timeline command timeout fails closed. A test in `coordinatorLease.integration.test.ts` pauses Redis writes with `CLIENT PAUSE ... WRITE` while the coordinator is crediting every 50 ms. The lease uses a 10 s renewal under a 30 s TTL, so no renewal can fall inside the 800 ms pause, and the renewal count is asserted unchanged. The credit times out with `Command timed out`, and the coordinator:

- logs `lease lost` with `timeline write error`;
- never logs a renewal error;
- leaves the key holding its old token;
- publishes nothing more.

After writes resume, the timed-out credit still lands: `last_active_success_ms` equals exactly the time passed to the timed-out call. It ran under the same token, which still held the key. That is safe, since it credits a cycle whose Kafka publish had already succeeded. The coordinator had still failed closed, because at the time it could not know whether the write had happened. Had the lease passed to another token first, the script's own token check at execution time would have refused it. The wrong-token tests show that refusal, but this test does not delay a write across a real takeover.

**A check that the test can fail.** With the credit error path temporarily changed to swallow the error and carry on, the test failed (`condition not met within 900 ms`: the coordinator stayed leader). The original file was then restored byte for byte.

All `CLIENT PAUSE` tests live in that one file, because vitest runs files in parallel and a pause freezes the whole Redis instance. For the same reason, the end-to-end test in the timeline file now uses a 5 s command timeout, well above any pause. Its earlier 1 s timeout could have been tripped by the other file's 1 s pause. The two integration files passed 8 runs in a row together.

```text
$ npx tsc --noEmit                       (no output, exit 0)
$ npx prettier --check src/*.ts          All matched files use Prettier code style!
$ npx vitest run --exclude '**/*.integration.test.ts'
      Tests  80 passed (80)
$ npx vitest run src/coverageTimeline.integration.test.ts src/coordinatorLease.integration.test.ts
      Tests  23 passed (23)
$ npx vitest run
 Test Files  8 passed (8)
      Tests  103 passed (103)
```

---

## The runtime experiments

One script ran every step against the real adsb.fi API, Redpanda and Redis, starting with no `{live-provider}` keys and no other publisher running.

**1. Bootstrap.** Coordinator A's first cycle only seeded freshness. The second was fresh and committed adsb.fi:

```text
21:35:21.690 cycle freshness=seeded credited=false published=167
21:35:24.037 cycle freshness=fresh credited=true published=166
21:35:24.038 adsb.fi authority committed and coverage opened {'timeline_version': 1, 'active_success_ms': 1790285724037}

heartbeat_ms=1790285721350 provider=adsbfi epoch=1 authority_since_ms=1790285724037
coverage_open_since_ms=1790285724037 last_active_success_ms=1790285724037 timeline_version=1
ZCARD {live-provider}:coverage = 0
```

**2. Normal extension.** Sampled every 3 s (`coverage_open_since_ms`, `last_active_success_ms`, `timeline_version`):

```text
1790285724037 1790285724037 1
1790285724037 1790285726417 1
1790285724037 1790285728616 1
1790285724037 1790285733216 1
```

The end moved forward on every credited cycle. The start and the version did not.

**3. Broker-induced failure.** With the segment open and the last success at 1790285735418, Redpanda was paused with `docker pause`. The experiment waited for a real publish error rather than assuming one:

```text
paused redpanda at 1790285736999
first publish error observed after 192793 ms
"message":"poll cycle error","error":"Request Produce(key: 0, version: 7) timed out"
21:38:49.655 coverage closed {'reason': 'failure', 'segment': 'adsbfi|1790285724037|1790285735418|failure', 'timeline_version': 2}

coverage_open_since_ms= last_active_success_ms=1790285735418 timeline_version=2
lease still held: pttl=11362
```

The segment closed at exactly the last success before the pause, 1790285735418. KafkaJS took **192.8 s** to report the failure, through its request timeouts and retries, so the broker was unusable for more than three minutes before the coordinator heard about it. Coverage did not grow during that time: the stuck publish never completed, so `last_active_success_ms` never moved, and the close ended where coverage truly ended. The heartbeat kept advancing and A kept its lease throughout.

**4. Recovery.** After `docker unpause`, the next cycle was fresh and opened a new segment:

```text
21:38:52.065 coverage opened {'timeline_version': 3, 'active_success_ms': 1790285932064}
provider=adsbfi epoch=1 authority_since_ms=1790285724037 coverage_open_since_ms=1790285932064 timeline_version=3
```

`epoch` and `authority_since_ms` were unchanged. Authority never moved, only coverage.

**5. Clean shutdown.** SIGINT to A:

```text
21:38:56.785 coverage closed {'reason': 'coordinator_shutdown', 'segment': 'adsbfi|1790285932064|1790285936784|coordinator_shutdown', 'timeline_version': 4}
21:38:56.786 lease released
```

**6. Crash and takeover.** A was restarted as A2. Its acquisition close found nothing open (the shutdown had closed it), and its second cycle reopened coverage at version 5. Follower B started. Then A2 was killed with `kill -9` at 1790285946437, its last success being 1790285944524. B acquired the lease 14.0 s later:

```text
21:39:20.449 lease acquired: now leader
21:39:20.451 coverage closed {'reason': 'coordinator_down', 'segment': 'adsbfi|1790285939978|1790285944524|coordinator_down', 'timeline_version': 6}
21:39:20.906 cycle freshness=seeded credited=false published=162
21:39:23.234 cycle freshness=fresh credited=true published=162
21:39:23.236 coverage opened {'timeline_version': 7, 'active_success_ms': 1790285963234}
```

B closed A2's segment at A2's own last success, before B polled. B's own first cycle only seeded, even though its `now` was newer than anything A2 had seen. The 18.7 s from A2's last success to B's first credit is uncovered.

**7. Wrong-token refusal.** B was frozen with SIGSTOP so it could not write. The real credit and close scripts were then run by hand with a bogus token:

```text
credit bogus -> lease_mismatch
close bogus  -> lease_mismatch
IDENTICAL: bogus token wrote nothing
```

B was resumed with SIGCONT and carried on extending under its own token.

**8. An already-closed timeline.** B was stopped cleanly (its `coordinator_shutdown` close took the version to 8, with 4 members). Coordinator C then ran with `ADSBFI_FETCH_TIMEOUT_MS=1` for 15 s:

```text
C request failures: 5, coverage closes logged: 0
timeline_version before=8 after=8; ZCARD before=4 after=4; HWM before=463817 after=463817
```

C's acquisition close, five failure closes and shutdown close all found nothing open and wrote nothing. The authority hash was identical except `heartbeat_ms`.

**The final timeline:**

```text
adsbfi|1790285724037|1790285735418|failure               score 1790285735418
adsbfi|1790285932064|1790285936784|coordinator_shutdown  score 1790285936784
adsbfi|1790285939978|1790285944524|coordinator_down      score 1790285944524
adsbfi|1790285963234|1790285968026|coordinator_shutdown  score 1790285968026
```

Afterwards the `{live-provider}` keys were deleted (DBSIZE 8308 to 8306), Redpanda was confirmed unpaused and healthy, and no coordinator was left running.

The script had one harmless bug: it started A and B in a subshell, so its `wait` on them failed with exit 127, and one snapshot in step 5 was read while A's close was still landing. The processes' own logs above show both shutdowns completed normally.

---

## Not tested live

- **A frozen adsb.fi feed.** There is no way to make the real feed repeat `now`. It is covered by the freshness unit tests, the coordinator unit tests and the real-Redis end-to-end test, which drives a coordinator with a stuck `now` and checks that the close ends at the last advancing cycle and nothing is published while frozen.
- **A timeline write timing out in a running production process.** Covered by the real-Redis timeout test above, not by the runtime script.

---

## Engineering debrief

**Data flow.** Each cycle fetches from adsb.fi, checks `now` against the freshness tracker, publishes every message to `adsb.raw`, and only then runs one lease-checked Redis script. That script commits authority on first use, opens a segment, or extends the open one. A failed cycle runs the close script instead, which Redis treats as a no-op if nothing is open. A new lease holder closes whatever its predecessor left open before it polls.

**Trade-off.** Coverage is deliberately undercounted in three places: the gap before every close, the first cycle after every acquisition, and any published cycle whose credit did not land. In exchange, coverage is never overstated, so a later signal-loss alert built on it can be late but never false. The ~3 minute KafkaJS failure delay shows why the credit has to follow the publish rather than precede it.

**Failure behaviour.** A broker outage closed coverage at the last real success, and recovery opened a new segment. A crash left a segment open that the next holder closed at the dead coordinator's last success. A wrong token wrote nothing. Repeated failures against a closed timeline changed nothing.

---

## Manual inspection commands

```bash
# Run a coordinator
cd services/ingestion-poller && npm run coordinate

# Authority and the open segment
docker exec sentinel-redis redis-cli HGETALL '{live-provider}:authority'

# Closed segments with their end times
docker exec sentinel-redis redis-cli ZRANGE '{live-provider}:coverage' 0 -1 WITHSCORES

# Watch extension without a new revision: run twice, a few seconds apart
docker exec sentinel-redis redis-cli HMGET '{live-provider}:authority' coverage_open_since_ms last_active_success_ms timeline_version

# Force a real publish failure, then recover (KafkaJS may take minutes to report it)
docker pause sentinel-redpanda
docker unpause sentinel-redpanda

# Tidy up after an experiment
docker exec sentinel-redis redis-cli DEL '{live-provider}:authority' '{live-provider}:coverage'
```

## Knowledge-check questions

1. In the broker experiment the close ended at 1790285735418, not at the time of the error 193 s later. What would CP3c have concluded if it had ended at the error time?
2. B's first cycle after the takeover had a newer `now` than A2 ever saw, yet was not credited. Why is that the right behaviour?
3. C ran five failed cycles and two other closes but `timeline_version` did not move. Which property of the close script makes that true?
4. The timed-out credit in the timeout test landed after the coordinator had given up. Why is that safe here, and what stops a late write from landing on a successor's timeline?

## Optional manual tweak

Start a coordinator with `ADSBFI_FROZEN_FEED_MS=1000` and watch its cycle log lines. Normal adsb.fi cycles are about 2.3 s apart and `now` moves in whole seconds, so most cycles should still be fresh. Then work out why a 1 s window is too tight for production, using the three `now` values measured above.

## Next

CP3c: the Alert Evaluator measures signal-loss silence only inside the owning provider's coverage. It starts with its own teach-back and scope confirmation.
