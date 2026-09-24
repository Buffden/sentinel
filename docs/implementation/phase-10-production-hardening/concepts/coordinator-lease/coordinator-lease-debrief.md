# Coordinator Lease and Heartbeat Debrief

Evidence from verifying CP3a on 2026-09-24, against local Redis 7.2.4 and Redpanda, with the coordinator polling the live adsb.fi API. The design is in [coordinator-lease.md](coordinator-lease.md).

---

## Before any code: the lease by hand

The teach-back and a manual Redis lease experiment were done in the previous session, before the coordinator code was written. The developer ran it against real Redis and it passed. Its raw output was not captured, so it is not reproduced here. Everything below was captured in this session.

---

## Review before running

The uncommitted implementation was reviewed against ADR-022 and the approved CP3a scope before anything ran. The scope was clean: no `provider`, `epoch`, coverage, health or timeline fields or behaviour, and the authority hash holds only `heartbeat_ms`. Three gaps were found and fixed before closing the checkpoint:

- **The timing check was too weak.** It required interval + timeout < TTL. The real worst case is interval + 2 x timeout (see the concept doc). The defaults were safe (9 s against 15 s), but TTL 10 s, interval 5 s, timeout 4 s passed the old check with a 13 s worst case. Fixed, with tests.
- **A shutdown race.** A cycle from a lost lease that finished after the process had regained the lease cleared the record of the new cycle, so shutdown released the lease without waiting for the new publish. Fixed, with a regression test (below).
- **No real-Redis test of a renewal timeout.** Added (below).

---

## The shutdown race, reproduced before the fix

The regression test was written first and run against the unfixed code. The steps: the old cycle is in flight, the lease is lost, the process regains it and starts a new cycle, the old cycle finishes, then shutdown begins. Shutdown must still be waiting on the new cycle.

```text
× still waits for the new cycle at shutdown when an old cycle finishes after reacquisition
AssertionError: expected true to be false // Object.is equality
      Tests  1 failed | 13 passed (14)
```

`true` is "shutdown already finished": it had released the lease with the new cycle still in flight. With the fix (a finishing cycle clears the in-flight record only if the record still refers to it), the same test passes, and the new cycle publishes before the release.

---

## A timed-out renewal still runs

The fail-closed test needed a real command timeout, not a mocked rejection. Redis 7's `CLIENT PAUSE <ms> WRITE` holds write commands, including write scripts, inside Redis while reads still work, so a renewal sent during the pause gets no reply. A direct probe first, with a 300 ms client timeout and a 5 s lease:

```text
acquired b5bb06dd-2140-4743-b619-6c0a52d647c5 pttl 4999
renew rejected after 302 ms: Command timed out
GET while paused true pttl 4693
after unpause pttl 4527 (>4000 means the timed-out renewal still executed)
```

The renewal timed out at the client, and the key still held the old token. But once writes resumed, Redis ran the renewal anyway: without it the remaining TTL would have been about 3.2 s, not 4.5 s. Its consequences, and why it is a liveness delay rather than split brain, are in the concept doc under the known limitation.

The integration test uses the same mechanism with a real `Coordinator`, a real `CoordinatorLease` and scaled-down timings (200 ms + 2 x 250 ms < 2000 ms). It checks, in order:

- the coordinator logs `lease lost` with `Command timed out` and stops being leader;
- the key still holds the old token, never deleted;
- nothing is published after the loss;
- after unpause, the stale renewal restores the TTL under the old token;
- once that key expires, the coordinator leads again under a new token and resumes publishing.

It passed six runs in a row. It lives in the same file as the lease tests on purpose: `CLIENT PAUSE` freezes the whole Redis instance, and vitest runs tests within one file in order but separate files in parallel.

---

## Automated checks

```text
$ cd services/ingestion-poller
$ npx tsc --noEmit
(no output, exit 0)

$ npx vitest run --exclude '**/*.integration.test.ts'
 Test Files  5 passed (5)
      Tests  59 passed (59)

$ npx vitest run src/coordinatorLease.integration.test.ts
 Test Files  1 passed (1)
      Tests  9 passed (9)

$ npx vitest run
 Test Files  6 passed (6)
      Tests  68 passed (68)
```

Prettier passes on every changed file, and `git diff --check` is clean. `ci.yml` fails a Prettier check, but it failed identically before this checkpoint (double-quoted job names), and lint-staged formats only `*.ts`.

The new tests:

- **`coordinator.test.ts`, 10 tests** with a controllable lease and fake timers: polls while leader; never fetches or publishes as a follower; takes over at the next retry; fails closed when renewal returns 0 and when it errors or times out, without deleting the key; returns to follower and can lead again; discards a cycle fetched after the lease was lost; the shutdown race above; shutdown order (publish, then stop renewal, then release); no release when not leader.
- **`coordinatorLease.integration.test.ts`, 9 tests** against real Redis: one acquirer with a TTL; a fresh token per acquisition; `heartbeat_ms` from Redis `TIME`, advancing; only `heartbeat_ms` in the authority hash; renewal extends the TTL; a wrong token neither renews nor moves the heartbeat; a follower acquires after expiry and the old token cannot resume; compare-and-delete releases only its own lease; the fail-closed timeout test above.
- **`config.test.ts`, 4 tests:** the defaults (5 s + 2 x 2 s < 15 s) pass; TTL 10 s, interval 5 s, timeout 4 s fails; equality fails; 1 ms below equality passes.

---

## The two-coordinator experiment

**Method.** Only Redis and Redpanda were started. Coordinator A started, then coordinator B 6 seconds later, both with default timings (TTL 15 s, renewal and follower retry 5 s, command timeout 2 s). Both ran as `node --import tsx src/coordinator.ts`, not through `npm` or the `tsx` wrapper, so `kill -9` would hit the real process rather than orphan a child. No standalone poller was running, and no `{live-provider}` keys existed at the start.

**While A led.** Sampled every 4 seconds:

```text
lease=f55f0a9a-8850-41a2-8376-ab96a9986d44 pttl=11589 heartbeat_ms=1790276417247
lease=f55f0a9a-8850-41a2-8376-ab96a9986d44 pttl=12357 heartbeat_ms=1790276422248
lease=f55f0a9a-8850-41a2-8376-ab96a9986d44 pttl=13119 heartbeat_ms=1790276427250
lease=f55f0a9a-8850-41a2-8376-ab96a9986d44 pttl=13877 heartbeat_ms=1790276432253
lease=f55f0a9a-8850-41a2-8376-ab96a9986d44 pttl=14659 heartbeat_ms=1790276437257

$ redis-cli HGETALL '{live-provider}:authority'
heartbeat_ms
1790276437257
```

- **The heartbeat advanced about every 5 seconds** (5001, 5002, 5003, 5004 ms apart), and the TTL never fell below 10 seconds.
- **The authority hash held only `heartbeat_ms`.**
- **Only A published.** A ran 13 cycles and published 2296 messages. The `adsb.raw` high watermark went from 455931 to 458227, exactly 2296. B ran no cycles and logged one line:

```text
{"timestamp":"2026-09-24T19:00:18.199Z","level":"info","service":"ingestion-coordinator","instance_id":"9dd0adb7-6922-4414-b9eb-3438df54ec38","message":"lease held by another coordinator: waiting as follower","holder_token":"f55f0a9a-8850-41a2-8376-ab96a9986d44","retry_ms":5000}
```

**Killing A.** A was killed with `kill -9`, so it could not release the lease. It had just renewed: the lease had 14965 ms left at the kill. A's last completed cycle was at 19:00:41.231 and the kill at 19:00:42.320, so a fetch was in flight and died with the process.

```text
kill_ms=1790276442320 pttl_at_kill=14965
key expired observed at 1790276457346
{"timestamp":"2026-09-24T19:00:58.221Z",...,"message":"lease acquired: now leader","lease_token":"b6918af0-cc8d-4ef8-8a15-c795c9fecdc0"}
takeover_ms=15901 (kill -> B acquired); expiry_after_kill_ms=15026; expiry_to_acquire_ms=875
```

**Takeover took 15.901 s**: 15.026 s for the key to expire, then 0.875 s until B's next retry. With the defaults the expected range is 10 to 20 seconds (the TTL left at the kill, plus up to one follower retry).

**B resumed where A stopped.** B's first cycle started at offset 458227, A's ending high watermark, so nothing was published in between:

```text
{"timestamp":"2026-09-24T19:00:58.835Z",...,"message":"poll cycle complete","aircraft_in_response":204,"published":175,...,"first_offset":"458227","lease_token":"b6918af0-cc8d-4ef8-8a15-c795c9fecdc0"}
```

Over the next 8 seconds B ran 4 cycles and published 701 messages, and the high watermark rose by exactly 701. A's total stayed at 2296.

**A's old token was refused.** Using A's token directly against the lease:

```text
renew with A token -> 0
release with A token -> 0
SET NX with A token -> (nil)
lease after attempts=b6918af0-cc8d-4ef8-8a15-c795c9fecdc0
heartbeat before=1790276463223 after=1790276463223
```

It could not renew, release or reacquire, and the heartbeat did not move.

**Clean shutdown of B.** SIGTERM:

```text
{"timestamp":"2026-09-24T19:01:07.482Z",...,"message":"shutdown initiated","signal":"SIGTERM"}
{"timestamp":"2026-09-24T19:01:07.483Z",...,"message":"lease released","lease_token":"b6918af0-cc8d-4ef8-8a15-c795c9fecdc0"}
{"timestamp":"2026-09-24T19:01:07.483Z",...,"message":"shutdown complete"}
```

B exited 0 and the lease key was gone. The authority hash (no TTL) was deleted by hand afterwards. About 3000 real adsb.fi messages from the run remain in `adsb.raw`. The Position Consumer processes them normally on its next run; its writes are idempotent and newest-wins.

---

## Separate observation: kafkajs `TimeoutNegativeWarning`

Both coordinators printed this at startup:

```text
(node:11532) TimeoutNegativeWarning: -1790276412245 is a negative number.
Timeout duration was set to 1.
```

It is the same warning recorded in the CP2 debrief and the Phase 10 Starting State. A bare kafkajs producer connect with `--trace-warnings` points to `RequestQueue.scheduleCheckPendingRequests` in kafkajs 2.2.4 on Node 23.11.0, before any coordinator code runs. It existed before CP3a and was not fixed here. It remains deferred to CP4.

---

## Engineering debrief

**Data flow.** A coordinator starts as a follower and tries `SET NX PX` every 5 seconds. Once it holds the lease it stamps the heartbeat, then renews every 5 seconds, each renewal writing `heartbeat_ms` in the same atomic script. Each 2 second cycle fetches from adsb.fi, checks the token is still held, and publishes to `adsb.raw`. Any renewal it cannot confirm sends it back to follower mode without touching the key.

**Trade-off.** A lease with expiry, rather than fencing, keeps the coordinator simple and needs no change downstream. The cost is the gap in ADR-022 section 8: a coordinator frozen past its TTL can still finish a send. A second, smaller cost appeared in this checkpoint: a timed-out command can still run in Redis, which can delay takeover by one TTL. Both are accepted for one active coordinator per deployment.

**Failure behaviour.** A killed leader stops publishing at once, its lease expires after at most 15 seconds, and a follower takes over at its next retry and continues from the same Kafka offset. The old token is useless afterwards.

---

## Manual inspection commands

```bash
# Run one coordinator (add a second terminal for a follower)
cd services/ingestion-poller && npm run coordinate

# Who holds the lease, and how long is left
docker exec sentinel-redis redis-cli GET '{live-provider}:lease'
docker exec sentinel-redis redis-cli PTTL '{live-provider}:lease'

# The heartbeat: run twice, 5 seconds apart, and compare
docker exec sentinel-redis redis-cli HGETALL '{live-provider}:authority'

# Check only the leader publishes: compare with the leader's published counts
docker exec sentinel-redpanda rpk topic describe adsb.raw -p

# Tidy up after an experiment (the authority hash has no TTL)
docker exec sentinel-redis redis-cli DEL '{live-provider}:authority'
```

## Knowledge-check questions

1. Takeover took 15.9 seconds, but the lease TTL is 15 seconds. Where did the extra 0.9 seconds come from, and what is the longest takeover the defaults allow?
2. B's first `first_offset` equalled A's final high watermark. What does that prove, and what would a gap or overlap have meant?
3. Why could A's token not reclaim the lease with `SET NX`, even though it was once the valid holder?
4. In the timeout probe, why does a remaining TTL of 4.5 s instead of 3.2 s prove the timed-out renewal ran?

## Optional manual tweak

Start one coordinator with `COORDINATOR_REDIS_COMMAND_TIMEOUT_MS=6000` and read the startup error. Then work out by hand the largest command timeout the default 15 s TTL and 5 s interval allow, and confirm the coordinator starts with that value and refuses one millisecond more.

## Next

CP3b: the adsb.fi authority hash and coverage timeline. It starts with its own teach-back and scope confirmation.
