# Coordinator Lease and Heartbeat: Design and Learning Reference

---

## What this checkpoint does

ADR-022 gives live provider authority to one process, the ingestion coordinator. Everything later in CP3 (the coverage timeline, provider health, failover and failback) assumes that only one coordinator is doing that work at a time. This checkpoint builds that single piece: a Redis lease that decides which coordinator process is allowed to work, and a heartbeat that says the holder is alive.

The coordinator runs the existing adsb.fi adapter from CP1, and only while it holds the lease. A second coordinator started alongside it waits as a follower, publishes nothing, and takes over once the first one dies and its lease expires.

That is all CP3a is. The coordinator has no provider health, no authority record, no coverage timeline, no OpenSky adapter and no failover. The Alert Evaluator is unchanged. The standalone `poll` (OpenSky) and `poll:adsbfi` commands still work exactly as before.

---

## In plain language

Think of a single key to a room. Whoever holds the key may work in the room. The key is on a timer: if the holder does not show it again within 15 seconds, it stops working, and anyone waiting can pick it up. A healthy holder shows the key every 5 seconds, and each time it does, it also writes the time on a board outside, which is the heartbeat.

If a holder cannot show its key, for any reason (Redis refused, Redis did not answer in time, the connection broke), it stops working at once and goes back to waiting. It never throws the key away, because by then the key might already belong to someone else.

The key is not a lock on Kafka. Kafka never asks who holds it. A holder that froze for longer than 15 seconds can wake up and finish a message it had already started sending, even though someone else now holds the key. The lease makes that rare, not impossible.

---

## Concepts

### Acquiring the lease

A coordinator claims `{live-provider}:lease` with a single Redis `SET NX PX` command: set the key only if it does not exist, with a 15 second expiry, in one atomic step. There is no gap between claiming and setting the expiry, so a crash cannot leave a lease that never expires.

The value is a fresh random token for every acquisition, not a stable instance name. If the value were the process name, a restarted process could "renew" the lease of its previous life and skip the expiry that is meant to protect a successor. With a fresh token, a process that lost the lease can only get it back through a new `SET NX`, which succeeds only once the key is free.

A follower retries every 5 seconds. It logs that it is waiting once per follower period, not on every retry.

### Renewal and the heartbeat

The leader renews every 5 seconds with a small Lua script that Redis runs atomically:

1. check that the key still holds this coordinator's token, and stop with 0 if not;
2. reset the expiry to 15 seconds;
3. write `heartbeat_ms` into `{live-provider}:authority`, using Redis `TIME`.

Because the heartbeat is written in the same atomic step as the token check, `heartbeat_ms` advancing always means "the holder of this exact token just renewed". Using Redis `TIME` rather than the coordinator's clock means every writer and reader of the heartbeat shares one clock.

The heartbeat is renewed independently of polling, backoff and pauses. It means only that a coordinator is alive and holds the lease, not that a provider is healthy. ADR-022 treats a heartbeat older than 60 seconds as stale. Nothing reads it yet.

In CP3a the authority hash contains only `heartbeat_ms`. A hash with only that field is **pre-authority bootstrap state**, not an authority record. CP3b must not read such a hash as a fully initialized authority record restored on restart. ADR-022's wording on this is to be clarified in CP3b.

### Failing closed

A renewal can end three ways other than success: Redis answers 0 (the token no longer matches), the command errors, or it times out. In all three cases the coordinator can no longer be sure it holds the lease, so it:

- forgets its token locally;
- stops renewing, polling and publishing immediately;
- does not delete the key, because it may already be a successor's;
- goes back to follower mode.

Every Redis command from the coordinator has a 2 second timeout, so a hung Redis turns into a lost lease rather than an indefinite wait.

### Why the timings are safe

The coordinator must notice a lost lease before the key can expire and a successor can take it. The worst case runs like this. Redis runs a renewal the moment it is sent, restarting the 15 second clock, but the reply takes up to one command timeout to arrive. The next renewal is scheduled 5 seconds after that reply and may itself wait a full command timeout before failing. So from the clock restarting to the coordinator noticing, the worst case is interval + 2 x timeout: 5 + 2 x 2 = 9 seconds, well inside 15.

The coordinator refuses to start if `COORDINATOR_RENEWAL_INTERVAL_MS + 2 x COORDINATOR_REDIS_COMMAND_TIMEOUT_MS` is not strictly less than `COORDINATOR_LEASE_TTL_MS`.

### Checking the lease before publishing

A cycle fetches from adsb.fi, then publishes to `adsb.raw`. The fetch can take seconds, so the coordinator checks again after the fetch that it still holds the same token. If it lost the lease in between, the fetched positions are discarded and logged, not published. This narrows the window in which an old leader can publish after losing the lease. It cannot close it: a Kafka send that has already started cannot be recalled.

### Clean shutdown

On SIGINT or SIGTERM the coordinator follows ADR-022's order:

1. stop starting new cycles and new acquisition attempts;
2. let the in-flight cycle finish, including its publish, while renewal keeps the lease alive;
3. (ADR-022 step 3, closing coverage, does not exist until the coverage timeline does);
4. stop renewal;
5. release the lease with a compare-and-delete script that deletes the key only if it still holds this token.

The lease is held until the last publish has finished, so a successor cannot start publishing while the old leader is still sending. A cycle left over from a lost lease never clears the record of a newer cycle, so shutdown always waits for the current one even after the process has lost and regained the lease.

### Known limitation: a timed-out command can still run

A client timeout does not cancel a command. It only stops the client waiting. The command may already be in Redis's queue or on the wire, and Redis can still run it later. This was observed directly; the evidence is in the debrief.

The consequences:

- **The old token's lease can be extended after the coordinator has failed closed.** The coordinator already forgot its token and stopped publishing, so nothing acts on that lease, but no one else can acquire it until it expires again.
- **Takeover can be delayed by up to one extra lease TTL**, about 15 seconds with the defaults.
- **The same applies to a follower's acquisition.** A `SET NX` that timed out at the client can still succeed in Redis later, leaving a lease that no running process believes it holds, until it expires.

This is a liveness delay, not a safety violation. It is not split brain, because the old coordinator stops publishing as soon as a renewal result is uncertain, and it is unrelated to the fencing gap below. Production behaviour was not changed to remove it in CP3a.

### Not fencing

The lease is a duplicate-instance guard. ADR-022 section 8 states the limit: a coordinator paused past its lease (a long garbage-collection pause, a suspended laptop) can still complete Kafka sends after another coordinator has taken over, and Kafka never checks the token. Enforcing that downstream (epoch fencing) is deferred in ADR-022, since it only matters for running several coordinators on purpose. CP3 supports one active coordinator per deployment.

The standalone `poll:adsbfi` and `poll` commands do not take the lease at all. Running either one alongside a coordinator gives two publishers, so an operator must not do that.

---

## Ownership

| Part | Owner | Reads | Writes |
| --- | --- | --- | --- |
| Lease | Ingestion coordinator | `{live-provider}:lease` | `{live-provider}:lease` (acquire, renew, release) |
| Heartbeat | Ingestion coordinator, inside the renewal script | Redis `TIME` | `heartbeat_ms` in `{live-provider}:authority` |
| adsb.fi polling and publishing | Ingestion coordinator, while leader | adsb.fi | `adsb.raw`, unchanged envelope and keying from CP1 |

No other service reads either key yet.

---

## Failure modes

**The leader is killed without releasing.** The key expires 15 seconds after its last renewal, and a follower acquires it at its next 5 second retry. Observed: 15.9 seconds from `kill -9` to takeover.

**Redis refuses a renewal (token mismatch).** The coordinator fails closed and returns to follower mode. It does not delete the key.

**Redis errors or does not answer within 2 seconds.** Same as above. The renewal may still run later in Redis and delay takeover by up to one TTL.

**The lease is lost while a fetch is in flight.** The fetched positions are discarded, not published.

**The coordinator freezes past its TTL mid-send.** The send can complete after a successor has started. This is the known fencing gap in ADR-022 section 8.

**A standalone poller runs alongside the coordinator.** Two publishers. Not guarded, by design, until ADR-022 retires the standalone commands.

**Redis is down at startup.** Acquisition attempts fail and are logged, and the coordinator keeps retrying as a follower. It publishes nothing. This follows from the code and was not exercised.

---

## Map to code

| Concept | Where |
| --- | --- |
| Acquire, renew and release, and both Lua scripts | `services/ingestion-poller/src/coordinatorLease.ts` |
| Follower and leader loop, fail-closed handling, check before publish, shutdown order | `services/ingestion-poller/src/coordinator.ts` |
| Lease timings and the timing check | `validateLeaseTiming`, `services/ingestion-poller/src/config.ts` |
| adsb.fi fetch reused from CP1 | `fetchAdsbfiCycle`, `services/ingestion-poller/src/adsbfiPoller.ts` |
| Start command | `coordinate` script, `services/ingestion-poller/package.json` |
| Coordinator decisions with a controllable lease | `services/ingestion-poller/src/coordinator.test.ts` |
| Lease scripts and fail-closed timeout against real Redis | `services/ingestion-poller/src/coordinatorLease.integration.test.ts` |
| Timing check | `services/ingestion-poller/src/config.test.ts` |
| Decision | ADR-022 sections 1, 7 and 8 |

---

## Retention questions

1. Why is the lease value a fresh random token instead of the process's name?
2. Why is `heartbeat_ms` written inside the renewal script rather than by a separate command?
3. Walk through the worst case that makes the timing rule interval + 2 x timeout, not interval + timeout.
4. A renewal times out. Why does the coordinator not delete the lease key?
5. Why can a timed-out renewal delay takeover, and why is that not split brain?
6. What does the check between fetch and publish protect against, and what can it not protect against?
7. Why does shutdown wait for the in-flight cycle before releasing the lease?

---

## Completion checklist

- [ ] I can explain how a coordinator acquires, renews and releases the lease, and why each step is atomic
- [ ] I can explain what `heartbeat_ms` does and does not mean
- [ ] I can explain the three ways a renewal fails and what the coordinator does in each
- [ ] I can derive the timing rule and say why the defaults satisfy it
- [ ] I can explain why the lease is not fencing, and the one situation where two coordinators can both publish
- [ ] I can explain why a hash with only `heartbeat_ms` is pre-authority bootstrap state
