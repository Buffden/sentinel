# adsb.fi Authority and Coverage Timeline: Design and Learning Reference

---

## What this checkpoint does

The outage experiment showed the Alert Evaluator reading "no position for 5 minutes" as "the aircraft went dark", even when the provider, not the aircraft, had stopped. ADR-022's fix is to measure silence only while the authoritative provider was actually delivering. That needs a written record of when it was. This checkpoint builds that record for adsb.fi:

- the `{live-provider}:authority` hash, which now records which provider is authoritative and the state of its open coverage segment;
- the `{live-provider}:coverage` sorted set, one member per closed coverage segment;
- the atomic, lease-checked Redis scripts that are the only way either is written.

Nothing reads the timeline yet. The Alert Evaluator starts using it in CP3c. There is still no provider health, no OpenSky authority and no failover, so once adsb.fi is committed it stays authoritative.

---

## In plain language

Picture a logbook kept by the coordinator. Every time it successfully hands a batch of fresh adsb.fi positions to Kafka, it writes "adsb.fi was watching until now". While things keep working, it keeps moving that "until now" forward on the current page. The moment a cycle fails, it closes the page at the last time it knows for certain adsb.fi was watching, and says why it closed. The next success starts a new page.

The logbook never guesses. The time between the last success and a failure, a crash or a restart is never written down as watched, and neither is any batch whose clock did not move.

---

## Concepts

### The authority record

| Field | Meaning |
| --- | --- |
| `provider` | The authoritative provider. Always `adsbfi` in this checkpoint |
| `epoch` | Authority term. 1 at the first commit, incremented on each later commit, never reset. A restart or lease takeover does not change it. In CP3b there is only one commit, so it stays 1 |
| `authority_since_ms` | When the current authority was committed |
| `coverage_open_since_ms` | Start of the open coverage segment, or empty when coverage is closed |
| `last_active_success_ms` | When the latest credited cycle's publish finished |
| `heartbeat_ms` | Written only by the CP3a lease renewal. Unchanged by this checkpoint |
| `timeline_version` | A revision number for the timeline (below) |

**The bootstrap rule.** A record counts as initialized only when both `provider` and `epoch` exist. A hash holding only `heartbeat_ms` (what CP3a leaves behind, since it never expires) is pre-authority state, handled exactly like a first deployment. Authority is committed by the first credited adsb.fi cycle, in the same atomic step that opens the first coverage segment.

### Coverage segments

A coverage segment is a period in which adsb.fi was delivering fresh observations and every one of them reached Kafka. Only one segment can be open, and it lives in the authority hash as `coverage_open_since_ms` to `last_active_success_ms`. When it closes, it moves into the coverage sorted set as one member:

- **Member:** `adsbfi|<start_ms>|<end_ms>|<reason>`, a plain delimited string. The same segment always produces the same string, so writing it twice stores it once.
- **Score:** the end time, so "segments ending after X" and pruning by age are both single range operations.
- **Reasons in use:** `failure` (a failed active cycle), `coordinator_shutdown` (clean shutdown) and `coordinator_down` (found still open by the next lease holder).

| Event | Effect |
| --- | --- |
| First credited cycle on a pre-authority hash | Commit adsb.fi and open a segment |
| Credited cycle, coverage closed | Open a new segment |
| Credited cycle, coverage open | Extend: move `last_active_success_ms` forward |
| Failed active cycle, coverage open | Close at `last_active_success_ms` as `failure` |
| Failed active cycle, coverage closed | Nothing |
| Clean shutdown | Close as `coordinator_shutdown` |
| A coordinator acquires the lease and finds a segment open | Close it as `coordinator_down`, before polling |

Every close ends at the last credited success, never at the failure, crash or shutdown time. The gap is never counted. A segment opened by one credited cycle and closed before another has no length: since CP3d it closes (one revision) without writing a member.

### What makes a cycle count

An active cycle fails on a fetch error or timeout, an error status, a response that fails validation (including a frozen feed, below), or a failed Kafka publish. A failed publish closes coverage but is not evidence about adsb.fi's health.

A cycle is credited only when it succeeded **and** its data was fresh. The order is fixed: fetch, validate, publish every message, and only then one Redis update stamped with the coordinator's time right after the publish finished. For a valid cycle with nothing to publish, that time is taken right after validation, and the cycle still counts. There is no transaction across Kafka and Redis, so if the process dies or Redis fails after the publish, the delivered positions are simply not credited. That can delay a future signal-loss alert. It can never cause a false one.

Losing the lease in the middle of a cycle is not a failure. This coordinator can no longer write, and the next lease holder closes whatever was left open as `coordinator_down`.

### Freshness: why repeated `now` values do not count

Every adsb.fi response carries `now`, the time of adsb.fi's snapshot. It is in whole seconds. A response whose `now` has not moved is a valid response that proves nothing new: coverage built on it would claim adsb.fi was watching when it may only have been repeating itself. So each lease acquisition keeps a small in-memory tracker, never stored in Redis, of the highest `now` seen and when it last advanced:

- **The first valid response after acquiring the lease seeds the tracker.** It is published, but not credited.
- **A response whose `now` is strictly greater than the highest seen is fresh.** It is published and credited.
- **A response whose `now` has not advanced, for less than 10 s, is unconfirmed.** It is published, but not credited.
- **Once `now` has not advanced for 10 s, the feed is frozen.** The cycle fails validation, is not published, and closes coverage as `failure`.

Because unconfirmed cycles are never credited, `last_active_success_ms` always points at the last cycle whose `now` advanced, and a frozen-feed close ends there without any special handling. Starting the tracker empty on every acquisition means a new lease holder must see `now` advance for itself. That costs one uncredited cycle after each acquisition, the conservative direction.

### `timeline_version` is a revision

Each atomic update that opens or closes a segment or commits authority increments `timeline_version` once. The bootstrap update commits authority and opens coverage together, so it takes the version from nothing to 1, not 2. Extending the open segment does not change it, and neither does a close with nothing to close. A reader can therefore tell a new timeline from an old one, but should not treat the number as an event count.

### One atomic script per write, checked against the lease

There are exactly two timeline writes, each a Redis script that runs atomically. Both first check that the lease still holds this coordinator's token, and write nothing if it does not:

- **Credit** bootstraps, opens or extends. It also refuses to write when the time is not after `last_active_success_ms` (the clock stepped back), so a segment can never start before, or extend backwards past, an earlier success. It refuses with an error if a provider other than adsb.fi holds authority, rather than overwriting it.
- **Close** closes the open segment at `last_active_success_ms` with a reason and prunes expired members. With nothing open it writes nothing at all.

The token check keeps the Redis timeline consistent even though the lease is not fencing. A coordinator frozen past its lease can still finish a Kafka send, but its credit afterwards is refused, so it can never extend or reopen coverage its successor has closed.

### Failing closed

A timeline write that is refused by the token check, errors or times out sends the coordinator back to follower mode, the same rule as a failed renewal. Carrying on would be unsafe: if a failure close did not land, coverage would stay open, and the next success would extend it across the failure.

A timed-out write is not a cancelled one. Redis can still run it after the client has given up. The coordinator has already failed closed by then, because it could not know the outcome. The late write itself stays safe:

- **Same token still valid.** A late credit records a cycle whose Kafka publish had already succeeded, and a late close only closes, so neither overstates coverage.
- **Lease has changed.** The script checks the token when Redis actually runs it, not when it was sent, so a late write from a coordinator whose lease has passed to another token changes nothing.

### Retention

A closed member is kept until its end is older than 87,330 s: live-state TTL (86,400 s) + the largest configured signal-loss threshold (900 s) + one scan interval (30 s), per ADR-022. The coordinator holds this as its own setting, `COVERAGE_RETENTION_MS`. Pruning happens inside the close script, only when a segment actually closes, so there is no separate pruning worker and a close with nothing open is a true no-op.

---

## Ownership

| Part | Owner | Reads | Writes |
| --- | --- | --- | --- |
| Authority record | Ingestion coordinator, through the credit and close scripts | `{live-provider}:lease`, `{live-provider}:authority` | All authority fields except `heartbeat_ms` |
| Coverage segments | Ingestion coordinator, through the close script | `{live-provider}:authority` | `{live-provider}:coverage` |
| Heartbeat | Ingestion coordinator, through the CP3a renewal script | Redis `TIME` | `heartbeat_ms` |
| Freshness tracker | Ingestion coordinator, in memory, per lease acquisition | adsb.fi `now` | Nothing in Redis |

Readers: none yet. The Alert Evaluator reads the timeline from CP3c on.

---

## Failure modes

**adsb.fi request fails, or the response is invalid.** Coverage closes as `failure` at the last success. Later failures are no-ops until a fresh success opens a new segment.

**The broker is down.** The publish fails, and coverage closes as `failure`. A hung broker can take KafkaJS minutes to report the failure (192.8 s was observed). Coverage stays truthful meanwhile, because only a completed publish can move `last_active_success_ms`.

**The feed freezes.** Repeated `now` values publish uncredited for up to 10 s, then fail, and coverage closes at the last cycle whose `now` advanced.

**Crash, or a lost lease.** The segment stays open in Redis. The next coordinator to acquire the lease (a restart, a follower, or the same process later) closes it as `coordinator_down` before polling, ending at the last success.

**A timeline write fails or times out.** The coordinator gives up the lease, and the next holder closes any open segment as `coordinator_down`.

**Publish succeeded, Redis credit did not land.** The positions are delivered but uncredited. Conservative.

**Clock steps backwards.** Credits are refused until the clock passes the last success again. No backward or overlapping segment is ever written.

---

## Map to code

| Concept | Where |
| --- | --- |
| Credit and close scripts, member format, retention cutoff | `services/ingestion-poller/src/coverageTimeline.ts` |
| Freshness tracker | `services/ingestion-poller/src/adsbfiFreshness.ts` |
| Cycle order, credit and failure close, acquisition and shutdown closes, failing closed | `activeCycle`, `creditCoverage`, `closeCoverage`, `attemptAcquire`, `shutdown`, `services/ingestion-poller/src/coordinator.ts` |
| Response `now` handed to the coordinator | `responseNowMs` on `SplitResult`, `services/ingestion-poller/src/adsbfiPoller.ts` |
| Frozen-feed window and retention settings | `ADSBFI_FROZEN_FEED_MS`, `COVERAGE_RETENTION_MS`, `services/ingestion-poller/src/config.ts` |
| Tests | `adsbfiFreshness.test.ts`, `coordinator.test.ts`, `coverageTimeline.integration.test.ts`, `coordinatorLease.integration.test.ts` |
| Decision | ADR-022 sections 3, 5, 6 and 7 |

---

## Retention questions

1. Why does a heartbeat-only hash count as a first deployment, and what would go wrong if it counted as a restart?
2. Why does every close end at `last_active_success_ms` rather than at the time the failure was noticed?
3. Why is the first cycle after every lease acquisition published but not credited?
4. A cycle published successfully but its Redis credit timed out. Why is that safe, and why might the credit still appear later?
5. Why must the acquisition close finish before the first poll?
6. Why does bootstrap take `timeline_version` to 1 and not 2?
7. The lease is not fencing. How does the token check still keep the timeline consistent when an old coordinator publishes after losing its lease?

---

## Completion checklist

- [ ] I can list the authority fields and say which event changes each one
- [ ] I can trace a cycle from fetch to credit and name every point where it can fail
- [ ] I can explain the freshness tracker and why repeated `now` values never extend coverage
- [ ] I can say when `timeline_version` changes and when it does not
- [ ] I can explain the three close reasons and who writes each one
- [ ] I can explain why every timeline write fails closed, and why a late-landing write stays safe
