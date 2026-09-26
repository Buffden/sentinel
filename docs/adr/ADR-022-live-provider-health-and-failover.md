# ADR-022: Live Provider Health, Failover and Observed Silence

**Status:** Accepted (2026-09-23). Implementation status: CP3a (coordinator lease and heartbeat), CP3b (adsb.fi authority and coverage timeline), CP3c (evaluator observed silence) and CP3d (provider health state machines) implemented; CP3e-CP3f pending.
**Date:** 2026-09-23
**Depends on:** ADR-007 (idempotency key schema), ADR-013 (Node.js ingestion poller), ADR-020 (aviation data provider strategy), ADR-021 (`adsb.raw` provider envelope)

---

## Context

ADR-020 makes adsb.fi the primary live source and OpenSky the fallback, with one authoritative provider at a time and no merging. It leaves three questions open: how provider health is detected, when to fail over and fail back, and how signal loss treats aircraft across an outage or a switch.

The Phase 10 CP3 outage experiment ([provider-outage-experiment](../implementation/phase-10-production-hardening/concepts/provider-outage-experiment/README.md)) cut adsb.fi off while the rest of the pipeline kept running. It found:

- **A mass alert.** All 78 airborne aircraft adsb.fi had just reported raised `SIGNAL_LOSS` in a single scan, 5 min 19 s after the last successful poll. The 44 grounded aircraft raised none.
- **No sign of the outage anywhere.** Nothing recorded that the provider, rather than the aircraft, had stopped.
- **Partial recovery.** After adsb.fi returned, 62 episodes cleared and 16 aircraft never returned.
- **A restart burst.** An evaluator restart after hours of downtime alerted 124 stale aircraft at once.

The evaluator reads "no position for the threshold" as "the aircraft went silent", which assumes a provider was watching the whole time. This decision makes that assumption explicit and enforceable.

---

## Decision

### 1. One ingestion coordinator

One process runs both provider adapters (adsb.fi and OpenSky), decides which provider is authoritative, and is the only writer of provider state to Redis. It replaces the separate `poll` and `poll:adsbfi` commands and reuses the CP1 and CP2 fetch, split and envelope logic.

Within a coordinator holding the lease, at most one adapter publishes to `adsb.raw` at a time: the authoritative adapter, or, during a switch, the single committing candidate. Section 8 describes the split-brain exception.

An **active cycle** is a request by the authoritative provider or by the single committing candidate, and its results are published. A **check** is any other request. A check is health evidence only: it never publishes, opens or extends coverage, or commits authority.

### 2. Provider health

Health is judged only from provider and request evidence, never from individual aircraft. Every request counts: active cycles and checks alike. A request's outcome is recorded as soon as the request and its validation finish, before anything is published. A publish that fails afterwards adds no health failure and does not undo the recorded success.

**A request fails on any of these:**
- a network error or timeout;
- a non-2xx status or a `429`;
- a response that fails provider validation (section 3);
- a frozen adsb.fi feed, meaning its `now` has not advanced for 10 s.

**A valid response with zero aircraft is a success.**

| State | Entered on | Leaves on |
| --- | --- | --- |
| `HEALTHY` | 120 s of all-successful requests in `RECOVERING`, or the first-deployment bootstrap (section 7) | Any failure: `DEGRADED`, except an OpenSky `429` with a retry time, which goes straight to `UNAVAILABLE` (paused) |
| `DEGRADED` | A failure while `HEALTHY`, or restored on restart (section 7) | Success: `HEALTHY`. 60 s after entering `DEGRADED` without a success: `UNAVAILABLE` |
| `UNAVAILABLE` | 60 s in `DEGRADED` without success; any failure while `RECOVERING`; a failure while health is unknown; for OpenSky, a `429` with a retry time from any state (sets `paused_until_ms`); or restored on restart (section 7) | First success: `RECOVERING` |
| `RECOVERING` | The first success after `UNAVAILABLE`, or after unknown or stale health (section 4) | 120 s with every request succeeding: `HEALTHY`. Any failure: `UNAVAILABLE`, clearing the recovery streak and restarting the provider's failure backoff |

`RECOVERING` has no failure tolerance, even while the provider is authoritative.

**Request rates:**

| Provider and state | Rate |
| --- | --- |
| adsb.fi, authoritative | Active cycle every 2 s, with CP1 backoff after failures |
| adsb.fi, otherwise | Check every 10 s, with CP1 backoff while failing |
| OpenSky, authoritative | Active cycle every 25 s |
| OpenSky, `HEALTHY` standby | Check every 15 min (96 credits a day) |
| OpenSky, `DEGRADED` standby | Recheck every 30 s, so a check lands inside the 60 s before `UNAVAILABLE` |
| OpenSky, paused | Nothing until `paused_until_ms`, then an immediate check |
| OpenSky, `UNAVAILABLE` and not paused | Backoff from 60 s, doubling to 15 min |
| OpenSky, `RECOVERING` | Every 25 s |

### 3. A successful active cycle

An active cycle succeeds only when all three hold:
1. **The request succeeds.**
2. **The response passes validation:**
   - adsb.fi: the CP1 `now` check, plus the frozen-feed check;
   - OpenSky: a numeric `time`, and `states` that is an array or `null`.
3. **Every message the cycle produced is published.**

A cycle that produces zero messages succeeds trivially, whether the provider returned nothing or Sentinel's own filters removed every aircraft. If any chunk of the publish fails, the whole cycle fails.

A failed publish fails the active cycle, which closes coverage, but it is never provider health evidence.

### 4. Authority

Authority is `adsbfi`, `opensky` or `none`. It changes only when a successful active cycle commits the change (section 6).

- **Loss of authority.** When the authoritative provider becomes `UNAVAILABLE`, authority becomes `none`. A `HEALTHY` provider reaches this after 60 s through `DEGRADED`, or immediately on an OpenSky `429` with a retry time. A `RECOVERING` one reaches it on its first failure.
- **Leaving `none`.** A selection round tries eligible providers one after another, without waiting between them, until one commits. The order is:
  1. `HEALTHY` providers, adsb.fi first;
  2. then the other eligible providers, adsb.fi first.

  If nothing commits, later rounds follow each provider's own request rate.

| State while authority is `none` | Eligible |
| --- | --- |
| `HEALTHY`, `DEGRADED` or `RECOVERING` | Yes. A success moves the provider along its normal path: `DEGRADED` to `HEALTHY`, and `RECOVERING` stays `RECOVERING` until its window ends |
| Unknown or stale | Yes, one immediate attempt per entry into `none`. A success means `RECOVERING` |
| `UNAVAILABLE`, not paused | Yes, one emergency attempt per entry into `none`, without resetting its backoff |
| `UNAVAILABLE`, paused | No, until `paused_until_ms` |

**Unknown** means the provider has no health record. **Stale** means there has been no request to it for longer than its current check interval. Staleness is derived, never stored: a stale provider, even one stored as `HEALTHY`, gets only the one immediate attempt, in the second group, and a success after staleness enters `RECOVERING` with a fresh streak.

- **One-shot attempts.** The unknown, stale and emergency attempts are once per entry into `none`, not once per retry. After its attempt, a provider is not excluded: each of its later requests, at its own rate, is still a candidate.
- **One request, both purposes.** While authority is `none`, a request to a provider is both its health evidence and a candidate delivery. There is never a separate check followed by a second request to attempt authority.
- **Delivery failures while `none`.** If a candidate's request and validation succeed but its publish fails, or its commit is refused because its time is not after the last success, authority stays `none`. That provider's next candidate delivery backs off from 60 s, doubling to 15 min, on the same per-provider request schedule; there is no separate selection-retry timer. This is not provider health.
- **The `none` record.** On an initialized timeline, `none` is stored literally as `provider=none`. `epoch` is kept, `authority_since_ms` becomes the time `none` began, coverage is closed, `last_active_success_ms` is kept, and the change takes one `timeline_version` revision. A timeline that has never had an authority (no `provider` or `epoch`) is treated as `none` in memory and is not written, so no epoch 0 exists. The first commit, by either provider, creates epoch 1.
- **Commit.** After a successful publish, COMMIT changes only authority, increments the epoch, sets `authority_since_ms`, keeps coverage closed and takes one `timeline_version` revision. CREDIT is separate: it never commits authority and only opens or extends coverage for the provider that already holds it. A seeded adsb.fi response may therefore become authoritative without claiming coverage; a later fresh cycle opens coverage.

- **Voluntary failback** from OpenSky requires adsb.fi `HEALTHY` and at least 5 min since OpenSky's authority was committed. There is no early failback.

### 5. Coverage and observed silence

**Coverage** means the periods when a provider was actually delivering authoritative observations. It is kept separately from authority and from health: one period of authority can contain several coverage segments.

**Segment timestamps.** Segments are stamped with the time a successful active cycle's publish stage completed:
- the first successful cycle opens a segment;
- each later successful cycle extends it.

**adsb.fi freshness.** An adsb.fi cycle is credited only after its `now` is seen to advance. Each lease acquisition starts a coordinator-local tracker of the highest `now` seen and the time it last advanced, kept in memory, not in Redis:
- the first valid response after acquiring the lease seeds the tracker and is not credited;
- a response whose `now` is strictly greater than the highest seen confirms freshness and is credited normally;
- a response whose `now` has not advanced may be published, but is not credited;
- once `now` has not advanced for 10 s, the cycle fails (the frozen-feed check in section 3).

Repeated `now` values never extend coverage, so a frozen-feed close ends at the last cycle whose `now` advanced.

**Closing a segment.** A segment closes at its last successful cycle, with a reason:

| Reason | When |
| --- | --- |
| `failure` | The first failed active cycle, even if the provider stays authoritative |
| `handover_attempt` | OpenSky is stopped for a failback attempt |
| `handover` | A failback has committed |
| `coordinator_shutdown` | Clean shutdown |
| `coordinator_down` | Found open by the next coordinator to acquire the lease: a restart, another coordinator, or the same process reacquiring |

**What never counts as coverage:**
- any period containing a known failure;
- a failback attempt;
- a period when the provider was deliberately inactive;
- the time since the last success, which only counts once the next cycle succeeds.

**Ownership.** An aircraft belongs to the provider whose position was last *accepted* into `entity:live` (its `provider` field). Ownership moves only when another provider's position is accepted. A position rejected by the monotonic guard does not move it.

**Observed silence.** Observed silence is the owner's coverage between `last_seen_ms` and the scan time. `SIGNAL_LOSS` fires only when observed silence reaches `SIGNAL_LOSS_THRESHOLD_MS`. Everything else is unchanged:
- grounded aircraft are skipped;
- the episode gate is written first;
- `dark_since_ms` is still `last_seen_ms`;
- `alert_id` is still deterministic.

**No ceiling.** An aircraft that is never observed again expires at its `entity:live` TTL without an alert.

**Processing time.** Coverage segments use processing time. This is an explicit exception to the source-time default. It affects when an alert fires, never its identity.

### 6. Ordering between Kafka and Redis

There is no transaction spanning Kafka and Redis. Every cycle and every switch runs fetch, validation and publication before any authority or coverage credit. Candidate delivery then COMMITs authority; if that same response qualifies as coverage, CREDIT follows as a separate lease-checked Redis write using the publish-completion time.

**Failback sequence:**
1. OpenSky finishes its cycle and stops. Its coverage closes as `handover_attempt`, while authority stays `opensky`.
2. adsb.fi runs a committing cycle.
3. On success, COMMIT changes authority to `adsbfi`; if that response qualifies for coverage, CREDIT opens coverage separately.

If adsb.fi's fetch, validation or publish fails, OpenSky resumes, and its coverage reopens only at its next successful cycle. A failed publish is not adsb.fi health evidence (section 3).

**Failure windows:**

| Failure | Effect |
| --- | --- |
| The publish fails during a switch | Nothing is committed. A failover stays at `none`, and a failback returns to OpenSky |
| The publish succeeds, then COMMIT fails or its result is unknown | Positions may have been delivered, but authority is not trusted; the coordinator fails closed |
| COMMIT succeeds, then CREDIT fails or its result is unknown | Authority may already have changed, but no new coverage is assumed; the coordinator fails closed. **This is conservative: it can delay a `SIGNAL_LOSS`, never cause a false one** |

### 7. Redis state, heartbeat and restart

All keys share the hash tag `{live-provider}`. The coordinator is the only writer.

| Key | Contents | Read by |
| --- | --- | --- |
| `{live-provider}:lease` | The coordinator's token. TTL 15 s, renewed every 5 s, the Alert Evaluator's lease convention | Coordinator |
| `{live-provider}:authority` (hash) | `provider`, `epoch` (1 at the first authority commit, incremented on each later commit, never reset, unchanged by a restart or lease takeover), `authority_since_ms`, `coverage_open_since_ms` (empty when closed), `last_active_success_ms`, `heartbeat_ms`, `timeline_version` | Evaluator, operators |
| `{live-provider}:coverage` (sorted set) | Closed segments: provider, start, end and reason, scored by end | Evaluator |
| `{live-provider}:health:adsbfi`, `{live-provider}:health:opensky` (hashes) | `state`, `state_since_ms`, `last_success_ms`, `last_failure_ms`, `consecutive_failures`, `last_error`, `success_streak_since_ms`. OpenSky also has `paused_until_ms`, `credits_remaining` and `last_probe_ms` | Operators. **Not used for silence** |

- **Writes.** Every write is an atomic script that checks the lease token first and writes nothing if the token does not match. `timeline_version` is a revision of the timeline, not an event count: COMMIT increments it once for an authority change, CREDIT increments it once when opening coverage, and CLOSE/RELINQUISH increment it when they change the timeline. Extending an already-open segment's `last_active_success_ms` does not increment it.
- **Reads.** At the start of each scan the evaluator reads `authority` and the retained `coverage` in one transaction. It evaluates every aircraft against that snapshot and logs `timeline_version`. A provider's segments are its closed members plus, if that provider is the authority and `coverage_open_since_ms` is set, the open span from `coverage_open_since_ms` to `last_active_success_ms`.
- **Retention.** A `coverage` member is kept until its end is older than live-state TTL + the largest configured signal-loss threshold + one scan interval (today 86,400 + 900 + 30 s). The 900 s is the evaluator's `.env` value. The code default is 300 s, and the outage experiment ran at 300 s because the evaluator did not load `.env`. The coordinator holds this as its own setting. Consistency with the other services' settings is documented, not enforced.
- **Heartbeat.** The lease renewal script writes `heartbeat_ms` every 5 s, independent of polling, backoff or pauses. It means only that the coordinator is alive and holds the lease. It is stale after 60 s.
- **Clean shutdown:**
  1. stop polling;
  2. finish any in-flight cycle;
  3. close coverage as `coordinator_shutdown`;
  4. stop renewal;
  5. release the lease.
- **Lost lease.** A coordinator that loses its lease stops polling, publishing and writing immediately.
- **Startup, with no initialized authority record** (a true first deployment): a record is initialized only when both `provider` and `epoch` exist, so a hash holding only `heartbeat_ms` counts as none. Authority starts `none`. A **true first deployment** is decided at lease acquisition: no initialized authority record and no provider health record. Only then does the first valid adsb.fi response initialize adsb.fi health directly as `HEALTHY`, right after the request and validation and before publishing. Committing authority remains the separate step after a successful publish, so a publish that fails leaves adsb.fi `HEALTHY` with authority still uninitialized. Either provider may make that first commit (section 4).
- **Lease acquisition, with an initialized record** (a restart, another coordinator taking over, or the same process reacquiring): before polling, close any open coverage as `coordinator_down`, and keep the stored `epoch`. Keep the stored authority, unless its provider is restored as `UNAVAILABLE`: then authority becomes `none` and a selection round starts. A provider with no health record, such as the first run of health tracking on an existing authority record, has unknown health: its first success enters `RECOVERING` and its first failure `UNAVAILABLE`. Restore health as follows:

| Stored | Restored as |
| --- | --- |
| `HEALTHY` or `DEGRADED` | `DEGRADED`, with the 60 s clock starting at restart |
| `UNAVAILABLE` or `RECOVERING` | `UNAVAILABLE`, with the success streak cleared |
| OpenSky with a future `paused_until_ms` | Paused until then |

A restart can delay failback, but never speed it up.

### 8. The lease is not fencing

The lease stops a second coordinator from doing work. It does not fence Kafka: a coordinator paused past its lease can still complete sends after another one takes over. That overlap is a **known violation of the single-authority invariant**. Idempotent history and newest-wins live state limit some of its effects. But `position.normalized` is published for every valid position, including ones live state rejects, so the Correlation Worker and Deviation Detector can receive both providers' positions.

CP3 supports one active coordinator per deployment.

### Thresholds

| Setting | Value |
| --- | --- |
| Unavailable (failover) | 60 s after entering `DEGRADED` without a success |
| Recovery window | 120 s, every request succeeding |
| Minimum time on OpenSky before failback | 5 min |
| adsb.fi check rate when not authoritative | 10 s |
| OpenSky check rates | 15 min standby, 30 s recheck when `DEGRADED` on standby, 25 s recovering |
| adsb.fi frozen feed | `now` not advancing for 10 s |
| Lease TTL and renewal (heartbeat) | 15 s and 5 s |
| Stale heartbeat | 60 s |

---

## Deferred

| Item | Reason |
| --- | --- |
| Epoch fencing enforced downstream | Only needed for several coordinators (high availability) |
| A recovery or resolution event on `alerts` | CP3 removes outage-caused alerts. Alert lifecycle belongs to the API |
| A provider-level alert type | Health is visible in logs and Redis. A new alert type is a separate decision |
| Mass alerts from a stopped Position Consumer | A consumer failure, not a provider failure: failure lab |
| Detecting a provider that is fresh but empty | An empty valid response is healthy by decision |
| Evaluator `.env` loading, timestamped JSON evaluator logs | CP4 configuration and logging work |
| Compression codecs | Failure lab |
| Enforcing retention consistency across services | Documented only |

---

## Alternatives Considered

- **Two pollers coordinating through a Redis lease.** Rejected: publishing would become distributed coordination, and both pollers could publish between a lease check and a send.
- **The Position Consumer owning provider freshness.** Rejected: it can't tell an empty sky from an outage, and failover would still live in ingestion.
- **Inferring an outage from mass silence.** Rejected: it judges the provider by its aircraft and needs an arbitrary threshold.
- **Suppressing only while the provider is down.** Rejected: aircraft that haven't returned would alert at recovery, including returning aircraft whose fresh positions the consumer hasn't yet written.
- **A cap on unobserved time.** Rejected: an arbitrary ceiling either disqualifies aircraft permanently or forces alerts Sentinel can't justify.

---

## Consequences

- **The experiment's outage would raise no alerts.** Aircraft that aren't transferred to OpenSky pause until adsb.fi covers them again, then alert individually if still absent.
- **After long coordinator downtime,** aircraft owned by the returning provider alert once they have had one threshold of its coverage, possibly many at once. Aircraft owned by an inactive provider expire silently.
- **The coordinator is a single point of failure.** It fails safe: its downtime delays alerts rather than causing them. Nothing restarts it automatically.
- **Cross-references.** The poller is restructured within ADR-013's Node.js decision, and ADR-020's decision 3 is fulfilled. `ARCHITECTURE.md`, `DATA_MODEL.md` (the `{live-provider}` keys) and the signal-loss use case are updated with the implementation.
