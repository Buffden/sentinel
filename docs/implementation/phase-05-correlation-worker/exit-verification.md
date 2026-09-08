# Phase 05 Exit Verification

Final inspection of the Correlation Worker and unscheduled-proximity path before Phase 06 begins. Records the state of every service, Kafka topic, Neo4j node/edge, and Redis key introduced or written by Phase 05.

Verified: 2026-09-08, against the local dev stack (`make up`), by running every automated integration suite touched by this phase plus direct manual inspection of Redis/Neo4j/Kafka/TimescaleDB and one real end-to-end run of each service. Evidence for each row lives in the corresponding concept debrief under `concepts/`; this document is the consolidated pass, not a restatement from memory.

---

## 1. Container health

| Container | Image | Status |
| --- | --- | --- |
| sentinel-redpanda | docker.redpanda.com/redpandadata/redpanda:v24.1.2 | healthy |
| sentinel-timescaledb | timescale/timescaledb:2.15.3-pg16 | healthy |
| sentinel-redis | redis:7.2.4-alpine | healthy |
| sentinel-neo4j | neo4j:5.19.0-community | healthy |

---

## 2. Services

| Service | Introduced in | Status |
| --- | --- | --- |
| correlation-worker | H3 candidates + distance filter + episode/evidence/publish gate + Kafka consumer/producer | Real service started (`tsx src/worker.ts`); consumer group `correlation-worker` joined, subscribed to `position.normalized`; 39/39 automated tests pass against real Redis/Neo4j/Kafka |
| alert-evaluator | Proximity candidate consumer (alongside existing leader-gated scan) | Real service started (`tsx src/evaluator.ts`); both `proximity candidates consumer running` and `acquired leader lease` logged at startup; 18/18 automated tests pass |
| api | `counterparty_entity_id` persistence + exposure (gap found and closed this pass) | 33/33 automated tests pass against real TimescaleDB/Redis |

---

## 3. Kafka topics written by Phase 05

| Topic | Producer | Records observed |
| --- | --- | --- |
| `proximity.candidates` | Correlation Worker | Real message consumed back via `rpk topic consume`: `{"pair_key":"demo-plane-a:demo-plane-b","entity_a_id":"demo-plane-a","entity_b_id":"demo-plane-b","episode_start_ms":...,"lat":37.00015...,"lon":-121,"distance_at_detection":33.35851559290627}` |
| `alerts` | Alert Evaluator (now also `UNSCHEDULED_PROXIMITY`, alongside pre-existing `SIGNAL_LOSS`) | Real message consumed: `{"alert_id":"demo-proximity-a:demo-proximity-b:UNSCHEDULED_PROXIMITY:...","entity_id":"demo-proximity-a","counterparty_entity_id":"demo-proximity-b","entity_type":"aircraft","alert_type":"UNSCHEDULED_PROXIMITY",...}` |

---

## 4. Neo4j

| Check | Result |
| --- | --- |
| `Entity` nodes exist for both sides of a real encounter | PASS — `demo-plane-a`/`demo-plane-b` and every `proximityEvent.integration.test.ts` case |
| `PROXIMITY_EVENT` edge exists with `idempotency_key = {pair_key}:{episode_start_ms}` | PASS — confirmed via `cypher-shell`: `"demo-plane-a", "demo-plane-b", "demo-plane-a:demo-plane-b:...", 33.35851559290627` |
| Redelivering the same encounter does not duplicate the edge | PASS — same-direction MERGE run twice produces 1 edge (`proximityEvent.integration.test.ts`); reversed-direction MERGE with the same key throws a uniqueness-constraint violation rather than silently duplicating (proven directly in `cypher-shell` before any code was written) |
| A known-associate pair has graph evidence but never enters `proximity.candidates` | PASS — `candidate-publication-gate` and `worker.integration.test.ts` known-associate cases: edge count 1, no candidate published |

---

## 5. Redis state

| Key pattern | Check | Result |
| --- | --- | --- |
| `geo-cell:{cell_id}` | Correlation Worker reads real Position Consumer output, no writes | PASS — `findProximityCandidates` is read-only against this key; confirmed by inspecting a real key (`geo-cell:8729a9752ffffff`) populated by live ADS-B traffic, untouched by any correlation-worker test run |
| `proximity-episode:{pair_key}` | Created on first confirmation; `episode_start_ms` fixed for the episode's life | PASS — `episode.integration.test.ts`; a later confirmation with a newer timestamp leaves `episode_start_ms` unchanged |
| `proximity-episode:{pair_key}` | TTL renews on each confirming ping; expires after a real gap with no confirmation | PASS — real 50ms TTL + 150ms wait produced a genuinely fresh `episode_start_ms` on the next confirmation, not a simulated one |
| `proximity-episode:{pair_key}` | `candidate_published` present (`0`/`1`) for unscheduled pairs, absent for known associates | PASS — `candidate-publication-gate` tests; confirmed live via `HGETALL proximity-episode:demo-plane-a:demo-plane-b` showing `candidate_published 1` after a real publish |

---

## 6. Correlation Worker: candidate lookup and distance filtering

| Check | Result |
| --- | --- |
| A same-cell pair is found at k=0 | PASS |
| A boundary-crossing pair (different cells, physically close) is found once k reaches the right ring | PASS — real h3-js exploration found a pair 19.98m apart in two different adjacent cells; `k=0` misses it, `k=1` finds it |
| A stale `geo-cell` member (older than the freshness bound) is excluded | PASS |
| A candidate beyond `PROXIMITY_THRESHOLD_METRES` is excluded after exact distance is computed | PASS |

---

## 7. Correlation Worker: episode, evidence, and publish gate

| Check | Result |
| --- | --- |
| A/B and B/A triggering the same encounter produce one canonical `pair_key` | PASS — `canonicalPairKey` unit tests; `proximityEvent.integration.test.ts`'s swapped-argument case produces exactly 1 edge, not a crash |
| One continuous encounter produces exactly one `proximity.candidates` message | PASS — `worker.integration.test.ts`: second ping in the same episode produces no second message |
| A known-associate pair produces graph evidence but no candidate | PASS |
| Redelivering the identical `position.normalized` message does not create a second episode or a second publish | PASS — explicit redelivery test (identical `timestamp_ms`, not a later one): 1 edge, 1 publish, second `handlePosition` call is a safe no-op |

---

## 8. Alert Evaluator: proximity consumer

| Check | Result |
| --- | --- |
| A real `proximity.candidates` message produces a real `UNSCHEDULED_PROXIMITY` alert on `alerts` | PASS — real end-to-end run, see section 3 |
| Alert `entity_type` is sourced from `entity:live:*`, defaults to `''` if absent | PASS — real run showed `"aircraft"` sourced from a seeded key; automated test confirms `''` when absent |
| Proximity consumer runs without acquiring the leader lease | PASS — real startup log shows `proximity candidates consumer running` logged before `acquired leader lease`, and the consumer processes messages regardless of which instance holds the lease |
| SIGNAL_LOSS and UNSCHEDULED_PROXIMITY alerts both flow through the same `alerts` topic with no special-case code in the API | PASS — `grep` across `services/api/src` for `alert_type`/`SIGNAL_LOSS`/`UNSCHEDULED_PROXIMITY` returns nothing; the sink treats every alert generically |

---

## 9. API persistence

A real gap was found and closed during this pass, not merely checked: the `alerts` table has carried `counterparty_entity_id` (with its own index) since Phase 01, but `alertSink.ts`'s `INSERT` never included it and `GET /alerts` never selected it — built for `SIGNAL_LOSS`, which has no counterparty, and never extended for proximity alerts. Fixed in both files; see `git log` on `services/api/src/sink/alertSink.ts` and `services/api/src/routes/alerts.ts`.

| Check | Result |
| --- | --- |
| `persistAlert` writes `counterparty_entity_id` for a proximity-style alert | PASS |
| `persistAlert` leaves `counterparty_entity_id` null for an alert type with none | PASS |
| `GET /alerts` includes `counterparty_entity_id` in its response | PASS |
| Idempotent replay (`ON CONFLICT (alert_id) DO NOTHING`) is unaffected by the added column | PASS — pre-existing replay tests still pass unchanged |

---

## 10. Failure experiments

| Experiment | Expected result | Observed result |
| --- | --- | --- |
| A/B and B/A triggering | One canonical episode, one graph edge, one candidate | PASS |
| Neo4j succeeds, Kafka publish never confirmed | `candidate_published` stays `0`; next qualifying ping retries the publish | PASS |
| Redeliver the same `position.normalized` message | No second episode, no second candidate, no second graph edge | PASS |
| Known-associate pair | Graph evidence recorded; no candidate; no alert | PASS |

---

## 11. Exit criteria

| Criterion | Result |
| --- | --- |
| One `proximity.candidates` event exists per continuous unscheduled encounter | PASS |
| One `PROXIMITY_EVENT` graph edge exists per episode | PASS |
| Known associates never enter the anomaly-candidate stream | PASS |
| SIGNAL_LOSS and UNSCHEDULED_PROXIMITY alerts coexist through the same canonical alert pipeline | PASS |
| No special-case route/proximity delivery code exists in the API | PASS |

**Phase 05 exit: COMPLETE**, pending the developer's own read-through of this document and the concept debriefs it summarizes. Composite correlation (proximity + recent signal-loss → `COMPOSITE`) is explicitly out of scope — see Phase 06.

---

## Cleanup note

Two things worth knowing before repeating this verification pass:

1. **Backlog replay.** Starting the real `api` service against this dev stack caused its `alert-sink` consumer group to replay a backlog of test/demo alert messages accumulated on the `alerts` topic across this phase's work, re-inserting rows earlier tests had already cleaned up. Its consumer group's committed offset determines how much backlog it replays, independent of `FROM_BEGINNING`.
2. **Background job control.** Three manually-started demo processes (`correlation-worker`, `alert-evaluator`, `api`) were each launched with `&` in separate tool calls; each `kill %1` only ever terminated the most recently backgrounded job in that shell, silently leaving the earlier ones running. All three kept consuming and cross-publishing real test messages to each other in the background — including regenerating already-cleaned-up rows — until this was caught via `ps aux` and every PID was killed explicitly. Confirmed clean afterward by PID (no `tsx` processes remained) and by store (0 test/demo keys in Redis, 0 test/demo nodes in Neo4j, 0 test/demo rows in `alerts`, all 1,656 remaining `alerts` rows are real ICAO24 entity IDs). When backgrounding more than one long-running process across tool calls, kill by PID, not by job number.
