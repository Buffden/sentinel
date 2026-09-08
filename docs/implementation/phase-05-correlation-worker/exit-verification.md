# Phase 05 Exit Verification

Final inspection of the Correlation Worker and unscheduled-proximity path before Phase 06 begins. Records the state of every service, Kafka topic, Neo4j node/edge, and Redis key introduced or written by Phase 05. Fill in each `Result` cell by actually running the check, not from memory of earlier debriefs.

Verified: —

---

## 1. Container health

| Container | Image | Status |
| --- | --- | --- |
| sentinel-redpanda | | |
| sentinel-timescaledb | | |
| sentinel-redis | | |
| sentinel-neo4j | | |

---

## 2. Services

| Service | Introduced in | Status |
| --- | --- | --- |
| correlation-worker | H3 candidates + distance filter + episode/evidence/publish gate + Kafka consumer/producer | |
| alert-evaluator | Proximity candidate consumer (alongside existing leader-gated scan) | |

---

## 3. Kafka topics written by Phase 05

| Topic | Producer | Records observed |
| --- | --- | --- |
| `proximity.candidates` | Correlation Worker | |
| `alerts` | Alert Evaluator (now also `UNSCHEDULED_PROXIMITY`, alongside pre-existing `SIGNAL_LOSS`) | |

---

## 4. Neo4j

| Check | Result |
| --- | --- |
| `Entity` nodes exist for both sides of a real encounter | |
| `PROXIMITY_EVENT` edge exists with `idempotency_key = {pair_key}:{episode_start_ms}` | |
| Redelivering the same encounter does not duplicate the edge | |
| A known-associate pair has graph evidence but never enters `proximity.candidates` | |

---

## 5. Redis state

| Key pattern | Check | Result |
| --- | --- | --- |
| `geo-cell:{cell_id}` | Correlation Worker reads real Position Consumer output, no writes | |
| `proximity-episode:{pair_key}` | Created on first confirmation; `episode_start_ms` fixed for the episode's life | |
| `proximity-episode:{pair_key}` | TTL renews on each confirming ping; expires after a real gap with no confirmation | |
| `proximity-episode:{pair_key}` | `candidate_published` present (`0`/`1`) for unscheduled pairs, absent for known associates | |

---

## 6. Correlation Worker: candidate lookup and distance filtering

| Check | Result |
| --- | --- |
| A same-cell pair is found at k=0 | |
| A boundary-crossing pair (different cells, physically close) is found once k reaches the right ring | |
| A stale `geo-cell` member (older than the freshness bound) is excluded | |
| A candidate beyond `PROXIMITY_THRESHOLD_METRES` is excluded after exact distance is computed | |

---

## 7. Correlation Worker: episode, evidence, and publish gate

| Check | Result |
| --- | --- |
| A/B and B/A triggering the same encounter produce one canonical `pair_key` | |
| One continuous encounter produces exactly one `proximity.candidates` message | |
| A known-associate pair produces graph evidence but no candidate | |
| Redelivering the identical `position.normalized` message does not create a second episode or a second publish | |

---

## 8. Alert Evaluator: proximity consumer

| Check | Result |
| --- | --- |
| A real `proximity.candidates` message produces a real `UNSCHEDULED_PROXIMITY` alert on `alerts` | |
| Alert `entity_type` is sourced from `entity:live:*`, defaults to `''` if absent | |
| Proximity consumer runs without acquiring the leader lease | |
| SIGNAL_LOSS and UNSCHEDULED_PROXIMITY alerts both flow through the same `alerts` topic with no special-case code in the API | |

---

## 9. Failure experiments

| Experiment | Expected result | Observed result |
| --- | --- | --- |
| A/B and B/A triggering | One canonical episode, one graph edge, one candidate | |
| Neo4j succeeds, Kafka publish never confirmed | `candidate_published` stays `0`; next qualifying ping retries the publish | |
| Redeliver the same `position.normalized` message | No second episode, no second candidate, no second graph edge | |
| Known-associate pair | Graph evidence recorded; no candidate; no alert | |

---

## 10. Exit criteria

| Criterion | Result |
| --- | --- |
| One `proximity.candidates` event exists per continuous unscheduled encounter | |
| One `PROXIMITY_EVENT` graph edge exists per episode | |
| Known associates never enter the anomaly-candidate stream | |
| SIGNAL_LOSS and UNSCHEDULED_PROXIMITY alerts coexist through the same canonical alert pipeline | |
| No special-case route/proximity delivery code exists in the API | |

**Phase 05 exit: INCOMPLETE**
