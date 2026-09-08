# Phase 05 — Correlation Worker + Unscheduled Proximity

## Goal

Detect proximity efficiently, persist relationship evidence, and emit unscheduled-proximity alerts through the existing alert path.

```text
position.normalized → Correlation Worker → H3 candidates → exact distance → Neo4j PROXIMITY_EVENT → proximity.candidates → Alert Evaluator → UNSCHEDULED_PROXIMITY
```

## Suggested Checkpoints

1. H3 same/neighbor-cell candidate lookup.
2. Exact distance filtering.
3. Canonical pair ordering (`min:max`).
4. Neo4j `MERGE` for one proximity episode.
5. `proximity-episode:{pair_key}` state and TTL.
6. One `proximity.candidates` event per encounter.
7. `KNOWN_ASSOCIATE` pair is persisted as graph evidence but filtered by the **Correlation Worker** before `proximity.candidates`.
8. Alert Evaluator converts an unscheduled candidate into `UNSCHEDULED_PROXIMITY`.
9. Existing API path persists and delivers it.

## Checkpoint progress

| Checkpoint | Scope | Status |
| --- | --- | --- |
| CP1: H3 candidate lookup | `findProximityCandidates`: gridDisk(k) union of `geo-cell:*` sorted sets, freshness filter, self-exclusion, dedup. No distance calc, no service loop | Done |
| CP2: Exact distance filtering | Haversine/great-circle distance over CP1's candidates against `PROXIMITY_THRESHOLD_METRES` | Not started |
| CP3: Canonical pair ordering | `pair_key = min(a,b):max(a,b)` so A/B and B/A triggering resolve to one identity | Not started |
| CP4: Neo4j proximity evidence | `MERGE` one `PROXIMITY_EVENT` edge per episode, idempotent under replay | Not started |
| CP5: Proximity episode state | `proximity-episode:{pair_key}` hash, TTL-based encounter gap detection | Not started |
| CP6: Candidate publication | One `proximity.candidates` event per new encounter; `candidate_published` retry flag | Not started |
| CP7: KNOWN_ASSOCIATE filtering | Graph evidence retained for known pairs; candidate never published for them | Not started |
| CP8: Alert Evaluator integration | Consume `proximity.candidates`, emit `UNSCHEDULED_PROXIMITY` (or `COMPOSITE` per existing signal-loss correlation) | Not started |
| CP9: Exit verification | Full path proven end-to-end; all required failure experiments below | Not started |

Correlation Worker service scaffold (`services/correlation-worker/`) was created in CP1 to hold real, immediately-used code — not a speculative wrapper — since the candidate-lookup function is Phase 05's actual first deliverable, not a placeholder for a hypothetical future consumer.

## Required Failure Experiments

- A/B and B/A triggering produce one canonical episode
- Neo4j succeeds but Kafka publish fails; `candidate_published=0` enables retry
- duplicate Kafka delivery does not create a second episode
- known-associate pair produces graph evidence but no candidate and no alert

## Exit Criteria

One candidate/alert exists per continuous unscheduled encounter, one graph edge exists per episode, and known associates never enter the anomaly-candidate stream.

---

## Contents

| Path | Description |
| --- | --- |
| [`concepts/`](concepts/README.md) | Concept notes and checkpoint debriefs, in reading order |
