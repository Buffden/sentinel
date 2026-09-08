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

## Progress

| Scope | Status |
| --- | --- |
| H3 candidate lookup — `findProximityCandidates`: gridDisk(k) union of `geo-cell:*` sorted sets, freshness filter, self-exclusion, dedup. No distance calc, no service loop | Done |
| Exact distance filtering — `filterByDistance`: real lat/lon from `entity:live:*`, great-circle distance, threshold cutoff, nearest-first ordering | Done |
| Canonical pair ordering — `pair_key = min(a,b):max(a,b)` so A/B and B/A triggering resolve to one identity | Not started |
| Neo4j proximity evidence — `MERGE` one `PROXIMITY_EVENT` edge per episode, idempotent under replay | Not started |
| Proximity episode state — `proximity-episode:{pair_key}` hash, TTL-based encounter gap detection | Not started |
| Candidate publication — one `proximity.candidates` event per new encounter; `candidate_published` retry flag | Not started |
| `KNOWN_ASSOCIATE` filtering — graph evidence retained for known pairs; candidate never published for them | Not started |
| Alert Evaluator integration — consume `proximity.candidates`, emit `UNSCHEDULED_PROXIMITY` (or `COMPOSITE` per existing signal-loss correlation) | Not started |
| Exit verification — full path proven end-to-end; all required failure experiments below | Not started |

The Correlation Worker service scaffold (`services/correlation-worker/`) holds real, immediately-used code, not a speculative wrapper — the candidate-lookup function is this phase's actual first deliverable.

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
