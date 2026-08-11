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

## Required Failure Experiments

- A/B and B/A triggering produce one canonical episode
- Neo4j succeeds but Kafka publish fails; `candidate_published=0` enables retry
- duplicate Kafka delivery does not create a second episode
- known-associate pair produces graph evidence but no candidate and no alert

## Exit Criteria

One candidate/alert exists per continuous unscheduled encounter, one graph edge exists per episode, and known associates never enter the anomaly-candidate stream.
