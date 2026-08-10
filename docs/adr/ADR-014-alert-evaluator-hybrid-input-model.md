# ADR-014: Hybrid Input Model for the Alert Evaluator

**Status:** Accepted
**Date:** 2026-08-09

---

## Context

The Alert Evaluator was originally designed to pull directly from all three persistence stores on each evaluation cycle:

- **Redis** — scan `entity:live:*` for signal loss (`last_seen_ms`)
- **TimescaleDB** — query `route_baseline` continuous aggregate for deviation detection (this aggregate was later found to be architecturally incorrect; see ADR-015)
- **Neo4j** — query `PROXIMITY_EVENT` and `KNOWN_ASSOCIATE` edges for proximity and composite detection

A review of the architecture raised a valid concern: coupling a single service to three different stores makes it harder to test in isolation, harder to reason about failure modes, and harder to evolve individual detection rules independently. The reviewer suggested introducing dedicated stream processors that publish derived facts onto Kafka topics, so that the Alert Evaluator becomes a pure Kafka consumer.

However, not all detection patterns map cleanly onto event streams. Signal loss detection is inherently a scheduled pull over an absence of events — there is no event to react to when an entity stops broadcasting. Refactoring it into a stream processor would require artificial heartbeat events or a timer service, adding complexity without adding correctness.

This ADR resolves the tension by adopting a hybrid model: detection patterns that naturally fit event streams are moved to Kafka topics with dedicated producers; detection patterns that require scheduled scanning of store state remain as direct reads.

---

## Decision

Adopt a **hybrid input model** for the Alert Evaluator:

- **Route deviation** and **unscheduled proximity** detection inputs move to dedicated Kafka topics (`deviation.candidates`, `proximity.candidates`), produced by a new Deviation Detector service and the existing Correlation Worker respectively.
- **Signal loss** detection remains a scheduled pull from Redis — the Alert Evaluator continues to scan `entity:live:*` directly.
- **Composite alert** assembly continues to read `alert-state:{entity_id}` from Redis for the loss window start time, and queries Neo4j directly for relationship context only — a targeted lookup, not a scan.

---

## New Service: Deviation Detector

A new Node.js service, `services/deviation-detector/`, is introduced with a single responsibility: consume the normalised position stream, compare each position against the route baseline, and publish deviation status events to Kafka.

| Direction | What |
|---|---|
| Consumes | `position.normalized` (consumer group: `deviation-detector`) |
| Reads from TimescaleDB | `route_reference_points` (via `route_references`) — reference route segments for the incoming entity (synthetic entities only); see ADR-015 |
| Publishes to | `deviation.candidates` |

The `deviation.candidates` event schema is defined in `DATA_MODEL.md` (Kafka Event Schemas section).

**Contract:**

- **Stateless:** emits `OUT_OF_RANGE` or `IN_RANGE` on every eligible ping. Does not track state between pings. `BACK_IN_RANGE` is replaced by `IN_RANGE` (every in-range ping, not just transitions). This eliminates the restart problem: a stateless detector produces the same output regardless of prior history.
- Does not apply the `DEVIATION_SUSTAINED_PINGS` filter. The Alert Evaluator owns all episode state via `deviation-state:{entity_id}` (hash: `count`, `episode_start_ms`, `last_processed_ms`, `alert_emitted`).
- Reads from `route_reference_points` (via `route_references`) — the correct v1 reference route schema. See ADR-015. Does not read `route_baseline` (which does not exist).
- Skips entities with no row in `route_references` — real ADS-B/AIS entities are not covered in v1.
- Does not write to Redis, Neo4j, or the `alerts` topic.
- Does not emit alerts. Anomaly rule evaluation is not its concern.

---

## Changes to the Correlation Worker

The Correlation Worker gains one additional output: when it detects an unscheduled proximity pair (no `KNOWN_ASSOCIATE` edge between the two entities), it publishes a candidate event to `proximity.candidates` in addition to writing the `PROXIMITY_EVENT` edge to Neo4j.

The Neo4j write is not removed — the graph remains the source of truth for relationship context and is queried by the API investigation panel. The Kafka publish is additive.

---

## Changes to the Alert Evaluator

| Detection | Before | After |
| --- | --- | --- |
| Signal Loss | Scheduled Redis scan (`entity:live:*`) | Unchanged — scheduled Redis scan |
| Route Deviation | Direct TimescaleDB query (`route_baseline`, not used) | Consumes `deviation.candidates` from stateless Deviation Detector |
| Unscheduled Proximity | Direct Neo4j query (`PROXIMITY_EVENT` edges) | Consumes `proximity.candidates` (one event per proximity episode) |
| Composite Alert | Redis scan + Neo4j | Supersession model: SIGNAL_LOSS emitted immediately; proximity arrival checks `alert-state` (active dark) or `recent-loss` (was dark); if match, emit COMPOSITE + supersede SIGNAL_LOSS |

The Alert Evaluator's `route_baseline` read is removed — that comparison now belongs to the Deviation Detector (which reads from `route_reference_points`, not a continuous aggregate). Its remaining TimescaleDB dependency is narrowed to `position_history`, read only when building the signal loss alert payload. Its Neo4j access narrows to composite alert context — a targeted lookup on a specific entity pair, not a scan.

---

## New Kafka Topics

| Topic | Producer | Consumer | Retention | Purpose |
| --- | --- | --- | --- | --- |
| `deviation.candidates` | Deviation Detector | Alert Evaluator | Short (1h) | Per-entity positions outside the route baseline, pre-filtered against baseline threshold |
| `proximity.candidates` | Correlation Worker | Alert Evaluator | Short (1h) | Unscheduled entity proximity pairs, pre-filtered to exclude known associates |

Short retention is intentional — these are derived, transient signals. If the Alert Evaluator is down briefly and misses a window of candidates, the underlying facts (position history, Neo4j edges) remain durable in their respective stores. There is no value in replaying stale proximity or deviation candidates.

---

## Why Signal Loss Stays as a Direct Redis Read

Signal loss is the detection of an absence of events. An aircraft that stops broadcasting does not produce a Kafka message saying "I have stopped broadcasting." The only way to detect it is to look at when the entity was last seen and compare that against the current time.

A stream processor approach would require either:
1. Artificial heartbeat events published by the position consumer on each successful write — adding a side-effect to the position consumer that has nothing to do with normalisation, and still requiring a scheduled scan to detect when heartbeats stop arriving.
2. A timer/scheduler service that fires synthetic events per entity — adding a new infrastructure component for no correctness gain.

The scheduled Redis scan is the correct model for this problem. It is simple, direct, and honest about what it is doing.

---

## Why Composite Alert Still Reads Neo4j Directly

The composite alert (US-06) requires relationship context that is not carried in `proximity.candidates`: specifically, whether the proximity entity is a known associate, and the full proximity window start time for the alert payload. The `proximity.candidates` event carries enough to trigger detection but not enough to assemble the alert payload.

Rather than bloating the `proximity.candidates` schema with relationship context that only the composite rule needs, the Alert Evaluator performs a single targeted Neo4j lookup at alert emission time. This is not a scan — it is a point query on a specific entity pair. The coupling is narrow and justified.

---

## Alternatives Considered

### Full stream processor model (rejected)

Move all detection inputs to Kafka, including signal loss. Requires artificial heartbeat events and a timer service to detect entity silence. Adds two new components and a new failure mode (missed heartbeats) to solve a problem that a scheduled Redis scan already handles correctly. Complexity increase is not justified.

### Keep all direct store reads (original design — rejected)

Valid for a first pass but couples the Alert Evaluator to three stores, making each detection rule harder to test in isolation and harder to evolve. The hybrid model resolves the legitimate parts of the coupling concern without over-engineering the signal loss case.

---

## Consequences

- A new service `services/deviation-detector/` must be scaffolded and added to `docker-compose.yml`
- Two new Kafka topics must be created in the infrastructure init script: `deviation.candidates`, `proximity.candidates`
- The Correlation Worker gains a Kafka produce call alongside its existing Neo4j write — both happen on the same proximity detection event
- The Alert Evaluator's `route_baseline` read is removed; its TimescaleDB access is narrowed to `position_history` only
- The Deviation Detector reads from `route_reference_points` (via `route_references`) — the v1 reference route schema defined in ADR-015
- The composite alert uses a supersession model — SIGNAL_LOSS is never held back; `recent-loss:{entity_id}` enables composite correlation after entity resumes
- Proximity is modeled as an episode (`proximity-episode:{pair_key}`), not a per-ping event; one `proximity.candidates` event per episode
