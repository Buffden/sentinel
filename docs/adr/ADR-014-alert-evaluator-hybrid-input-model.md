# ADR-014: Hybrid Input Model for the Alert Evaluator

**Status:** Accepted
**Date:** 2026-08-09

---

## Context

The Alert Evaluator was originally designed to pull directly from all three persistence stores on each evaluation cycle:

- **Redis** — scan `entity:live:*` for signal loss (`last_seen_ms`)
- **TimescaleDB** — query `route_baseline` continuous aggregate for deviation detection
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

A new Node.js service, `services/deviation-detector/`, is introduced with a single responsibility: consume the normalised position stream, compare each position against the route baseline, and publish deviation candidates to Kafka.

| Direction | What |
|---|---|
| Consumes | `position.normalized` (consumer group: `deviation-detector`) |
| Reads from TimescaleDB | `route_baseline` — current time bucket baseline for the incoming entity |
| Publishes to | `deviation.candidates` |

**Contract:**
- Publishes a candidate event for every position that falls outside the baseline threshold — does not apply the sustained-ping filter. The Alert Evaluator owns the `DEVIATION_SUSTAINED_PINGS` counter and suppression logic.
- Does not write to Redis, Neo4j, or the `alerts` topic.
- Does not emit alerts. Anomaly rule evaluation is not its concern.

---

## Changes to the Correlation Worker

The Correlation Worker gains one additional output: when it detects an unscheduled proximity pair (no `KNOWN_ASSOCIATE` edge between the two entities), it publishes a candidate event to `proximity.candidates` in addition to writing the `PROXIMITY_EVENT` edge to Neo4j.

The Neo4j write is not removed — the graph remains the source of truth for relationship context and is queried by the API investigation panel. The Kafka publish is additive.

---

## Changes to the Alert Evaluator

| Detection | Before | After |
|---|---|---|
| Signal Loss | Scheduled Redis scan (`entity:live:*`) | Unchanged — scheduled Redis scan |
| Route Deviation | Direct TimescaleDB query (`route_baseline`) | Consumes `deviation.candidates` |
| Unscheduled Proximity | Direct Neo4j query (`PROXIMITY_EVENT` edges) | Consumes `proximity.candidates` |
| Composite Alert | Redis (`alert-state`) + Neo4j scan | Redis (`alert-state`) + Neo4j targeted lookup (unchanged) |

The Alert Evaluator drops its TimescaleDB dependency entirely. Its Neo4j access narrows from a broad edge scan to a targeted lookup used only when assembling composite alert context — checking known associates and fetching the proximity window for a specific entity pair.

---

## New Kafka Topics

| Topic | Producer | Consumer | Retention | Purpose |
|---|---|---|---|---|
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
- The Alert Evaluator's TimescaleDB dependency is removed entirely
- The Alert Evaluator's Neo4j access is narrowed to composite alert context only — it no longer scans for recent `PROXIMITY_EVENT` edges
- The following documents require updates to reflect this architecture:
  - `docs/ARCHITECTURE.md` — new service contract, updated topic table, updated consumer groups, updated persistence ownership, updated data flow diagram
  - `docs/DATA_MODEL.md` — new Kafka event schemas for `deviation.candidates` and `proximity.candidates`
  - `docs/implementation/phase-04-alert-pipeline.md` — Alert Evaluator no longer reads TimescaleDB; Deviation Detector is a new service introduced here or in a dedicated phase
  - `docs/implementation/phase-05-correlation.md` — Correlation Worker also publishes to `proximity.candidates`
  - `docs/use-cases/US-04-route-deviation/` — updated flow through Deviation Detector
  - `docs/use-cases/US-05-unscheduled-proximity/` — updated flow via `proximity.candidates`
  - `docs/use-cases/US-06-composite-alert/` — updated input sources for Alert Evaluator
  - `docs/architecture.puml` and exported SVGs — new service node and topic edges
  - `README.md` — architecture overview and data flow summary
  - `CLAUDE.md` — Kafka topic naming conventions section
