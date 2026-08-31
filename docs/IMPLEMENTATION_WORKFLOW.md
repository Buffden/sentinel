# Implementation Workflow

This document defines how Sentinel implementation work is executed. The permanent phase roadmap lives in `docs/implementation/`; those files are intentional, versioned planning artifacts and must be kept aligned with the architecture.

The companion `docs/IMPLEMENTATION_PLAYBOOK.md` defines the overall implementation philosophy. This file focuses on the per-phase working loop.

---

## Sources of Truth

Before implementing a checkpoint, use this order:

1. `README.md` — project scope and high-level architecture.
2. `docs/adr/` — accepted architectural decisions and rejected alternatives.
3. `docs/ARCHITECTURE.md` — service boundaries, ownership, data flow, and delivery semantics.
4. `docs/DATA_MODEL.md` — canonical database, Redis, and Kafka contracts.
5. `docs/use-cases/` — expected behavior and scenario-level flows.
6. `docs/implementation/phase-XX-*.md` — implementation order and phase boundaries.
7. Existing code, migrations, configuration, and tests — executable reality.

If implementation evidence contradicts an architectural document, stop before silently redesigning the system. Explain the evidence, update the relevant ADR/contract, then change code and documentation together.

---

## Permanent Phase Roadmap

Sentinel intentionally maintains the following implementation plans:

1. Phase 01 — Infrastructure + Canonical Schemas + Observability Skeleton
2. Phase 02 — Live Position Pipeline
3. Phase 03 — Signal Loss + Alert Delivery Foundation
4. Phase 04 — Route Deviation
5. Phase 05 — Correlation Worker + Unscheduled Proximity
6. Phase 06 — Composite Correlation
7. Phase 07 — Workspace + Operator Scope
8. Phase 08 — Alert Lifecycle + Distributed Fan-Out
9. Phase 09 — Entity Investigation
10. Phase 10 — Production Hardening + Failure Lab

Do not delete or recreate this directory as an ad-hoc planning mechanism. Update the relevant phase file when an accepted architectural change alters implementation order or phase scope.

---

## Starting a Phase

When beginning a phase:

1. Read the relevant ADRs, architecture contracts, data model, use cases, and phase plan.
2. Inspect the code already implemented by earlier phases.
3. Separate what is already decided from implementation choices that remain open.
4. Identify the system-design concepts and failure boundaries involved.
5. Produce the smallest useful first checkpoint.
6. Do not implement future-phase behavior unless it is a genuine prerequisite.

Implementation choices such as Kafka partition count, batch size, pool sizing, timeout defaults, and library configuration should be treated as tunable unless an ADR makes them architectural guarantees.

---

## Work in Small Observable Checkpoints

The authoritative 15-step implementation sequence is defined in `CLAUDE.md` under "Implementation Sequence" and "Permanent sequencing rules". Apply it to every checkpoint in every subsystem.

A checkpoint is not complete because code compiles. Every checkpoint must include observable evidence in real system state, at least one exercised failure boundary, passing quality gates, and updated documentation before the developer confirms completion.

---

## Vertical-Slice Rule

Prefer proving one end-to-end path over generating every layer in advance.

For example, Phase 03 establishes the first alert serving slice:

```text
Redis live state
→ Alert Evaluator
→ Kafka alerts
→ API consumer
→ TimescaleDB alerts
→ authenticated WebSocket
→ operator-visible alert
```

Later alert types reuse this serving path rather than redesigning it.

Phase boundaries matter: final-state use-case diagrams may show features delivered in later phases. The current phase file determines what must be implemented now.

---

## Delivery and Idempotency Mental Model

Sentinel uses at-least-once Kafka processing. Do not describe the whole pipeline as exactly-once.

The target guarantees are:

- Kafka processing: at-least-once.
- Durable database side effects: idempotent / exactly-once effect through deterministic identity and database constraints.
- Redis live state: monotonic by source event time; stale events cannot regress newer state.
- WebSocket delivery: at-least-once; clients deduplicate by alert identity plus lifecycle version/status semantics.
- Leader election: prevents concurrent active Alert Evaluators; it is not a substitute for durable idempotency.

---

## Failure-First Questions

For important distributed flows, explicitly ask:

- What if the event is delivered twice?
- What if an older event arrives after a newer one?
- What if persistence succeeds but the Kafka offset is not committed?
- What if Redis restarts?
- What if Neo4j succeeds but Kafka publication fails?
- What if the Alert Evaluator loses its lease mid-evaluation?
- What happens on service restart?
- What happens during intentional historical backfill?

Classify each failure as handled now, protected by an invariant, deliberately deferred, or out of scope.

---

## Hands-On Infrastructure Expectations

The developer should be able to inspect the system directly:

- Kafka/Redpanda: topics, partitions, consumer groups, offsets, lag, replay.
- TimescaleDB: schema, rows, indexes, hypertable chunks, `EXPLAIN ANALYZE`, duplicate writes.
- Redis: hashes, sorted sets, TTLs, lease ownership, pub/sub behavior.
- Neo4j: nodes, relationships, `MERGE`, known-associate filtering, proximity evidence.
- H3: cell conversion, neighbors/k-rings, boundary movement, resolution trade-offs.
- WebSockets: connection authentication, event delivery, reconnection, duplicate tolerance.

Do not hide unfamiliar infrastructure behind abstractions before the underlying behavior is understood.

---

## After Each Meaningful Checkpoint

Provide a short engineering debrief covering:

- what was built;
- the data flow;
- the main system-design concept;
- the accepted trade-off;
- the relevant failure behavior;
- commands or queries the developer can run independently;
- 2–4 knowledge-check questions;
- the smallest logical next checkpoint.

The success criterion is that the developer can explain what was built, why it works, what can fail, how to inspect it, and which trade-offs were accepted.
