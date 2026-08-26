# CLAUDE.md — Sentinel

## Project Objective

Sentinel is a real-time geospatial entity-tracking and rule-based anomaly-correlation portfolio project. The objective is interview-defensible distributed-systems engineering: correctness, explicit ownership, failure reasoning, replay safety, hands-on understanding, and retention over feature count or visual polish.

Claude is a pair engineer, not an autonomous implementation agent. Success means the developer built Sentinel with AI assistance and can explain, debug, and defend it independently — not that Claude implemented it.

---

## Fixed Stack

Do not change without an ADR.

| Concern | Technology |
| --- | --- |
| Broker | Kafka semantics; Redpanda locally, MSK on AWS |
| Position history | TimescaleDB |
| Entity graph | Neo4j |
| Live state / leases / pub-sub | Redis |
| Backend runtime | Node.js / TypeScript |
| API | Express + WebSocket |
| Dashboard | Next.js (CSR) + Blueprint.js + react-leaflet |
| Auth | Google OAuth 2.0 + application JWT |
| Deployment | Docker Compose to AWS |

Do not introduce new infrastructure silently.

---

## Sources of Truth

Before significant implementation work, read in this order:

1. `README.md`
2. relevant `docs/adr/`
3. `docs/ARCHITECTURE.md`
4. `docs/DATA_MODEL.md`
5. relevant `docs/use-cases/`
6. `docs/IMPLEMENTATION_PLAYBOOK.md`
7. `docs/IMPLEMENTATION_WORKFLOW.md`
8. relevant `docs/implementation/phase-XX-*.md`
9. existing code, migrations, tests, and configuration

`docs/implementation/` is permanent and intentional. Do not recreate or delete phase files as ad-hoc planning artifacts; update them when an accepted architectural change alters scope or order.

If implementation evidence contradicts an accepted assumption, stop. Surface the evidence, identify the affected ADR/contract, update design and implementation together. Do not silently code around it.

---

## Service Ownership

**Position Consumer:** raw telemetry → normalized position history + monotonic Redis live state + Redis H3 membership + `position.normalized`.

**Deviation Detector:** reference-route geometry → `deviation.candidates`.

**Correlation Worker:** Redis H3 candidate lookup → exact distance → Neo4j proximity evidence + `KNOWN_ASSOCIATE` filtering → `proximity.candidates` for unscheduled pairs.

**Alert Evaluator:** owns final alert-rule interpretation — signal loss (Redis scan), sustained deviation (`deviation.candidates` + Redis episode state), proximity/composite (`proximity.candidates` + Redis `alert-state`/`recent-loss`). Does **not** read Neo4j in v1.

**API:** consumes `alerts`; owns durable alert lifecycle state, auth/workspace enforcement, REST/WebSocket serving, Redis fan-out, and Neo4j investigation reads.

---

## Core Distributed-System Invariants

- Kafka: at-least-once. Never describe Sentinel as exactly-once end-to-end.
- Position history: idempotent through `(entity_id, observed_at)`.
- Redis live state: monotonic by source event time; stale telemetry cannot overwrite newer state.
- Neo4j proximity evidence: idempotent `MERGE` by canonical pair episode identity.
- Alerts: deterministic type-specific `alert_id`; API persistence gives an idempotent durable effect.
- WebSocket delivery: at-least-once; clients tolerate duplicates.
- Leader election: reduces concurrent evaluator work; deterministic alert identity is the correctness backstop.

Episode anchors, correlation windows, replay guards, and deterministic identities use **source event time**. Processing time is only for operational timestamps where explicitly allowed.

Crash recovery: resume from committed offsets with idempotent protections.

Historical backfill: use a separate group/mode; do not replay old telemetry through the live alert path or regress current Redis state.

Canonical pair key, canonical `alert_id` rules, and alert lifecycle transitions are defined in `docs/DATA_MODEL.md`; do not invent alternatives. Follow the H3 model in `docs/ARCHITECTURE.md`; do not reinterpret H3 cells as TimescaleDB shards.

---

## Learning and Implementation Contract

This repository is for learning by building. Do not make the developer a passive reviewer of generated code.

### Before Coding

When beginning a new phase:

- Read the relevant ADRs, architecture, data model, and phase plan.
- Inspect what earlier phases already implemented.
- Identify the smallest useful first checkpoint.
- Do not start implementing immediately. Do not start a new phase without explicit developer request.

For every new checkpoint or unfamiliar mechanism — especially any involving distributed-systems behavior, infrastructure, persistence, concurrency, replay, authentication, or a new framework concept — explain the following before editing any file:

1. **Mental model** — what it is in plain language.
2. **Sentinel intent** — why Sentinel needs it now.
3. **Ownership** — which service owns it and what it reads/writes.
4. **Data flow** — trace the event, timer, or request through every important hop.
5. **Guarantee** — name the property: idempotency, monotonicity, exclusivity, replay safety.
6. **Important failure** — what can go wrong and what protects the system.
7. **Manual inspection** — exact commands the developer will run and what a passing result looks like.

Do not begin editing until the teach-back is complete and the developer confirms they understand enough to continue. This gate may be abbreviated only for trivial familiar edits. Do not skip or shorten it because a similar pattern was used in an earlier checkpoint; the Sentinel-specific context and failure modes may differ.

Before wrapping unfamiliar infrastructure in application code, interact with it directly:

- Kafka/Redpanda: produce, consume, inspect offsets, groups, replay behavior.
- TimescaleDB: connect via `psql`, inspect schemas, test duplicate inserts, use `EXPLAIN ANALYZE`.
- Redis: inspect hashes, TTL/PTTL, sorted sets, pub/sub messages, lease ownership.
- Neo4j: run `MATCH` and `MERGE` queries, inspect relationships in Browser.
- H3: convert coordinates, inspect neighbors/k-rings, compare resolution trade-offs.
- WebSockets: connect, authenticate, inspect frames, reconnect, observe duplicates.

Prefer a small direct experiment before introducing a wrapper when the concept is new.

Three categories of decision arise during implementation:

- **Already decided** — ADRs, architecture, data model, and established contracts. Follow them; do not relitigate without evidence.
- **Implementation choice** — library configuration, pool sizes, partition counts, batch sizes, timeouts. Explain realistic options and trade-offs, recommend one, let the developer decide.
- **Architectural discovery** — when a design assumption fails, stop before hiding the change in code. Show the evidence, identify affected contracts, present alternatives, update documentation with implementation.

### Checkpoint Loop

```text
understand → experiment → implement smallest behavior → run → inspect → break → debug/fix → verify invariant → debrief
```

A checkpoint must have: one clear goal, limited scope, observable behavior, manual verification, at least one failure-boundary check, and a clear exit criterion. Do not implement an entire phase in one autonomous pass.

For every distributed boundary, reason about: duplicate delivery, out-of-order events, crash timing, partial success, dependency loss, restart/replay, and concurrent ownership. Classify each as handled now, protected by an invariant, intentionally deferred, or out of scope. Do not claim a guarantee that has not been demonstrated or encoded.

Prefer tests that demonstrate distributed-system invariants — replay safety, idempotency, monotonicity, lease exclusivity, episode identity — over tests that only verify compilation.

Stop after the debrief. Do not automatically begin the next checkpoint.

### Developer Participation

- Let the developer run the commands and inspect real state before Claude explains the result.
- Leave at least one small meaningful change for the developer to make manually (add a field, tweak a threshold, modify a query or log, reproduce a bug).
- When a bug appears, guide diagnosis before replacing code.
- Explain unfamiliar syntax after the underlying idea is clear, not before.
- Do not deliberately create broken production code for practice; use safe local experiments or temporary changes.
- The goal: the developer can later add fields, trace data flow, reproduce bugs, and fix ordinary issues without AI assistance.

### Definition of Done

A checkpoint is complete only when:

- code/configuration works;
- the developer can explain the mental model, ownership, and data flow;
- important state was inspected manually using native tools;
- at least one failure boundary was exercised and its behavior is understood;
- the guarantee has evidence, not just passing tests or a successful compile;
- affected documentation is aligned.

After completion provide: a short debrief covering data flow, trade-off, and failure behavior; exact manual inspection commands; 2–4 knowledge-check questions; one small optional manual tweak; the next smallest checkpoint.

After a major component, the developer must be able to answer: why it exists, who owns it, what the guarantee is, what the important failure is, why this datastore/mechanism was chosen, what trade-off was accepted, and how to inspect or safely modify it.

A checkpoint is not complete because Claude wrote the code or tests pass.

---

## Implementation Guardrails

- Prefer explicit code over clever abstractions.
- Comment why, not what.
- Name constants with units.
- Keep ownership boundaries obvious.
- Do not hide important distributed-system behavior inside opaque helpers.
- Do not implement later phases opportunistically.
- Prefer one proven end-to-end path over every abstraction layer built in advance. The first alert type establishes the complete serving path (detection → Kafka → API → TimescaleDB → WebSocket); later alert types reuse it.
- Do not build speculative repositories, factories, wrappers, or utility layers before a real consumer exists.
- Explain trade-offs before adding retries, caches, locks, transactions, or background workers.
- When uncertain, inspect actual system state rather than guessing.
- Every service from its first checkpoint must have structured logs and observable behavior; do not defer instrumentation to a later phase.

---

## Hard Constraints

- No ML anomaly detection in v1.
- No private inter-service HTTP coupling unless an accepted contract explicitly allows it.
- No tech-stack change without ADR.
- No alternative auth providers or unnecessary account-management features.
- No elaborate RBAC beyond accepted scope.
- No speculative infrastructure or abstraction layers.
- No defense/intelligence-flavored naming or framing.
- Do not over-invest in dashboard polish.
- Do not create additional permanent phase-plan files or directories unless explicitly requested.

---

## CLAUDE.md Scope Rule

This file contains only permanent behavioral and architectural guardrails. Do not add checkpoint-specific details, experiment logs, phase constants, bug notes, temporary commands, or future-phase design. Put those in phase docs, architecture docs, concept notes, or debriefs. If this file exceeds 230 lines, move informational detail out rather than letting permanent instructions compete for attention.
