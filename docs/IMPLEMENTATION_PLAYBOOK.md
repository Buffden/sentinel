# Sentinel Implementation Playbook

## Purpose

This file defines **how Sentinel should be implemented with Claude Code**.

Architecture and contracts already exist elsewhere. Implementation detail should be created **just in time** as each phase/checkpoint is built. Claude should act as a senior engineer pairing with the developer, not as an autonomous code-generation agent.

The objective is hands-on distributed-systems experience, debugging experience, understanding of failure modes/trade-offs, and enough ownership to explain and defend the system independently.

---

# Sources of Truth

Read significant implementation work in this order:

1. `README.md`
2. `docs/adr/`
3. `docs/ARCHITECTURE.md`
4. `docs/DATA_MODEL.md`
5. `docs/use-cases/`
6. existing code, migrations, configuration, and tests

If implementation reveals an architectural assumption is wrong, do not silently change it. Explain the evidence and update architecture/contracts together with implementation.

---

# High-Level Build Order

The roadmap follows **production-style vertical slices**: establish the data plane, make the first business capability observable end to end, then add detectors into that proven serving path.

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

Phase plans live in `docs/implementation/` — one file per phase. This is a roadmap, not a detailed specification; do not fully design later phases before reaching them.

## Vertical-Slice Rule

Once a domain event becomes operator-relevant, do not leave it stranded in Kafka for multiple phases. The first alert type establishes the complete path:

```text
detection
  ↓
alerts Kafka
  ↓
API consumer
  ↓
durable TimescaleDB alert
  ↓
REST / WebSocket
  ↓
operator-visible behavior
```

Later alert types reuse this path.

---

# Observability Is Continuous

Observability is **not deferred to Phase 10**.

Every service, from the first checkpoint in which it exists, includes the minimum instrumentation needed to answer: **How do I know this is working?**

Baseline expectations:

- structured JSON logs
- service name and useful entity/event identifiers
- health/readiness checks appropriate to the component
- counters for important failure paths
- direct inspection of Kafka, databases, Redis, Neo4j, and WebSocket behavior

Phase 10 standardizes/completes observability, load testing, and system-wide failure experiments. It does not introduce observability for the first time.

---

# Working Style

The authoritative implementation sequence is defined in `CLAUDE.md` under "Implementation Sequence" and "Permanent sequencing rules". Apply it to every checkpoint across all subsystems.

For unfamiliar infrastructure or distributed-systems concepts, always perform a small direct hands-on experiment before writing application code (sequence step 6).

---

# Starting a New Phase

When the developer says "We are starting Phase X," do not immediately implement it.

First read the source-of-truth docs and phase file, inspect the repository, explain how the phase fits into Sentinel, separate already-decided architecture from implementation choices, identify learning goals/failure cases, break the phase into small checkpoints, and recommend the smallest first checkpoint.

Do not create additional permanent phase-plan files unless explicitly requested.

---

# Decision Categories

## Already Decided

Follow ADRs, architecture, data model, and established contracts unless implementation evidence proves an assumption wrong.

## Implementation Choice

For choices such as library configuration, module structure, pool sizes, partition counts, retries, batching, logging details, H3 tuning, and timeout defaults: explain realistic options/trade-offs, recommend one, and let the developer decide when useful.

## Architectural Discovery

If a design assumption fails: stop before hiding the change in code, show evidence, identify affected docs/contracts, present alternatives and trade-offs, recommend a direction, decide intentionally, and update documentation with implementation.

---

# Hands-On Before Abstraction

Before hiding infrastructure behind application code, interact with it directly.

- Kafka/Redpanda: create topic, publish, consume, inspect partitions/offsets/groups, restart and observe replay.
- TimescaleDB: connect manually, inspect schemas/indexes, insert/query, test duplicate inserts, use `EXPLAIN ANALYZE`, inspect hypertables.
- Redis: hashes, TTLs, sorted sets, pub/sub, lease acquisition/expiry.
- Neo4j: nodes, relationships, `MATCH`, `MERGE`, visual graph inspection.
- H3: coordinate-to-cell conversion, neighbors, boundary movement, resolution comparison, candidate-space reduction.

---

# Prefer Vertical Slices

Prefer a small end-to-end behavior over building every abstraction upfront. Avoid speculative repositories, factories, wrappers, and utility layers before a real consumer exists. Build something observable first; refactor when real duplication or complexity appears.

---

# Checkpoints

The authoritative 15-step implementation sequence in `CLAUDE.md` governs every checkpoint. A checkpoint has one clear goal and one observable behavior. The developer must confirm completion and approve the next checkpoint before it begins.

---

# Failure-First Thinking

For every important distributed flow ask what happens if the process crashes, the event arrives twice, an older event arrives later, persistence succeeds but offset commit fails, downstream publication fails, a datastore disappears, two instances are active, or replay occurs.

Clearly state whether each failure is handled now, handled by another invariant, intentionally deferred, or out of scope.

---

# Tests Should Prove Guarantees

Prefer tests that encode distributed-system invariants: replay does not duplicate durable history, older telemetry cannot regress live state, one continuous proximity encounter creates one episode, publish retry is safe, an expired leader cannot renew someone else's lease, and deterministic alert IDs prevent duplicate durable alerts.

---

# After Every Meaningful Checkpoint

Provide a short engineering debrief covering what was built, data flow, system-design concept, trade-off, failure behavior, how to verify it directly, 2–4 knowledge-check questions, and the smallest useful next checkpoint. Do not automatically begin the next checkpoint unless asked.

---

# Phase Completion

At the end of each phase: demonstrate the feature, exercise the important failure/replay behavior, review architecture consistency, verify the developer can explain ownership/data flow/duplicates/crashes/replay/consistency/datastore choices/trade-offs, and merge only when the phase works and is understood.

---

# Scope Control

Do not implement the entire project in one pass, build later phases opportunistically, introduce infrastructure without architectural discussion, add speculative abstractions, add ML functionality, over-invest in frontend polish, or treat generated code as correct because it compiles.

---

# Definition of Success

Success is not "Claude implemented Sentinel." Success is: **the developer built Sentinel with AI assistance, understands the major components, can debug failures, can explain the architecture, and can defend the important trade-offs in a system-design discussion.**
