# IMPLEMENTATION_WORKFLOW.md

# Purpose

Sentinel maintains a permanent `docs/implementation/` directory containing the approved high-level phase sequence. These phase files define **what each phase must prove**, not every implementation detail in advance.

Detailed checkpoint plans are still created just in time from the current architecture, contracts, and code that already exists.

The goal is to:

- preserve a stable, reviewable implementation roadmap;
- avoid speculative low-level planning;
- implement Sentinel in small, observable increments;
- make implementation a hands-on system-design learning process;
- ensure the developer can explain and defend what is being built.

Claude should behave as a senior engineer pairing with the developer, not as an autonomous code generator.

---

# Sources of Truth

Use this precedence before implementing anything significant:

1. `docs/adr/` — accepted architectural decisions and rejected alternatives
2. `docs/ARCHITECTURE.md` — service boundaries, ownership, data flow, and system behavior
3. `docs/DATA_MODEL.md` — canonical schemas, Redis keys, Kafka contracts, and database contracts
4. `docs/use-cases/` — expected behavior and scenarios
5. `docs/implementation/phase-XX-*.md` — approved phase scope, experiments, and exit criteria
6. Existing code, migrations, configuration, and tests — current executable reality

`README.md` and `CLAUDE.md` provide project orientation and working conventions but do not override ADRs or canonical contracts.

---

# Starting a Phase

When beginning a phase:

1. Read the relevant phase file in `docs/implementation/`.
2. Read the ADRs, architecture sections, data contracts, and use cases that govern it.
3. Inspect everything already implemented that the phase depends on.
4. Identify what is already decided versus what remains an implementation choice.
5. Identify the system-design concepts involved.
6. Produce a concise checkpoint plan for the current phase.
7. Start with the smallest useful observable checkpoint.

Do not redesign previously accepted architecture silently. If implementation evidence invalidates an assumption, stop, explain the evidence, and update the affected ADR/contracts before changing behavior.

---

# Work in Small Checkpoints

Do not implement an entire phase in one large generation pass.

Prefer:

concept
→ small experiment
→ minimal implementation
→ run it
→ inspect it
→ test failure behavior
→ understand it
→ next checkpoint

A checkpoint should produce a useful observable result.

For unfamiliar infrastructure, interact with the infrastructure directly before hiding it behind application abstractions.

---

# Prefer Vertical Slices

Prefer one small end-to-end path over generating every layer in advance.

For example:

OpenSky sample event
→ Kafka
→ Position Consumer
→ TimescaleDB

and later:

signal loss
→ Alert Evaluator
→ `alerts`
→ API persistence
→ WebSocket delivery

Once an operator-visible alert path exists, later detectors should reuse it rather than create parallel serving paths.

Avoid speculative factories, generic repositories, wrappers around every dependency, and abstractions for future phases.

---

# Teach Before Automating

When introducing an unfamiliar concept, establish the mental model before library syntax. Examples include Kafka topics/partitions/groups/offsets, at-least-once delivery, replay, idempotency, event ordering, TimescaleDB hypertables, H3, Redis TTLs and sorted sets, leases, Neo4j relationships, and WebSockets.

Use Sentinel-specific examples and explain the relevant failure boundary.

---

# Hands-On First

The developer should be able to inspect and manipulate each major infrastructure component directly:

- **Kafka / Redpanda:** topics, partitions, offsets, consumer groups, replay, lag
- **TimescaleDB:** schema, inserts, indexes, `EXPLAIN ANALYZE`, hypertable chunks, duplicate writes
- **Redis:** hashes, TTLs, sorted sets, leader lease state, pub/sub
- **Neo4j:** nodes, relationships, `MERGE`, graph queries
- **H3:** coordinate-to-cell conversion, neighbors/k-rings, cell-boundary movement

Claude should provide concrete commands and experiments as the relevant phase is implemented.

---

# Failure-First Thinking

For important flows, explicitly consider:

- process crash boundaries;
- duplicate delivery;
- older events arriving after newer events;
- persistence success followed by offset/publish failure;
- dependency loss;
- multiple instances running simultaneously;
- restart and replay behavior.

Distinguish what is handled now, protected by another invariant, deliberately deferred, or out of scope.

---

# Tests Should Demonstrate Guarantees

Important tests should encode distributed-system invariants, for example:

- replaying a position event creates no duplicate history row;
- older telemetry cannot regress Redis live state or geo-cell membership;
- one proximity encounter produces one candidate episode;
- a failed proximity Kafka publish can be retried safely;
- an expired evaluator cannot renew or delete another instance's lease;
- deterministic alert identity prevents duplicate durable alert rows;
- WebSocket delivery may repeat after replay, and clients safely deduplicate by `alert_id`.

Do not describe transport delivery as exactly-once. Sentinel uses at-least-once messaging with idempotent durable effects.

---

# Observability Is Part of Implementation

Every phase must provide enough observability to answer: **How do I know this is working?**

Inspect logs, Kafka topics/offsets/lag, TimescaleDB rows/query plans, Redis keys/TTLs, Neo4j relationships, container health, and WebSocket events as appropriate.

Phase 10 completes and standardizes production hardening; it does not introduce observability for the first time.

---

# Scope Control

Work only on the current checkpoint and genuine prerequisites.

Do not:

- implement later phases opportunistically;
- introduce infrastructure not in the architecture;
- add speculative abstractions or ML functionality;
- silently redesign services;
- replace the approved 10-phase roadmap with ad-hoc planning;
- turn tuning values into architectural guarantees without evidence.

---

# After Each Meaningful Checkpoint

Provide a short debrief covering:

- what now works;
- the data flow;
- the system-design concept demonstrated;
- the important trade-off;
- failure behavior;
- commands/queries to verify it independently;
- 2–4 knowledge-check questions;
- the smallest logical next checkpoint.

---

# Definition of Done

A checkpoint is complete only when implementation, tests, running behavior, observable verification, relevant failure behavior, and developer understanding are all present.

The final success criterion is:

> The developer can explain what was built, why it works, what can fail, how to inspect it, and what trade-offs were made.
