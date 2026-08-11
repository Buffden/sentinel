# IMPLEMENTATION_WORKFLOW.md

# Purpose

Sentinel intentionally does not maintain a permanent `docs/implementation/` directory.

Detailed implementation plans are created **just in time**, based on the current architecture and the code that already exists.

The goal is to:

* avoid speculative implementation planning;
* keep architectural documentation stable;
* implement Sentinel in small, observable increments;
* make implementation a hands-on system-design learning process;
* ensure the developer understands and can defend what is being built.

Claude should behave as a senior engineer pairing with the developer, not as an autonomous code generator.

---

# Sources of Truth

Before implementing anything significant, use this order:

1. `README.md`

   * project scope;
   * current build phase;
   * high-level build order.

2. `docs/adr/`

   * architectural decisions;
   * rationale;
   * rejected alternatives.

3. `docs/ARCHITECTURE.md`

   * service boundaries;
   * ownership;
   * data flow;
   * system behavior.

4. `docs/DATA_MODEL.md`

   * canonical schemas;
   * Redis keys;
   * Kafka contracts;
   * database contracts;
   * event fields.

5. `docs/use-cases/`

   * expected behavior and scenarios.

6. Existing code, migrations, configuration, and tests

   * current executable reality.

---

# Starting a Phase

When beginning a phase:

1. Read the relevant architecture and design documents.
2. Inspect everything already implemented that the new work depends on.
3. Identify what is already decided.
4. Identify real implementation choices that remain open.
5. Identify the system-design concepts involved.
6. Produce a concise implementation plan for the current phase.
7. Recommend the smallest useful first checkpoint.

Do not write code until the developer understands the first checkpoint.

The plan should separate:

## Already Decided

Architecture that should be followed rather than debated again.

## Implementation Choices

Decisions that can reasonably be made during implementation.

Examples:

* Kafka partition count;
* batching;
* retry strategy;
* library usage;
* module structure;
* connection-pool configuration;
* timeout defaults.

Explain meaningful trade-offs and recommend a starting choice.

Values that depend on runtime behavior should be validated experimentally rather than treated as permanent truths.

## Learning Goals

Concepts the developer should understand while implementing the phase.

## Checkpoints

Small pieces of implementation that can be built and verified independently.

## Failure Cases

Important failures that should be understood or tested.

---

# Work in Small Checkpoints

Do not implement an entire phase in one large code-generation pass.

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

For example, a Kafka checkpoint might be:

1. start Redpanda;
2. create one topic;
3. publish one event manually;
4. consume it manually;
5. inspect the partition and offset;
6. then implement the same flow in application code.

Do not hide unfamiliar infrastructure behind application abstractions before the developer has interacted with it directly.

---

# Prefer Vertical Slices

Prefer proving one small end-to-end path over generating every layer in advance.

Good:

OpenSky sample event
→ Kafka
→ Position Consumer
→ TimescaleDB

Then extend it.

Avoid starting with:

* generic repository abstractions;
* large utility layers;
* speculative factories;
* wrappers around every infrastructure dependency;
* future-phase abstractions.

Build enough structure for the current requirement.

Refactor when repeated patterns actually appear.

---

# Teach Before Automating

When introducing an unfamiliar technology or distributed-systems concept, first establish the mental model.

Examples include:

* Kafka topics;
* partitions;
* consumer groups;
* offsets;
* replay;
* at-least-once delivery;
* idempotency;
* event ordering;
* TimescaleDB hypertables;
* indexing;
* Redis live state;
* TTLs;
* leader election;
* leases;
* Neo4j relationships;
* H3;
* WebSockets;
* event time vs processing time.

Use Sentinel-specific examples.

Do not explain only library syntax.

For example, before teaching `consumer.run()`, explain:

* what Kafka is assigning to the consumer;
* what an offset represents;
* when an offset is committed;
* what happens if the process crashes;
* why the same event may be processed again.

---

# Hands-On First

For unfamiliar infrastructure, prefer small manual experiments.

## Kafka / Redpanda

The developer should eventually be able to:

* create a topic;
* inspect partitions;
* produce a message;
* consume a message;
* inspect offsets;
* restart a consumer;
* observe replay;
* understand consumer groups.

## TimescaleDB

The developer should:

* inspect the schema;
* insert data manually;
* query it;
* inspect indexes;
* use `EXPLAIN ANALYZE`;
* test duplicate insertion;
* inspect hypertable/chunk behavior.

## Redis

The developer should:

* inspect hashes;
* inspect TTLs;
* inspect sorted sets;
* observe lease state;
* watch keys expire;
* manually inspect live entity state.

## Neo4j

The developer should:

* create nodes;
* create relationships;
* query them;
* inspect the graph;
* understand `MERGE`;
* see how duplicate evidence is prevented.

## H3

The developer should:

* convert coordinates to H3 cells;
* inspect neighboring cells;
* move entities across cell boundaries;
* understand how resolution changes candidate density.

Claude should provide the commands needed for these experiments.

---

# Failure-First Thinking

For important distributed flows, identify the relevant failure boundaries.

Ask questions such as:

* What if the process crashes here?
* What if this event is delivered twice?
* What if an older event arrives after a newer one?
* What if persistence succeeds but Kafka offset commit fails?
* What if Redis disappears?
* What if Neo4j succeeds but downstream publication fails?
* What if two instances run at the same time?
* What happens on restart?
* What happens during replay?

Not every failure must be solved immediately.

Clearly distinguish:

* handled now;
* protected by another invariant;
* deliberately deferred;
* out of scope.

---

# Tests Should Demonstrate Guarantees

Important tests should encode system invariants.

Prefer tests such as:

* replaying a position event does not create duplicate history;
* older telemetry cannot replace newer Redis live state;
* one proximity encounter produces one candidate episode;
* a failed Kafka publish can be retried safely;
* an expired alert evaluator cannot renew another instance's lease;
* deterministic alert identity prevents duplicate durable alerts.

Tests should explain why the distributed-system behavior is correct, not merely verify function syntax.

---

# Observability Is Part of Implementation

For every important component, the developer should know how to answer:

> How do I know this is working?

Show how to inspect:

* application logs;
* Kafka topics;
* Kafka offsets;
* consumer lag;
* TimescaleDB rows;
* query plans;
* Redis keys;
* TTLs;
* Neo4j relationships;
* Docker container health;
* WebSocket events.

Prefer observable evidence over assumptions.

---

# Architectural Changes During Implementation

Implementation may reveal that a planning assumption was wrong.

If that happens:

1. stop before silently changing architecture;
2. explain the evidence;
3. identify the affected ADR, architecture section, or data contract;
4. present realistic alternatives;
5. explain trade-offs;
6. recommend an option;
7. let the developer make the architectural decision;
8. update documentation and code together.

Do not preserve a clearly incorrect architectural assumption merely because it was previously documented.

Do not silently redesign Sentinel inside implementation code.

---

# Scope Control

Work only on the current checkpoint and genuine prerequisites.

Do not:

* implement later phases opportunistically;
* introduce infrastructure not in the architecture;
* add speculative abstractions;
* add ML functionality;
* redesign services without discussion;
* recreate the deleted implementation-plan directory;
* turn tuning values into architectural guarantees without evidence.

If useful future work is discovered, mention it briefly and continue the current task.

---

# After Each Meaningful Checkpoint

Give the developer a short debrief.

## What We Built

What now works?

## Data Flow

Where does data enter, move, and leave?

## System-Design Concept

What important concept did this checkpoint demonstrate?

## Trade-Off

What meaningful trade-off did we accept?

## Failure Behavior

What happens during the most relevant failure?

## Verify It Yourself

Give commands, queries, API calls, or experiments the developer can run independently.

## Knowledge Check

Ask 2–4 questions about the implementation.

Examples:

* Why can Kafka deliver this event more than once?
* Why does this database write remain safe during replay?
* Why is this state stored in Redis instead of TimescaleDB?
* What happens if this service crashes immediately after this line?

## Next Checkpoint

Recommend the smallest logical next step.

---

# Definition of Done

A checkpoint is not complete merely because code was generated or compiled.

For infrastructure-heavy work, completion should normally include:

* implementation;
* test;
* running behavior;
* observable verification;
* relevant failure behavior;
* developer understanding.

The final success criterion is:

> The developer can explain what was built, why it works, what can fail, how to inspect it, and what trade-offs were made.
