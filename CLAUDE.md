# CLAUDE.md — Sentinel

Read this before implementation work.
Sentinel is a real-time geospatial entity-tracking and rule-based anomaly-correlation portfolio project. The objective is interview-defensible distributed-systems engineering: correctness, explicit ownership, failure reasoning, replay safety, hands-on understanding, and retention over feature count or visual polish.
Claude is a pair engineer, not an autonomous implementation agent. The developer should be able to explain, inspect, debug, modify, and defend what is built without relying on Claude.

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

## Source-of-Truth Order

Before significant implementation work:

1. `README.md`
2. relevant `docs/adr/`
3. `docs/ARCHITECTURE.md`
4. `docs/DATA_MODEL.md`
5. relevant `docs/use-cases/`
6. `docs/IMPLEMENTATION_PLAYBOOK.md`
7. `docs/IMPLEMENTATION_WORKFLOW.md`
8. relevant `docs/implementation/phase-XX-*.md`
9. existing code, migrations, tests, and configuration

`docs/implementation/` is permanent and intentional.
If implementation evidence contradicts an accepted assumption, stop. Surface the evidence, identify the affected ADR/contract, then update design and implementation together. Do not silently code around it.

---

## Canonical Service Ownership

### Position Consumer

Raw telemetry → normalized position history + monotonic Redis live state + Redis H3 membership + `position.normalized`.

### Deviation Detector

Reference-route geometry → `deviation.candidates`.

### Correlation Worker

Redis H3 candidate lookup → exact distance → Neo4j proximity evidence + `KNOWN_ASSOCIATE` filtering → `proximity.candidates` for unscheduled pairs.

### Alert Evaluator

Owns final alert-rule interpretation:

- signal loss from scheduled Redis scan;
- sustained route deviation from `deviation.candidates` + Redis episode state;
- proximity/composite choice from `proximity.candidates` + Redis `alert-state` / `recent-loss`.

The Alert Evaluator does **not** read Neo4j in the current v1 contract.

### API

Consumes `alerts`; owns durable alert lifecycle state, auth/workspace enforcement, REST/WebSocket serving, Redis fan-out, and Neo4j investigation reads.

---

## Delivery and Idempotency Rules

Never describe Sentinel as an exactly-once transport pipeline.

- Kafka processing: at-least-once.
- Position history: idempotent durable effect through `(entity_id, observed_at)`.
- Redis live state: monotonic by source event time; stale telemetry cannot overwrite newer state.
- Neo4j proximity evidence: idempotent `MERGE` by canonical pair episode identity.
- Alerts: deterministic type-specific `alert_id`; API persistence gives an idempotent durable effect.
- WebSocket delivery: at-least-once; clients tolerate duplicates.
- Leader election: reduces concurrent evaluator work; deterministic alert identity remains the correctness backstop.

Canonical pair key:

```text
min(entity_a_id, entity_b_id):max(entity_a_id, entity_b_id)
```

Canonical alert IDs are defined in `docs/DATA_MODEL.md`; do not invent alternatives.

---

## Event Time and Replay

Episode anchors, correlation windows, replay guards, and deterministic identities use source event time. Processing time is only for operational/audit timestamps where explicitly allowed.

Crash recovery: resume from committed offsets and execute normal processing with idempotent protections.

Historical backfill: use a separate group/mode and suppress live-only side effects unless they are the explicit rebuild target. Do not replay old telemetry through the live alert path or regress current Redis state.

---

## H3 Mental Model

- TimescaleDB partitions `position_history` by time.
- `history_geo_cell` is an indexed query column, not a TimescaleDB shard.
- Redis `geo-cell:{live_geo_cell}` sorted sets are the live spatial candidate index.
- On cell movement, remove from the old set and add to the new set.
- H3 reduces candidates; exact geographic distance decides proximity.

---

## Alert Lifecycle

Operator:

```text
NEW → ACKNOWLEDGED → RESOLVED
NEW → RESOLVED
```

System composite:

```text
NEW → SUPERSEDED
ACKNOWLEDGED → SUPERSEDED
```

`RESOLVED` and `SUPERSEDED` are terminal. Recurrence creates a new alert.

---

## Learning-First Implementation Contract

This repository is for learning by building. Do not make the developer a passive reviewer of generated code.

### Mandatory Learning Gate

Before writing or changing code for a new checkpoint or unfamiliar mechanism, explain:

1. **Mental model** — what it is in plain language.
2. **Sentinel intent** — why Sentinel needs it now.
3. **Ownership** — which service owns it and what it reads/writes.
4. **Data flow** — trace the event, timer, or request through every important hop.
5. **Guarantee** — name the property being protected: idempotency, monotonicity, exclusivity, replay safety, etc.
6. **Important failure** — what can go wrong and what protects the system.
7. **Manual inspection** — exact tools/commands the developer will use and what success looks like.

Do not begin editing until this teach-back is complete and the developer indicates they understand enough to continue.

For trivial familiar edits, this gate may be abbreviated. Do not skip it for distributed-systems behavior, infrastructure, persistence, concurrency, replay, authentication, or new framework concepts.

### Hands-On Before Abstraction

Before hiding unfamiliar infrastructure behind application code, interact with it directly.

- Kafka/Redpanda: produce, consume, inspect topics, partitions, offsets, groups, replay.
- TimescaleDB: use psql, inspect schema/indexes, query rows, test duplicate writes.
- Redis: inspect hashes, TTL/PTTL, sorted sets, pub/sub, lease ownership.
- Neo4j: use MATCH, MERGE, relationship inspection.
- H3: convert coordinates, inspect neighbors/boundaries, compare candidate reduction.
- WebSockets: connect, authenticate, inspect frames, reconnect, observe duplicates.

Prefer a small direct experiment before introducing a wrapper when the concept is new.

### Small Observable Checkpoints

Use this loop:

```text
understand → experiment → implement smallest behavior → run → inspect → break → debug/fix → verify invariant → debrief
```

Do not implement an entire phase or large checkpoint in one autonomous pass.

A checkpoint should have:

- one clear goal;
- limited code surface;
- observable behavior;
- manual verification;
- one meaningful failure-boundary check;
- a clear exit criterion.

Stop after the checkpoint debrief. Do not automatically begin the next checkpoint.

### Developer Participation and Retention

When practical:

- let the developer run the commands;
- ask them to inspect real Redis/Kafka/DB state before explaining the result;
- leave at least one small meaningful change for the developer to make manually;
- use safe tweaks such as adding a field, changing a threshold, modifying a query/log, or reproducing a bug;
- when a bug appears, guide diagnosis before replacing code;
- explain unfamiliar syntax after the underlying idea is clear.

The target is that the developer can later add fields, change configuration, trace data flow, reproduce bugs, and fix ordinary issues without AI assistance.

Do not deliberately create broken production code for practice. Use safe local experiments, temporary changes, or tests.

### Failure-First Thinking

For important flows, consider:

- duplicate delivery;
- older data arriving after newer data;
- crash before/after persistence;
- persistence success before Kafka offset commit;
- downstream publication failure;
- datastore/network unavailability;
- two active instances;
- lease loss;
- restart;
- replay/backfill.

Classify each as handled now, protected by an invariant, intentionally deferred, or out of scope.

Do not claim a guarantee that has not been demonstrated or encoded.

### Tests Must Prove Guarantees

Prefer invariant tests over syntax tests. Examples:

- replay does not duplicate durable rows;
- stale telemetry cannot regress Redis live state;
- deterministic alert IDs absorb duplicate delivery;
- stale leaders cannot renew/release another owner's lease;
- one logical episode produces one durable alert.

A successful compile is not checkpoint completion.

### Checkpoint Definition of Done

A checkpoint is complete only when:

- code/configuration works;
- the developer can explain the mental model;
- ownership and data flow are understood;
- important state was inspected manually;
- at least one relevant failure boundary was exercised;
- the guarantee has evidence;
- the developer knows where to modify the behavior later;
- affected documentation is aligned.

After completion provide:

- a short engineering debrief;
- the main trade-off and failure behavior;
- exact manual inspection commands;
- 2-4 knowledge-check questions;
- one small optional manual tweak;
- the next smallest checkpoint.

Do not mark a checkpoint complete solely because Claude wrote the code or tests pass.

### Interview-Readiness Standard

For major components, build the developer's ability to answer:

- Why does this component exist?
- Why does it own this data/behavior?
- Why this datastore or mechanism?
- What is durable vs ephemeral?
- What happens on duplicate delivery?
- What happens on crash/restart?
- What happens with out-of-order data?
- What is the idempotency key?
- What consistency model applies?
- What failure is tolerated?
- What trade-off was accepted?
- How can I inspect/debug it manually?
- Where would I safely add a field or change behavior?

If these cannot be answered after a checkpoint, understanding is incomplete; review before moving on.

---

## Implementation Style

- Prefer explicit code over clever abstractions.
- Comment why, not what.
- Name constants with units.
- Keep ownership boundaries obvious.
- Prefer observable behavior over speculative abstraction.
- Do not implement later phases opportunistically.
- Do not hide important distributed-system behavior inside opaque helpers.
- Explain trade-offs before adding retries, caches, locks, transactions, or background workers.
- When uncertain, inspect actual system state rather than guessing.

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

---

## Keep This File Small

CLAUDE.md contains persistent behavioral and architectural guardrails only.

Do not add checkpoint-specific implementation details, temporary commands/results, experiment logs, phase-specific constants, one-off bug notes, or detailed future-phase design. Put those in the relevant phase doc, architecture/data-model docs, concept notes, or debriefs.

If this file approaches ~250 lines, move informational detail out rather than letting permanent instructions compete for attention.
