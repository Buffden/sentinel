# US-10: Event Replay and Historical Backfill

**Actor:** System / Developer
**Status:** Defined

---

## Story

As the system, I want crash recovery and intentional historical replay to be explicit, separate modes so consumers can recover missed work without corrupting current live state or accidentally re-notifying historical anomalies.

---

## Acceptance Criteria

### Mode A — Crash recovery

- A consumer restart resumes from its last committed Kafka offset.
- Normal service behavior remains enabled.
- Duplicate delivery is expected and handled by the normal idempotency/state guards.
- Older telemetry cannot regress Redis current state.

### Mode B — Intentional historical backfill

- Runs under a separate consumer group or explicit backfill mode.
- Writes only the explicitly selected rebuild target.
- Suppresses live Redis state, Redis pub/sub, derived candidate topics, and alerts unless one of those is explicitly the isolated rebuild target.
- Must not share live ephemeral namespaces/state with the production path.
- A safe v1 example is rebuilding `position_history` from retained `adsb.raw` / `ais.raw` records through a backfill-mode Position Consumer.
- Redis live-state recovery uses latest-row-per-entity from TimescaleDB or a deliberately bounded recent Kafka tail; arbitrary full-history replay is not used to warm live Redis.

### Neo4j data-loss boundary

A total Neo4j data-loss rebuild is **not** modeled as simply rewinding the normal Correlation Worker in v1. The live Correlation Worker depends on Redis H3/current-position state to generate pair candidates; suppressing those side effects removes the state required to recompute historical proximity.

Neo4j process restart with persisted storage is normal recovery. Reconstructing a lost graph requires a dedicated offline recomputation from durable historical positions (or a future durable proximity-evidence stream) and is separate from the ordinary Kafka backfill mechanism.

---

## Flow Diagrams

### Automatic Restart Replay

![Automatic Restart Replay](../../../diagrams/docs/use-cases/US-10-event-replay/automatic-restart-replay.svg)

Normal crash recovery resumes from committed offsets and uses regular idempotency protections.

### Intentional Backfill

![Intentional Backfill](../../../diagrams/docs/use-cases/US-10-event-replay/intentional-backfill.svg)

The v1 diagram shows a safe example: rebuilding TimescaleDB position history using a separate backfill consumer path while suppressing live side effects.

---

## Architectural Justification

Justifies: [ADR-001 - Kafka over Direct HTTP Ingestion](../../adr/ADR-001-kafka-over-http-ingestion.md), [ADR-007 - Deterministic Idempotency Identity](../../adr/ADR-007-idempotency-key-schema.md)

Kafka retention enables both automatic catch-up and intentional reprocessing, but "replay" does not mean every derived store can be reconstructed by running the live service unchanged. Rebuildability depends on each service's inputs and state dependencies, so backfill targets must be explicit and isolated.
