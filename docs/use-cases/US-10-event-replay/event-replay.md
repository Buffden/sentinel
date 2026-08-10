# US-10: Event Replay from Kafka Offset

**Actor:** System
**Status:** Defined

---

## Story

As the system, I want to replay events from a past Kafka offset so that a consumer that was down or restarted can backfill its state without data loss.

---

## Acceptance Criteria

**Mode A — Crash recovery (automatic):**
- A consumer that restarts picks up from its last committed Kafka offset and processes all missed events normally
- No manual intervention required; idempotency keys prevent duplicate records in TimescaleDB and Neo4j
- The timestamp guard (`incoming.timestamp_ms > stored.last_seen_ms`) prevents Redis live-state regression from re-delivered events

**Mode B — Historical backfill (intentional):**
- A developer can rewind a consumer group to an earlier offset to rebuild a specific store (e.g. Neo4j after restart, TimescaleDB history after a schema migration)
- Backfill mode runs under a separate consumer group (or explicit `--mode=backfill` flag) and disables ephemeral side effects: Redis live-state writes, `position-updates` pub/sub, `deviation.candidates`, `proximity.candidates`, and `alerts` are suppressed
- Only the target store is written during backfill
- Backfill mode must not be used to re-warm Redis live state from arbitrary historical replay; prefer latest-row-per-entity from TimescaleDB or a bounded recent Kafka tail for Redis warm-up

---

## Flow Diagrams

### Automatic Restart Replay

![Automatic Restart Replay](../../../diagrams/docs/use-cases/US-10-event-replay/automatic-restart-replay.svg)

A consumer that crashes resumes from its last committed offset on restart, replaying all missed events safely via idempotent writes.

### Intentional Backfill

![Intentional Backfill](../../../diagrams/docs/use-cases/US-10-event-replay/intentional-backfill.svg)

A developer resets a consumer group offset to an earlier point to rebuild store state (e.g. Neo4j after a restart or after a corrected anomaly rule) without re-querying the source feeds.

---

## Architectural Justification

Justifies: [ADR-001 - Kafka over Direct HTTP Ingestion](../../adr/ADR-001-kafka-over-http-ingestion.md)

Kafka's log-based storage retains events for a configurable retention window regardless of whether any consumer has read them. Consumers track their own offset per partition - a restart simply resumes from the last committed offset. Direct HTTP push has no equivalent: once a downstream service misses an HTTP call, the event is gone unless the producer has its own buffer. Replay is also the mechanism by which Neo4j state can be rebuilt after a restart without re-querying the source feeds.
