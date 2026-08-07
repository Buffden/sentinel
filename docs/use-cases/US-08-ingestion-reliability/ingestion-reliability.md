# US-08: Burst-Tolerant Ingestion

**Actor:** Ingestion client
**Status:** Defined

---

## Story

As the ingestion client, I want to publish position pings to the pipeline without being blocked by slow downstream consumers so that bursts in feed volume do not cause data loss.

---

## Acceptance Criteria

- The ingestion poller can publish at its maximum poll rate without waiting for the position consumer or correlation worker to process each event
- A downstream consumer being slow, restarting, or temporarily unavailable does not cause the poller to drop events or block
- Events published during a consumer outage are retained and processed when the consumer recovers

---

## Flow Diagrams

**Normal ingestion** - the poller publishes position pings to Kafka and returns immediately; each consumer group reads independently at its own pace with no back-pressure reaching the poller.

**Consumer outage recovery** - Kafka retains events on disk while a consumer is down; on recovery the consumer resumes from its last committed offset and replays all missed events safely.

---

## Architectural Justification

Justifies: [ADR-001 - Kafka over Direct HTTP Ingestion](../../adr/ADR-001-kafka-over-http-ingestion.md)

Direct HTTP from the poller to downstream services couples producer throughput to consumer availability. If a consumer is slow or restarting, the poller must implement its own retry, backoff, and buffer logic. Kafka acts as a durable temporal buffer between producer and consumers - the poller publishes and moves on, consumers read at their own pace, and Kafka retains events for the configured retention window regardless of consumer state.
