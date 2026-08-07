# POC-02: OpenSky Feed

**Branch:** `poc/opensky-feed`
**Status:** Not started

---

## Risk

The OpenSky Network API is an external dependency with rate limits, credential requirements, and a data format that must match what the pipeline assumes. If it does not behave as expected, the synthetic generator becomes the primary data path and the ingestion design changes.

---

## Goal

Confirm that live ADS-B data is accessible, parseable, and sufficient for the system's needs. Also validate that the Kafka producer and DLQ routing work correctly against real feed data.

---

## Validate

- OpenSky credentials work and the REST API responds
- Response payload contains the fields assumed in the pipeline: entity identifier (ICAO hex), timestamp (Unix ms), latitude, longitude, and altitude
- Polling at the intended interval does not exceed rate limits
- Live data volume is sufficient to exercise the pipeline (or document that the synthetic generator is needed)
- A deliberately malformed event is correctly routed to the DLQ topic without crashing the consumer (US-09)
- Events produced to Kafka have the correct schema to support replay from an earlier offset (US-10)

---

## Done When

- A poller script successfully fetches and prints parsed position records from the live OpenSky API
- Rate limit behaviour is documented (requests per minute allowed, response on breach)
- Data format is confirmed or any field-name mismatches are noted for the normalization step
- A simulated bad event lands in the DLQ with the original payload and rejection reason attached
- Replaying from an earlier Kafka offset produces the same events in the same order

---

## ADR Coverage

[ADR-001 - Kafka over Direct HTTP Ingestion](../../adr/ADR-001-kafka-over-http-ingestion.md)

## Use Case Coverage

- [US-08](../../use-cases/US-08-ingestion-reliability/ingestion-reliability.md) - burst-tolerant ingestion
- [US-09](../../use-cases/US-09-dead-letter-queue/dead-letter-queue.md) - dead-letter queue
- [US-10](../../use-cases/US-10-event-replay/event-replay.md) - event replay
