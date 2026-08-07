# POC-02: OpenSky Feed

**Branch:** `poc/opensky-feed`
**Status:** Not started

---

## Risk

The OpenSky Network API is an external dependency with rate limits, credential requirements, and a data format that must match what the pipeline assumes. If it does not behave as expected, the synthetic generator becomes the primary data path and the ingestion design changes.

---

## Goal

Confirm that live ADS-B data is accessible, parseable, and sufficient for the system's needs. Also validate the full two-stage ingestion pipeline: the Poller publishing raw events to `adsb.raw`, and the Position Consumer reading from `adsb.raw`, normalising, publishing to `position.normalized`, and routing malformed events to the DLQ.

---

## Scope

This POC covers two components working together:

- **Ingestion Poller** - polls OpenSky, publishes raw events to `adsb.raw`. Responsible only for fetching and forwarding. No parsing, no DLQ.
- **Position Consumer** - reads from `adsb.raw`, attempts to parse and normalise, writes to `position.normalized` on success, routes to `adsb.dlq` on failure. DLQ routing is a consumer responsibility, not a poller responsibility.

---

## Validate

- OpenSky credentials work and the REST API responds
- Response payload contains the fields assumed in the pipeline: entity identifier (ICAO hex), timestamp (Unix ms), latitude, longitude, and altitude
- Polling at the intended interval does not exceed rate limits
- Live data volume is sufficient to exercise the pipeline (or document that the synthetic generator is needed)
- Poller publishes raw event bytes to `adsb.raw` without any parsing - format fidelity only
- Position Consumer successfully reads from `adsb.raw` and publishes normalised events to `position.normalized`
- A deliberately malformed event injected into `adsb.raw` is routed by the Position Consumer to `adsb.dlq` with the original payload and rejection reason attached - consumer does not crash or stall (US-09)
- Events in `adsb.raw` can be replayed from an earlier offset and the Position Consumer produces the same output in the same order (US-10)

---

## Done When

- Poller script successfully fetches live OpenSky data and publishes raw payloads to `adsb.raw`
- Rate limit behaviour is documented (requests per minute allowed, response on breach)
- Data format is confirmed or any field-name mismatches are noted for the normalisation step
- Position Consumer reads from `adsb.raw` and normalised events appear in `position.normalized`
- A simulated bad event in `adsb.raw` lands in `adsb.dlq` with original payload and rejection reason - confirmed via Redpanda console or consumer script
- Replaying `adsb.raw` from an earlier offset produces the same normalised output in `position.normalized`

---

## ADR Coverage

[ADR-001 - Kafka over Direct HTTP Ingestion](../../adr/ADR-001-kafka-over-http-ingestion.md)

## Use Case Coverage

- [US-08](../../use-cases/US-08-ingestion-reliability/ingestion-reliability.md) - burst-tolerant ingestion
- [US-09](../../use-cases/US-09-dead-letter-queue/dead-letter-queue.md) - dead-letter queue
- [US-10](../../use-cases/US-10-event-replay/event-replay.md) - event replay
