# POC-02: OpenSky Feed

**Branch:** `poc/opensky-feed`
**Status:** Not started

---

## Risk

The OpenSky Network API is an external dependency with rate limits, credential requirements, and a data format that must match what the pipeline assumes. If it does not behave as expected, the synthetic generator becomes the primary data path and the ingestion design changes.

---

## Decisions Made Here

### Authentication — OAuth2 client credentials

OpenSky uses OAuth2 client credentials (not HTTP basic auth). The poller requests a bearer token from the OpenSky token endpoint using `OPENSKY_CLIENT_ID` and `OPENSKY_CLIENT_SECRET`, then attaches it as `Authorization: Bearer <token>` on each API request. Authenticated requests receive higher rate limits than anonymous access. Tokens are cached and refreshed before expiry.

**Why not basic auth?** OpenSky's current API uses OAuth2 client credentials for authenticated access. `OPENSKY_USERNAME` / `OPENSKY_PASSWORD` is not the current auth model and must not appear in configuration.

### Timestamp — `time_position` with null guard

Each OpenSky state vector carries two timestamps: `time` (server-side collection time) and `time_position` (last time the transponder updated position). Sentinel uses `time_position` as `timestamp_ms` because it reflects when the aircraft was actually at that position — not when the server collected it.

`time_position` **can be null** when a state vector is assembled from non-position messages or when the aircraft's last position update predates the current poll window. Handling decision: **skip with metric increment** (`states_without_position_total`) — a null `time_position` is a valid OpenSky source state, not a parse failure; DLQ'ing it would flood the DLQ with routine data and obscure genuine failures. Similarly, null `latitude`/`longitude` fields are skipped with a metric.

Events where `now() - time_position > STALE_POSITION_THRESHOLD_MS` at ingest time are also **skipped with a metric** (`states_stale_position_total`) — they represent outdated tracks that would mislead the staleness detection logic. These are parseable events, not DLQ candidates; log at debug level only.

### Request scope — bounding-box over global

OpenSky charges credits per request. Global `/states/all` requests consume more credits than bounding-box requests (`/states/all?lamin=...&lomin=...&lamax=...&lomax=...`). The poller uses a configurable bounding box (`OPENSKY_BBOX`) to constrain the polling area and reduce credit consumption. Default: a region covering the intended demo area.

---

## Goal

Confirm that live ADS-B data is accessible, parseable, and sufficient for the system's needs. Also validate the full two-stage ingestion pipeline: the Poller publishing raw events to `adsb.raw`, and the Position Consumer reading from `adsb.raw`, normalising, publishing to `position.normalized`, and routing malformed events to the DLQ.

---

## Scope

This POC covers two components working together:

- **Ingestion Poller** - polls OpenSky with OAuth2 bearer token and bounding-box, publishes raw events to `adsb.raw`. Responsible only for fetching and forwarding. No parsing, no DLQ.
- **Position Consumer** - reads from `adsb.raw`, attempts to parse and normalise, writes to `position.normalized` on success, routes to `adsb.dlq` on failure. DLQ routing is a consumer responsibility, not a poller responsibility.

---

## Validate

- OAuth2 client credentials (`OPENSKY_CLIENT_ID` / `OPENSKY_CLIENT_SECRET`) exchange for a bearer token successfully
- Token cached and refreshed before expiry — confirmed by logging token acquisition events
- Bounding-box request returns state vectors and is cheaper than global `/states/all`
- Response payload contains the fields assumed in the pipeline: `icao24` (entity_id), `time_position` (timestamp), `latitude`, `longitude`, `baro_altitude`
- `time_position` null (or null `latitude`/`longitude`) → skipped with metric increment `states_without_position_total` (not DLQ'd)
- Events where `now() - time_position > STALE_POSITION_THRESHOLD_MS` → skipped with metric `states_stale_position_total` (not DLQ'd)
- Polling at the intended interval does not exceed rate limits; rate limit behaviour documented
- Live data volume is sufficient to exercise the pipeline (or document that the synthetic generator is needed)
- Poller publishes raw event bytes to `adsb.raw` without any parsing - format fidelity only
- Position Consumer successfully reads from `adsb.raw` and publishes normalised events to `position.normalized`
- A deliberately malformed event injected into `adsb.raw` is routed by the Position Consumer to `adsb.dlq` with the original payload and rejection reason attached - consumer does not crash or stall (US-09)
- Events in `adsb.raw` can be replayed from an earlier offset and the Position Consumer produces the same output in the same order (US-10)

---

## Done When

- Poller fetches live OpenSky data via OAuth2 bearer token and bounding-box request
- Token refresh flow confirmed (token cached, re-requested before expiry)
- `time_position` null and stale events are skipped with correct metric increments (not DLQ'd); DLQ only receives structurally unparseable events
- Rate limit behaviour documented (credits per request, requests per minute allowed, response on breach)
- Data format confirmed or field-name mismatches noted for the normalisation step
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
