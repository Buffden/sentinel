# ADR-014: ADS-B Exchange as the ADS-B Data Source

**Status:** Accepted
**Date:** 2026-08-09

---

## Context

The ingestion poller requires a live ADS-B feed to poll for aircraft position data. Two community receiver networks were evaluated:

1. **OpenSky Network** — academic/research-focused aggregator; filters certain aircraft on operator request (military, LADD-enrolled private aircraft)
2. **ADS-B Exchange** — community-operated aggregator with an explicit no-filter policy; publishes every position report received regardless of aircraft type or operator privacy request

Both networks:
- Receive the same 1090 MHz ADS-B transponder signals via volunteer ground receivers
- Use ICAO24 hex code as the primary aircraft identifier
- Expose a REST API returning state vectors (lat, lon, altitude, heading, speed, squawk, callsign)
- Have significant geographic overlap — commercial traffic over well-covered regions appears in both feeds

The choice of data source is a Phase 2 implementation detail. The downstream pipeline (Kafka topic `adsb.raw`, position consumer normalisation, TimescaleDB, Redis, Neo4j) is identical regardless of which feed is polled.

---

## Decision

Use **ADS-B Exchange** as the ADS-B data source.

---

## Reasoning

**Unfiltered entity set produces a denser graph.** OpenSky redacts aircraft whose operators have enrolled in privacy programmes. ADS-B Exchange publishes all received position reports. A denser, less curated entity set produces more proximity events in Neo4j, which makes the correlation worker and composite alert logic more interesting to demonstrate and exercise.

**Lower feed latency.** ADS-B Exchange aggregates closer to real-time than OpenSky, which introduces aggregation delay. For a live-map demonstration, lower latency produces more visually responsive marker movement.

**No architectural change.** Switching the feed source requires only a different HTTP client and field-name mapping in the position consumer's normalisation step. The Kafka topic name (`adsb.raw`), the normalised event schema, and all downstream services are unchanged. This is not a tech stack change and does not require modifying any other ADR.

---

## Alternatives Considered

### OpenSky Network (rejected)

- More generous free tier (anonymous: 400 requests/day; authenticated users: higher limits with no paid plan required)
- Extensively documented REST API with stable field names
- Research-grade historical archive suitable for seed data and replay — ADS-B Exchange does not offer equivalent historical access
- Cost: filtered entity set reduces graph density; aggregation delay reduces live-map responsiveness
- Rejected in favour of ADS-B Exchange for the reasons above; would be the preferred choice if historical data replay or a more forgiving free tier becomes necessary

### Both feeds simultaneously (rejected)

- Both networks receive the same underlying transponder signals; commercial traffic over well-covered regions appears in both feeds with the same ICAO24 identifier
- The idempotency key `{entity_id}:{timestamp_ms}` does not deduplicate position reports from different receivers that timestamp the same ping differently — dual-sourcing the same entity type inflates `position_history`, skews the `route_baseline` continuous aggregate, and could produce false deviation increments
- Adding a second ADS-B source demonstrates no new system design concept; multi-source ingestion is better demonstrated by adding AIS (vessels) as a genuinely different entity type with a distinct schema and Kafka topic (`ais.raw`)

---

## Consequences

- The ingestion poller polls the ADS-B Exchange REST API; an `ADSB_EXCHANGE_API_KEY` environment variable is required and must be added to `.env.example`
- ADS-B Exchange field names differ from OpenSky (e.g. `hex` vs `icao24`, `alt_baro` vs `baro_altitude`); the position consumer's normalisation step must map ADS-B Exchange field names to the internal normalised schema — this mapping should be explicit and commented, not implicit
- ADR-013 references OpenSky by name; the implementation follows this ADR (ADS-B Exchange) — ADR-013 remains valid for the language and runtime choice
- Rate limits on the ADS-B Exchange free tier must be respected; `POLL_INTERVAL_MS` should be set conservatively during development to avoid exhausting the quota
- If historical data replay is needed for development or load testing, OpenSky's research archive remains available as a seed data source independent of the live feed choice
