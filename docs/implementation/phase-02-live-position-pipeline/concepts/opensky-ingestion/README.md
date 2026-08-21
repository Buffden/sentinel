# OpenSky Ingestion Poller

The OpenSky ingestion checkpoint is about connecting Sentinel to a real external data source for the first time. A long-running poller fetches live ADS-B state vectors from the OpenSky Network REST API on a fixed interval and publishes one `adsb.raw` Kafka message per aircraft, keyed by ICAO 24-bit address.

Read these in order:

| File | What it covers |
| --- | --- |
| [opensky-api.md](opensky-api.md) | OpenSky state vector format, array-of-arrays response shape, field index map, `time_position` vs `last_contact`, rate limits |
| [polling-loop.md](polling-loop.md) | Provider-fidelity responsibility boundary, bounding box, fetch timeout, error handling, graceful shutdown |
| [opensky-poller-debrief.md](opensky-poller-debrief.md) | What we actually observed — three poll cycles, partition state, record inspection, key verification |

## Diagrams

| File | Shows |
| --- | --- |
| [polling-sequence.puml](polling-sequence.puml) | Time-ordered interaction: poller startup, poll cycle, Kafka publish, interval wait, shutdown |
