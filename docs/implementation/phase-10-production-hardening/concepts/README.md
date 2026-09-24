# Phase 10 Concepts

Concept notes, experiment records and debriefs, in the order you'd read them while working through this phase.

| Folder | Checkpoint | Observable result |
| --- | --- | --- |
| [provider-experiment/](provider-experiment/) | Pre-CP1 | A 15 minute side-by-side OpenSky and adsb.fi measurement over SF Bay that decided ADR-020: adsb.fi as the regional primary, OpenSky as the fallback |
| [adsbfi-primary-ingestion/](adsbfi-primary-ingestion/) | CP1 | Real adsb.fi positions flow through the `{ provider, payload }` envelope on `adsb.raw` into `position_history` and Redis with provider `adsbfi`, altitudes in metres, the ground state mapped, and matching source timestamps in both stores |
| [opensky-fallback-hardening/](opensky-fallback-hardening/) | CP2 | The OpenSky poller projects its daily credit spend at startup, logs `credits_remaining` every cycle, and on a real `429` pauses once for OpenSky's retry time (about 8 hours when observed) with no requests in between |
| [provider-outage-experiment/](provider-outage-experiment/README.md) | CP3 (research) | Cutting adsb.fi off with the rest of the pipeline running made all 78 airborne aircraft raise `SIGNAL_LOSS` in one scan. Grounded aircraft raised none, and 16 aircraft stayed gated after recovery. This is the evidence for ADR-022 |
| [coordinator-lease/](coordinator-lease/) | CP3a | Two ingestion coordinators run against one Redis lease: only the holder polls adsb.fi and publishes, `heartbeat_ms` advances every 5 s, and after the leader is killed with `kill -9` the follower takes over in 15.9 s and continues from the same Kafka offset |
| [authority-coverage-timeline/](authority-coverage-timeline/) | CP3b | The coordinator commits adsb.fi authority and keeps a Redis coverage timeline: fresh cycles extend coverage without a new revision, a real broker outage closed it as `failure` at the last success, a `kill -9` left a segment the next lease holder closed as `coordinator_down`, and a wrong token wrote nothing |
