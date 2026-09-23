# Phase 10 Concepts

Concept notes, experiment records and debriefs, in the order you'd read them while working through this phase.

| Folder | Checkpoint | Observable result |
| --- | --- | --- |
| [provider-experiment/](provider-experiment/) | Pre-CP1 | A 15 minute side-by-side OpenSky and adsb.fi measurement over SF Bay that decided ADR-020: adsb.fi as the regional primary, OpenSky as the fallback |
| [adsbfi-primary-ingestion/](adsbfi-primary-ingestion/) | CP1 | Real adsb.fi positions flow through the `{ provider, payload }` envelope on `adsb.raw` into `position_history` and Redis with provider `adsbfi`, altitudes in metres, the ground state mapped, and matching source timestamps in both stores |
