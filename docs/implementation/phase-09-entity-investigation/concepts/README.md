# Phase 09 Concepts

Concept notes and debrief records for Phase 09 checkpoints, in the order you'd read them while working through the phase.

| Folder | Observable result |
| --- | --- |
| [entity-list-scope-filtering/](entity-list-scope-filtering/) | CP1: a new, workspace-scoped `GET /entities` (Redis), fail-closed with no saved workspace, demo ad-hoc bbox/unfiltered fallback — kept separate from the existing unscoped `GET /entities/live` map-viewport query. Verified with 116 passing tests and a live manual run against the real dev server (France vs. New York scoping, fail-closed, demo fallback all confirmed against real Redis/Postgres state). |
| [entity-detail-alert-join/](entity-detail-alert-join/) | CP2: `GET /entities/:entity_id` joins Redis live state (no staleness cutoff -- a dark entity's last known state is the point) with its recent alert history (as primary entity or counterparty) from Postgres. Closes the by-id enumeration gap CP1's list-only scoping left open, fail-closed to `404`. Verified with 124 passing tests (8 new, including two route-mount-order regressions) and a live manual run against the real dev server. |

CP3 (`GET /entities/:entity_id/history`, TimescaleDB), CP4 (`GET /entities/:entity_id/graph`, first Neo4j read in the API service), and CP5 (richer alert filtering) are not yet built.
