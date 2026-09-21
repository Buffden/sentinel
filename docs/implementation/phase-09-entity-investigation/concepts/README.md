# Phase 09 Concepts

Concept notes and debrief records for Phase 09 checkpoints, in the order you'd read them while working through the phase.

| Folder | Observable result |
| --- | --- |
| [entity-list-scope-filtering/](entity-list-scope-filtering/) | CP1: a new, workspace-scoped `GET /entities` (Redis), fail-closed with no saved workspace, demo ad-hoc bbox/unfiltered fallback — kept separate from the existing unscoped `GET /entities/live` map-viewport query. Verified with 116 passing tests and a live manual run against the real dev server (France vs. New York scoping, fail-closed, demo fallback all confirmed against real Redis/Postgres state). |

CP2 (`GET /entities/:entity_id` — live state + recent alerts, including resolving raw entity_ids shown elsewhere in the UI to a display-friendly identity), CP3 (`GET /entities/:entity_id/history`), CP4 (`GET /entities/:entity_id/graph`, first Neo4j read in the API service), and CP5 (richer alert filtering) are not yet built.
