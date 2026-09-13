# Phase 07 Concepts

Concept notes and debrief records for Phase 07 checkpoints, in the order you'd read them while working through the phase.

| Folder | Observable result |
| --- | --- |
| [workspace-scope-contract/](workspace-scope-contract/) | Pre-CP1 design (contract, demo-role exclusion, `entity_types` restricted to `["aircraft"]`, `404` no-workspace convention, validation rules) plus CP1's implementation: a real `PUT`/`POST /users/me/workspace` round trip proven through the actual dashboard and API, with the resulting row inspected directly in Postgres |
| [alert-scope-filtering/](alert-scope-filtering/) | Pre-CP2 design (ADR-012's stale alert-payload field names corrected, demo-session bbox scoping resolved, one shared `matchesScope` predicate settled on) plus CP2's implementation: `GET /alerts` genuinely scoped for both operator and demo sessions, proven with 21 tests against real Postgres and confirmed live against thousands of real alerts (2724 in-scope out of 2945 total, matching an independent SQL count exactly) |

Phase 07 is in progress; see the phase [`README.md`](../README.md) checkpoint table.
