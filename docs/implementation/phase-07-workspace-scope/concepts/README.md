# Phase 07 Concepts

Concept notes and debrief records for Phase 07 checkpoints, in the order you'd read them while working through the phase.

| Folder | Observable result |
| --- | --- |
| [workspace-scope-contract/](workspace-scope-contract/) | Pre-CP1 design (contract, demo-role exclusion, `entity_types` restricted to `["aircraft"]`, `404` no-workspace convention, validation rules) plus CP1's implementation: a real `PUT`/`POST /users/me/workspace` round trip proven through the actual dashboard and API, with the resulting row inspected directly in Postgres |
| [alert-scope-filtering/](alert-scope-filtering/) | Pre-CP2 design (ADR-012's stale alert-payload field names corrected, demo-session bbox scoping resolved, one shared `matchesScope` predicate settled on) plus CP2's implementation: `GET /alerts` genuinely scoped for both operator and demo sessions, proven with 21 tests against real Postgres and confirmed live against thousands of real alerts (2724 in-scope out of 2945 total, matching an independent SQL count exactly) |
| [ws-alert-scope-filtering/](ws-alert-scope-filtering/) | Pre-CP3 design (`ConnectionState` consolidation, fail-closed async scope load, demo reusing `positionBBox`) plus CP3's implementation: the `alert-events` push stream now genuinely scope-filtered, proven with 13 tests against a real Postgres/Redis/WebSocket server, including a real cross-service test-pollution bug caught and fixed along the way |
| [workspace-reconnect-flow/](workspace-reconnect-flow/) | Pre-CP4 design (generation-guarded `forceReconnect`, `onReconnect` reuse) plus CP4's implementation: the reconnect-on-save mechanism built and the exact race it guards against reproduced and fixed under test (10 tests), later exercised live for real through CP5's save flow |
| [workspace-scope-controls/](workspace-scope-controls/) | The approved mockup (`mockups/workspace-scope-editor.svg`) plus CP5's implementation: a real, clickable scope editor calling CP1-CP4's already-proven functions, confirmed live and refined twice from real feedback — a dark-theme control restyle and a genuine double-toggle checkbox bug only the live click-through caught |
| [two-operator-verification/](two-operator-verification/) | CP6, the phase's own exit test: two real, independent Google accounts with different saved scopes, confirmed live to receive different alert sets, matching an independent Postgres count exactly on both sides |

Phase 07 is complete: every checkpoint (CP1 through CP6) is Done. See the phase [`README.md`](../README.md) checkpoint table and `two-operator-verification/two-operator-verification-debrief.md` for the full exit-criteria pass.
