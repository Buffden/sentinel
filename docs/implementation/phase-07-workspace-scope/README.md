# Phase 07 — Workspace + Operator Scope

## Goal

Make authentication operationally meaningful: an operator's geographic bounds, entity-type filter, and alert-type filter are saved, enforced server-side on both REST and WebSocket delivery, and visible/editable through the dashboard.

See [`phase-07-workspace-scope.md`](phase-07-workspace-scope.md) for the original phase plan (goal, backend/frontend scope, required experiments, exit criteria) — the source plan this README tracks progress against, updated in place only for accepted architectural changes (per `CLAUDE.md`), not rewritten casually.

---

## Checkpoint progress

| Checkpoint | Scope | Status |
| --- | --- | --- |
| Pre-CP1 | Design-only: resolve the `POST`/`PUT /users/me/workspace` contract — request/response shapes, demo-role exclusion, `entity_types` restricted to `["aircraft"]` in v1, `404` no-workspace convention, validation rules. Documented in `DATA_MODEL.md` and this checkpoint's concept doc before any code exists. No code. | Done |
| CP1 | Predefined region list + `POST`/`PUT /users/me/workspace` implementing Pre-CP1's resolved contract. No REST/WebSocket filtering enforcement yet. API only. | Done |
| Pre-CP2 | Design-only: correct ADR-012's stale alert-payload field names against the real payload builders, resolve demo-session scoping (an ad-hoc `bbox` query param, not a saved workspace), and settle on one shared `matchesScope` predicate reused by CP2 and the future CP3. Documented in `ADR-012`, `DATA_MODEL.md`, and this checkpoint's concept doc before any code exists. No code. | Done |
| CP2 | Server-side REST filtering: `GET /alerts` applies the caller's saved scope (or demo's ad-hoc `bbox`) before returning rows, implementing Pre-CP2's resolved contract. | Done |
| Pre-CP3 | Design-only: consolidate `wsServer.ts`'s per-connection state into one `ConnectionState` record, resolve fail-closed loading semantics for an operator's async scope lookup, and confirm demo reuses the existing position `positionBBox` for alerts rather than a new mechanism. Documented in `DATA_MODEL.md` and this checkpoint's concept doc before any code exists. No code. | Done |
| CP3 | Server-side WebSocket filtering: operator scope loaded once at connection open, demo filtered by its live `positionBBox`, both reusing CP2's `matchesScope`, applied to `alert-events` fan-out, implementing Pre-CP3's resolved contract. | Done — 13 tests against a real Postgres/Redis/WebSocket server; a live full-pipeline push demonstration was never run (see CP6's debrief), but the fan-out code exercised is identical regardless of whether the alert-events message came from a real detection or a test publish |
| Pre-CP4 | Design-only: a `generation`-guarded `forceReconnect` for the dashboard's WebSocket client (avoiding a duplicate-reconnect race against the existing drop-and-retry logic), and confirmation that the pre-existing `onReconnect` contract already satisfies "workspace restore on reconnect" without new code. Documented before any code exists. No code. | Done |
| CP4 | Scope-update flow: `PUT` updates the saved scope, dashboard reconnects the WebSocket to pick it up; workspace restore on reload/reconnect, implementing Pre-CP4's resolved contract. | Done — 10 tests including the reconnect-race fix reproduced under test, plus exercised live for real through CP5's save flow during CP5/CP6 testing |
| CP5 | Frontend workspace-controls checkpoint (mockup → approval → implementation): scope prompt, region/entity/alert-type controls, disabled for demo role, saved scope visibly restored after reload. Mockup approved 2026-09-13, implemented, then refined twice from live feedback: a checkbox/select restyle for the dark theme, and a real bug fix (duplicate onClick handlers double-toggling every alert-type checkbox to a net no-op). | Done |
| CP6 | End-to-end verification through the browser: two operators with different scopes see different alerts/map data live. | Done |

Look up any checkpoint's commit with `git log --oneline` from the repository root, once commits exist.

**Two things already existed before this phase started, and are not this phase's work:** Google OAuth + JWT auth (ADR-011) and the `user_workspaces`/`users` tables (migrations 003/004) were built ahead of schedule, alongside the rest of the auth stack. Phase 07 connects code to an already-accepted contract, it does not design auth or the table from scratch.

**API convention adopted at Pre-CP1, applies to every checkpoint above and beyond this phase:** new read endpoints use `POST` with params in the body, not `GET` with a query string, so filters can extend the body later without a breaking URL change. This does not retrofit already-shipped `GET /alerts` or `GET /entities/live`. The WebSocket upgrade handshake is unaffected — it must remain an HTTP `GET` per the WebSocket protocol (RFC 6455), and Sentinel already avoids the underlying problem by loading scope server-side rather than via handshake query params.

---

## Contents

| Path | Description |
| --- | --- |
| [`phase-07-workspace-scope.md`](phase-07-workspace-scope.md) | Original phase plan: goal, backend/frontend scope, required experiments, exit criteria |
| [`concepts/`](concepts/README.md) | Concept notes and checkpoint debriefs, in reading order |

Phase 07 is complete: every checkpoint above (CP1 through CP6) is Done. Closed out 2026-09-14 with two real, independent Google accounts confirming different saved scopes produce different alert sets, matching an independently-computed Postgres count exactly on both sides (2724 in-scope for one operator's US scope, 0 for the other's Western Europe scope, out of 2945 total real alerts). See `concepts/two-operator-verification/two-operator-verification-debrief.md` for the full exit-criteria pass, including the one honestly-acknowledged gap (a live full-pipeline WebSocket push demonstration was never run, though the code path it would exercise is already proven by CP3's own integration tests).
