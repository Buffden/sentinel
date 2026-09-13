# Phase 07 — Workspace + Operator Scope

## Goal

Make authentication operationally meaningful: an operator's geographic bounds, entity-type filter, and alert-type filter are saved, enforced server-side on both REST and WebSocket delivery, and visible/editable through the dashboard.

See [`phase-07-workspace-scope.md`](phase-07-workspace-scope.md) for the original phase plan (goal, backend/frontend scope, required experiments, exit criteria) — the source plan this README tracks progress against, updated in place only for accepted architectural changes (per `CLAUDE.md`), not rewritten casually.

---

## Checkpoint progress

| Checkpoint | Scope | Status |
| --- | --- | --- |
| Pre-CP1 | Design-only: resolve the `POST`/`PUT /users/me/workspace` contract — request/response shapes, demo-role exclusion, `entity_types` restricted to `["aircraft"]` in v1, `404` no-workspace convention, validation rules. Documented in `DATA_MODEL.md` and this checkpoint's concept doc before any code exists. No code. | Done |
| CP1 | Predefined region list + `POST`/`PUT /users/me/workspace` implementing Pre-CP1's resolved contract. No REST/WebSocket filtering enforcement yet. API only. | Pending |
| CP2 | Server-side REST filtering: `GET /alerts` applies the caller's saved scope before returning rows. | Pending |
| CP3 | Server-side WebSocket filtering: `{connection_id -> scope}` map loaded at WS upgrade, applied to `alert-events` fan-out. | Pending |
| CP4 | Scope-update flow: `PUT` updates the saved scope, dashboard reconnects the WebSocket to pick it up; workspace restore on reload/reconnect. | Pending |
| CP5 | Frontend workspace-controls checkpoint (mockup → approval → implementation): scope prompt, region/entity/alert-type controls, disabled for demo role, saved scope visibly restored after reload. | Pending |
| CP6 | End-to-end verification through the browser: two operators with different scopes see different alerts/map data live. | Pending |

Look up any checkpoint's commit with `git log --oneline` from the repository root, once commits exist.

**Two things already existed before this phase started, and are not this phase's work:** Google OAuth + JWT auth (ADR-011) and the `user_workspaces`/`users` tables (migrations 003/004) were built ahead of schedule, alongside the rest of the auth stack. Phase 07 connects code to an already-accepted contract, it does not design auth or the table from scratch.

**API convention adopted at Pre-CP1, applies to every checkpoint above and beyond this phase:** new read endpoints use `POST` with params in the body, not `GET` with a query string, so filters can extend the body later without a breaking URL change. This does not retrofit already-shipped `GET /alerts` or `GET /entities/live`. The WebSocket upgrade handshake is unaffected — it must remain an HTTP `GET` per the WebSocket protocol (RFC 6455), and Sentinel already avoids the underlying problem by loading scope server-side rather than via handshake query params.

---

## Contents

| Path | Description |
| --- | --- |
| [`phase-07-workspace-scope.md`](phase-07-workspace-scope.md) | Original phase plan: goal, backend/frontend scope, required experiments, exit criteria |
| [`concepts/`](concepts/README.md) | Concept notes and checkpoint debriefs, in reading order |

Phase 07 is in progress. Pre-CP1's design is resolved and documented; CP1's implementation has not started.
