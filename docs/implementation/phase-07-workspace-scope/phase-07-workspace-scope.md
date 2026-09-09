# Phase 07 — Workspace + Operator Scope

## Goal

Make authentication operationally meaningful by adding saved operator scope and enforcing it server-side — and make that scope something an operator can actually see and change, not just a database row.

## Backend

- `user_workspaces` persistence
- `GET /workspaces` / `PUT /workspaces`
- geographic bounds
- entity-type filters
- alert-type filters
- server-side filtering for REST alert reads
- server-side filtering for live alert and position WebSocket delivery
- workspace restore on reconnect/login

Do not add elaborate RBAC roles in v1 unless an ADR changes scope.

## Frontend

A dedicated workspace-controls checkpoint, after the backend checkpoints above are done and verified — not built in the same pass. Follows `CLAUDE.md`'s existing mockup → developer approval → implementation gate before any UI code.

- minimal controls for selecting/editing geographic bounds, entity-type filters, and alert-type filters
- saved workspace visibly restored after reload/reconnect — not just persisted silently
- keep it functional, not polished; this is not the phase to invest in visual design beyond the existing design tokens

## Vertical-Slice Exit

Operator changes workspace scope → backend persists and enforces it → the map and alert feed visibly reflect only the allowed scope, and continue to after a reload or reconnect. Backend-only completion (persistence + enforcement proven via API/WebSocket inspection, no UI) is a checkpoint milestone inside this phase, not the phase's own exit criterion — see `IMPLEMENTATION_WORKFLOW.md`'s "Vertical phases, horizontally decomposed correctness checkpoints."

## Required Experiments

- two users with different geographic workspaces receive different alert sets
- out-of-scope REST queries do not leak data
- reconnect restores saved workspace
- changing workspace updates subsequent WebSocket filtering
- changing workspace scope in the UI visibly changes what the map/alert feed show, without a page reload

## Exit Criteria

Authenticated users have durable workspace configuration, REST and WebSocket data are scoped server-side, visibility rules are consistent across reconnects, and an operator can view and change their own scope through the dashboard.

Not fully broken down into checkpoints yet — refine the backend/frontend checkpoint sequence when this phase actually starts, per the "don't over-plan future phases" rule.
