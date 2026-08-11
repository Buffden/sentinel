# Phase 07 — Workspace + Operator Scope

## Goal

Make authentication operationally meaningful by adding saved operator scope and enforcing it server-side.

## What to Build

- `user_workspaces` persistence
- `GET /workspaces` / `PUT /workspaces`
- geographic bounds
- entity-type filters
- alert-type filters
- server-side filtering for REST alert reads
- server-side filtering for live alert and position WebSocket delivery
- workspace restore on reconnect/login

Do not add elaborate RBAC roles in v1 unless an ADR changes scope.

## Required Experiments

- two users with different geographic workspaces receive different alert sets
- out-of-scope REST queries do not leak data
- reconnect restores saved workspace
- changing workspace updates subsequent WebSocket filtering

## Exit Criteria

Authenticated users have durable workspace configuration, REST and WebSocket data are scoped server-side, and visibility rules are consistent across reconnects.
