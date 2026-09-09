# Phase 09 — Entity Investigation

## Goal

Build the operator workflow for understanding an incident across Sentinel's polyglot stores.

This is expected to be the largest frontend phase in the roadmap. Deliberately kept high-level here — refine into a real checkpoint sequence when this phase actually starts, not now.

## Backend

- `GET /entities` — live positions from Redis, workspace-scoped
- `GET /entities/:entity_id` — live state + recent alerts
- `GET /entities/:entity_id/history` — TimescaleDB timeline
- `GET /entities/:entity_id/graph` — Neo4j proximity/associate context
- richer `GET /alerts` filtering and `GET /alerts/:alert_id`

## Frontend

One or more dedicated investigation-UI checkpoints, detailed later. Follows the mockup → approval → implementation gate, same as every other operator-visible checkpoint in this roadmap.

- entity details panel
- position/state timeline
- relationship evidence (graph pivot)
- alert context — why did this fire, what evidence supports it

Keep UI functional rather than polished.

## Vertical-Slice Exit

Operator clicks an entity or an alert and can investigate the incident — current state, history, relationship evidence, and alert context — through the dashboard, not by querying Redis/TimescaleDB/Neo4j directly. Backend-only completion (the four read endpoints above, proven via direct API calls) is a checkpoint milestone inside this phase, not the phase's own exit criterion.

## Learning Goal

For every query ask: **which datastore best matches this access pattern?**

- current state → Redis
- time-ordered history → TimescaleDB
- relationship traversal → Neo4j
- durable incident state → TimescaleDB alerts

## Exit Criteria

An operator can open an alert, inspect current state and track, pivot into relationship evidence, and do so through the dashboard, server-side workspace scope enforced, without direct datastore access.

Do not fully break this phase into micro-checkpoints yet — refine when Phase 09 starts.
