# Phase 09 — Entity Investigation

## Goal

Build the operator workflow for understanding an incident across Sentinel's polyglot stores.

This was the largest frontend phase in the roadmap. The checkpoint sequence it was broken into, with a concept note and debrief for each, is tracked in [concepts/README.md](concepts/README.md).

## Status

Complete and merged to `develop` and `main` (PRs #155 and #156). The exit criteria below are met.

- Backend: five checkpoints, one per read endpoint group listed below.
- Frontend: three checkpoints building the entity detail panel's Overview, History and Relationships tabs.
- A later product change replaced the one-panel-per-entity design with a single, always visible entity detail panel that updates in place on each selection.

Not built: the map-overlay position track. The approved mockup names it, but the History tab checkpoint deliberately shipped only the in-panel altitude chart and left the overlay as a separate follow-on. It has not been scheduled into a phase yet.

## Backend

- `GET /entities` — live positions from Redis, workspace-scoped
- `GET /entities/:entity_id` — live state + recent alerts
- `GET /entities/:entity_id/history` — TimescaleDB timeline
- `GET /entities/:entity_id/graph` — Neo4j proximity/associate context
- richer `GET /alerts` filtering and `GET /alerts/:alert_id`

## Frontend

Dedicated investigation-UI checkpoints, listed in [concepts/README.md](concepts/README.md). Followed the mockup → approval → implementation gate, same as every other operator-visible checkpoint in this roadmap.

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
