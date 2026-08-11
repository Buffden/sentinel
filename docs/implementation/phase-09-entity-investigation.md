# Phase 09 — Entity Investigation

## Goal

Build the operator workflow for understanding an incident across Sentinel's polyglot stores.

## What to Build

- `GET /entities` — live positions from Redis, workspace-scoped
- `GET /entities/:entity_id` — live state + recent alerts
- `GET /entities/:entity_id/history` — TimescaleDB timeline
- `GET /entities/:entity_id/graph` — Neo4j proximity/associate context
- richer `GET /alerts` filtering and `GET /alerts/:alert_id`
- dashboard live map, alert feed, and investigation panel

Keep UI functional rather than polished.

## Learning Goal

For every query ask: **which datastore best matches this access pattern?**

- current state → Redis
- time-ordered history → TimescaleDB
- relationship traversal → Neo4j
- durable incident state → TimescaleDB alerts

## Exit Criteria

An operator can open an alert, inspect current state and track, pivot into relationship evidence, and do so through server-side workspace scope without direct datastore access.
