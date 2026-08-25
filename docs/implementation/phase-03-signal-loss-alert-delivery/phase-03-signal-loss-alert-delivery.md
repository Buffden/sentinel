# Phase 03 — Signal Loss + Alert Delivery Foundation

## Goal

Build Sentinel's first complete operator-visible anomaly slice.

```text
entity:live:* → scheduled scan → Alert Evaluator → alerts Kafka → API consumer → TimescaleDB alerts → GET /alerts + authenticated WebSocket
```

This phase establishes the serving path that all later alert types reuse.

## What to Build

### Alert Evaluator
- Redis leader lease with safe acquire/renew/release semantics
- scheduled signal-loss scan using `last_seen_ms`
- `alert-state:{entity_id}` episode state
- deterministic `SIGNAL_LOSS` alert emission

### API / Alert Sink Foundation
- Express API scaffold
- Google ID-token verification and JWT issuance/validation
- minimal user persistence needed for authenticated access
- Kafka consumer for `alerts`
- idempotent TimescaleDB alert persistence
- basic `GET /alerts`
- authenticated WebSocket
- delivery of new alerts to clients connected to the current API instance
- `GET /entities/live?bbox=...` for initial map seed from Redis live state
- Redis `position-updates` pub/sub subscriber; forward to connected WebSocket clients

Multi-instance fan-out, workspace scope, acknowledge/resolve, and richer lifecycle semantics are intentionally deferred.

### Dashboard (closes the vertical slice)
Built last, after the API is serving both position updates and alerts.

- Next.js scaffold with Blueprint.js layout
- Google OAuth login page and JWT storage
- react-leaflet map with moving flight markers (course_deg rotation, tooltip)
- WebSocket hook: receives position updates and alert events on the same connection
- Filter panel: airborne toggle, entity subtype, altitude range, callsign search
- Alert list panel: live-updating from WebSocket; new alerts appear without page refresh
- Operator can watch a flight go dark and see the signal loss alert appear on screen

See `concepts/fe-dashboard-live-map/README.md` in Phase 02 for the full viewport culling and filter design.

## Required Failure Experiments

- kill evaluator leader and confirm follower takeover
- repeated scans of one dark entity emit one alert per episode
- crash API after DB write but before offset commit; replay creates no duplicate durable row
- invalid/expired JWT is rejected for REST and WebSocket
- WebSocket client reconnects after drop and receives the next position tick without manual refresh

## Exit Criteria

An operator opens the dashboard, sees live flights on the map, watches a flight go dark, and sees a signal loss alert appear in the alert panel — all without leaving the browser or touching the CLI. Every later detector becomes operator-visible by publishing the canonical `alerts` contract.
