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

Multi-instance fan-out, workspace scope, acknowledge/resolve, and richer lifecycle semantics are intentionally deferred.

## Required Failure Experiments

- kill evaluator leader and confirm follower takeover
- repeated scans of one dark entity emit one alert per episode
- crash API after DB write but before offset commit; replay creates no duplicate durable row
- invalid/expired JWT is rejected for REST and WebSocket

## Exit Criteria

A signal loss is visible end to end from detection through durable persistence and operator delivery, and every later detector can become operator-visible by publishing the canonical `alerts` contract.
