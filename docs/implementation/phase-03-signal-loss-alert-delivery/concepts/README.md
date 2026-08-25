# Phase 03 Concepts

Concept notes and debrief records for Phase 03 checkpoints, in the order you'd read them while working through the phase.

| Folder | What's inside |
| --- | --- |
| [leader-election/](leader-election/) | Redis leader lease: acquire with SET NX PX, safe renewal with compare-and-PEXPIRE, release with compare-and-DEL, follower polling interval, and what happens when the leader dies mid-scan |
| [signal-loss-detection/](signal-loss-detection/) | Scheduled scan design: how last_seen_ms is compared against the silence threshold, why the scan is idempotent, and how episode gating prevents duplicate alert emission for the same dark period |
| [alert-state-episode/](alert-state-episode/) | alert-state:{entity_id} hash fields (dark_since_ms, signal_loss_alert_id), why the episode anchor is source event time, how the deterministic alert_id is derived, and how the state is cleared on resume |
| [api-scaffold/](api-scaffold/) | Express service scaffold, Google ID-token verification flow, JWT issuance and validation, minimal user persistence in TimescaleDB, and how auth integrates with both REST and WebSocket |
| [alert-sink/](alert-sink/) | Kafka consumer for the alerts topic, idempotent TimescaleDB alert write via ON CONFLICT DO NOTHING on alert_id, GET /alerts endpoint, and the crash boundary between DB write and offset commit |
| [websocket-serving/](websocket-serving/) | Authenticated WebSocket upgrade, position-updates Redis pub/sub subscriber, viewport bbox filtering before forwarding to clients, GET /entities/live seed endpoint from Redis live state, and alert-events fan-out |
| [dashboard/](dashboard/) | Next.js scaffold with Blueprint.js, Google OAuth login, react-leaflet live map with moving markers, WebSocket hooks for position and alert streams, filter panel, and alert list panel |
