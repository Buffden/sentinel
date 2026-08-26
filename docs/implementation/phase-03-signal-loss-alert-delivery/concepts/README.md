# Phase 03 Concepts

Concept notes and debrief records for Phase 03 checkpoints, in the order you'd read them while working through the phase.

| Folder | Observable result |
| --- | --- |
| [leader-election/](leader-election/) | Redis `alert-evaluator:leader` key exists with correct instance_id; follower waits; kill leader, follower acquires lease within one TTL |
| [signal-loss-detection/](signal-loss-detection/) | Dark entity triggers scan; `alert-state:{entity_id}` written; alert appears on `alerts` Kafka topic with deterministic alert_id |
| [alert-state-episode/](alert-state-episode/) | Repeated scans of same dark entity emit one alert only; entity resumes, `recent-loss` written and `alert-state` deleted; entity goes dark again, second alert with new alert_id emitted |
| [api-scaffold/](api-scaffold/) | Express up; Google OAuth flow completes; Sentinel JWT issued in HttpOnly cookie; 401 on invalid or expired token for REST and WebSocket |
| [alert-sink/](alert-sink/) | Kafka alert consumed; TimescaleDB row written via `ON CONFLICT (alert_id) DO NOTHING`; `GET /alerts` returns it; replay produces no duplicate row |
| [websocket-serving/](websocket-serving/) | Authenticated WebSocket connects; position updates stream to client; `GET /entities/live?bbox=` returns live entities from Redis within viewport |
| [dashboard/](dashboard/) | Browser: live flights on map, flight goes dark, signal loss alert appears in alert panel without page refresh; reconnect re-seeds map and alert list |
