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

---

## Dev environment notes

### Redpanda v24 + KafkaJS v2.2.4 compatibility

Two incompatibilities discovered during Phase 03 testing with Redpanda v24.1.2 and KafkaJS v2.2.4:

**1. `rpk topic produce` defaults to Snappy compression; KafkaJS has no Snappy decoder.**

KafkaJS v2.x does not include a Snappy codec. Any batch produced by `rpk topic produce` without an explicit compression flag is Snappy-compressed. When KafkaJS attempts to fetch that batch it throws `KafkaJSNotImplemented: Snappy compression not implemented` and stops the consumer's polling loop silently.

Always pass `--compression none` when producing test messages with rpk:

```bash
echo '{"icao24":"test123",...}' | docker exec -i sentinel-redpanda rpk topic produce adsb.raw --compression none
```

Messages produced by KafkaJS (the ingestion poller and position consumer) are uncompressed by default and are safe to consume.

**2. `FROM_BEGINNING=true` triggers an incompatible seek API in Redpanda v24.1.2.**

When KafkaJS sets `fromBeginning: true` and sends a seek-to-start request to Redpanda v24, the consumer's group join fails with `KafkaJSNotImplemented`. The group state stays `Empty` and no messages are processed. This does not happen when `fromBeginning: false` is used and the consumer starts from the committed offset.

For dev restarts, always start the position consumer with `FROM_BEGINNING=false`:

```bash
FROM_BEGINNING=false node_modules/.bin/tsx src/consumer.ts
```

This is safe for restarts because the consumer group retains committed offsets between runs. Use `rpk group seek` if you need to reset to a specific offset for testing.
