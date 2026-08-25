# position-updates Pub/Sub

---

## What it is

`position-updates` is a Redis pub/sub channel. The Position Consumer publishes a lightweight position snapshot to it after every accepted write. API instances subscribe and fan the update out to connected WebSocket clients for live map rendering.

---

## Fire-and-forget semantics

Redis pub/sub is ephemeral. There is no message persistence, no consumer group, no offset, and no replay. If no subscriber is connected when the message is published, the message is dropped. If a subscriber disconnects and reconnects, it misses every message published during the gap.

This is intentional. A live map position update is only useful now. A client that missed a position tick will receive the next one in a few seconds and its view of the entity will catch up. There is no value in storing or replaying old map ticks — the durable position record is already in `position_history`.

---

## Why failure must not block offset commit

A Redis pub/sub failure (connection reset, Redis restart) is caught and logged at error level, but the error is not rethrown. The offset is committed regardless.

At the point `publishPositionUpdate` runs, the position is already written to `position_history` and Redis live state. Blocking the offset commit over a WebSocket convenience channel would cause Kafka to redeliver the message, which would replay all prior idempotent writes and then attempt the pub/sub publish again — but if Redis is down, it will fail again. The result is an indefinitely stalled consumer that blocks all processing for every entity on the partition, over a transient WebSocket map update.

The correct failure mode is: log the error, commit the offset, and let the client's next position update restore the map view.

---

## Payload shape

A subset of `position.normalized` — only what the API needs to push a map update to a WebSocket client.

```json
{
  "entity_id": "def456",
  "entity_type": "aircraft",
  "timestamp_ms": 1787634583000,
  "lat": 51.5,
  "lon": -0.1,
  "altitude_m": 10150,
  "speed_mps": 220.5,
  "course_deg": 270,
  "callsign": "BA100",
  "live_geo_cell": "87194ad33ffffff"
}
```

`live_geo_cell` is included so the API can scope updates to clients whose workspace intersects that cell, without having to recompute the cell from lat/lon.

Full canonical fields (provider, squawk, H3 history cell, etc.) are in `position.normalized` for services that need them. The pub/sub payload is intentionally minimal.

---

## Difference from position.normalized

| | `position.normalized` | `position-updates` |
| --- | --- | --- |
| Transport | Kafka | Redis pub/sub |
| Persistence | Yes (Kafka retention) | No |
| Consumers | Deviation Detector, Correlation Worker | API WebSocket layer |
| Payload | Full canonical schema + both H3 cells | Lightweight map fields + live_geo_cell |
| Failure | Blocks offset commit | Logged, does not block |
| Ordering | Per-entity partition affinity | No ordering guarantee |
| Replay | Yes, via consumer group offset reset | Not possible |

---

## At-least-once from the client's perspective

The DATA_MODEL specifies WebSocket lifecycle delivery as at-least-once. Clients must tolerate duplicate position updates (e.g., if the API reconnects to Redis pub/sub and replays from a different starting point, or if multiple API instances publish to the same client). This is not a correctness concern for a live map — the client renders the most recent position it receives.
