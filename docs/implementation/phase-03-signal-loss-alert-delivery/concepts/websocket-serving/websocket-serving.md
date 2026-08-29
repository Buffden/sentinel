# WebSocket Serving — Design and Learning Reference

CP6 adds the live-update layer to the API: an authenticated WebSocket server that fans out position updates and alert events to connected browser clients, and a `GET /entities/live` endpoint for initial hydration.

---

## What this checkpoint adds

The alert sink is complete. CP6 closes the real-time delivery path to the browser:

1. `ws` WebSocket server attached to the Express HTTP server.
2. JWT validation on upgrade — same cookie check as REST endpoints.
3. Redis subscriber for `position-updates` and `alert-events` pub/sub channels.
4. Per-connection bbox state; `subscribe` message from client updates it.
5. Position messages forwarded only if the entity falls within the client's bbox.
6. Alert events forwarded to all connected clients.
7. `GET /entities/live?bbox=` endpoint for dashboard hydration.

---

## Concepts in plain language

### 1. What a WebSocket is

A WebSocket is a persistent, full-duplex TCP connection established via an HTTP upgrade handshake. The browser sends a normal HTTP request with an `Upgrade: websocket` header. If the server accepts, both sides switch to the WebSocket protocol and can push frames to each other at any time — no polling, no request-response cycle.

This is why live position updates and alerts appear in the dashboard without a page refresh.

### 2. Why auth happens at upgrade time

The browser sends cookies on the upgrade request automatically (same-origin). The API reads the `sentinel_jwt` cookie from the upgrade request headers and validates it before completing the handshake. If the token is missing or invalid, the server responds with 401 and the connection is never established.

This is the same JWT check as REST endpoints — no separate auth mechanism. A valid token at upgrade time does not guarantee the connection stays valid forever. CP7 (demo mode) adds mid-connection expiry enforcement for demo JWTs.

### 3. Redis pub/sub — fire and forget

Redis pub/sub is not a message queue. When the API publishes to `position-updates` or `alert-events`, Redis delivers the message to all current subscribers and discards it. There is no persistence, no offset, no replay.

This means:
- If no client is subscribed, the message is lost.
- If the API crashes and restarts, messages published during the gap are gone.
- Clients recover missed messages via `GET /entities/live` and `GET /alerts` on reconnect — not via pub/sub replay.

### 4. Two separate Redis subscriber connections

ioredis requires a dedicated connection for pub/sub — a connection in subscribe mode cannot issue regular commands. The API therefore maintains two Redis connections:

- `redis` — the regular command connection used for all non-pub/sub operations.
- `redisSub` — the subscriber connection used exclusively for `SUBSCRIBE position-updates alert-events`.

### 5. Per-connection bbox filtering

Each WebSocket connection stores a bounding box: `{ minLat, minLon, maxLat, maxLon }`. When the client sends a `{ type: 'subscribe', bbox: [...] }` message, the server updates that connection's stored bbox. Position updates are only forwarded to a client if the entity's `lat` and `lon` fall within that client's current bbox. Alert events are forwarded to all clients regardless of bbox.

This prevents a client viewport covering London from receiving position updates for entities over Tokyo.

### 6. GET /entities/live — hydration, not live delivery

On page load and on every WebSocket reconnect, the dashboard calls `GET /entities/live?bbox=minLat,minLon,maxLat,maxLon` before opening the WebSocket. This seeds the map with all currently live entities in the viewport.

The endpoint scans all `entity:live:*` Redis hashes, filters by bbox and staleness (`last_seen_ms` within `2 × SIGNAL_LOSS_THRESHOLD_MS`), and returns up to 500 entities. Empty string values from Redis become `null` in the response. Float fields are parsed from their Redis string representation.

This is a REST endpoint — it returns a snapshot. Live updates after that come via the WebSocket.

### 7. Reconnect recovery

If the WebSocket drops (network blip, server restart), the client:

1. Calls `GET /entities/live?bbox=...` — re-seeds the map.
2. Calls `GET /alerts` — re-seeds the alert panel.
3. Opens a new WebSocket and sends `subscribe` with the current bbox.

Any position updates or alert events missed during the disconnected window are recovered from TimescaleDB and Redis live state via these two REST calls. Nothing is permanently lost as long as the entity is still live in Redis and the alert is still open in TimescaleDB.

---

## Data flow

See [`websocket-serving-flow.puml`](websocket-serving-flow.puml) for the full sequence diagram.

**Upgrade and subscribe:**
The browser sends an HTTP upgrade request with the `sentinel_jwt` cookie. The API validates the JWT, completes the handshake, and the connection enters the connected state. The client sends a `subscribe` message with its current bbox. The server stores the bbox for that connection.

**Position update delivery:**
The Position Consumer publishes to Redis `position-updates`. The API's subscriber connection receives it, parses the entity lat/lon, and forwards the message to every connected client whose stored bbox contains that position.

**Alert event delivery:**
CP5 publishes to Redis `alert-events` after each DB write. The API's subscriber connection receives it and forwards to all connected clients. Clients deduplicate by `alert_id`.

**Hydration:**
`GET /entities/live?bbox=` scans `entity:live:*` Redis hashes, applies bbox and staleness filters, and returns up to 500 entity snapshots.

---

## Guarantee

| Property | Mechanism |
| --- | --- |
| Auth on upgrade | JWT validated from cookie before handshake completes; 401 if invalid |
| Position delivery | At-least-once via pub/sub; clients use highest `timestamp_ms` seen per entity |
| Alert delivery | At-least-once via pub/sub; clients deduplicate by `alert_id` |
| Missed updates on reconnect | Recovered via `GET /entities/live` + `GET /alerts` hydration |

---

## Failure modes

| Scenario | Result |
| --- | --- |
| Missing or expired JWT on upgrade | 401; connection not established |
| Redis subscriber disconnects | ioredis auto-reconnects and re-subscribes; messages during gap are lost; clients recover on reconnect |
| API crashes mid-session | Client WebSocket closes; client re-runs hydration sequence on reconnect |
| Entity outside client bbox | Position update not forwarded; entity appears on map after client pans to it via hydration |
| Alert event delivered twice | Client receives duplicate; deduplicates by `alert_id`; no duplicate UI entry |
| `GET /entities/live` with no bbox | 400; bbox is required |
| Redis scan returns more than 500 entities | Response capped at 500; remaining entities appear as client pans |

---

## Environment variables

| Variable | Purpose |
| --- | --- |
| `REDIS_URL` | Used for both the command connection and the subscriber connection |

---

## Manual inspection

1. Connect with `wscat` and verify 401 on missing token:
   ```bash
   npx wscat -c ws://localhost:3000/ws
   # Expected: connection refused with 401
   ```

2. Connect with a valid token and verify the handshake completes:
   ```bash
   npx wscat -c ws://localhost:3000/ws --header "Cookie: sentinel_jwt=<token>"
   # Expected: connected
   ```

3. Send a `subscribe` message and confirm the server stores the bbox:
   ```bash
   # In wscat session:
   {"type":"subscribe","bbox":[-90,-180,90,180]}
   ```

4. Produce a position update to Redis and confirm it arrives in wscat:
   ```bash
   redis-cli PUBLISH position-updates '{"entity_id":"test123","lat":51.5,"lon":-0.1,"timestamp_ms":1787634060000}'
   ```

5. Produce an alert event and confirm it arrives in wscat:
   ```bash
   redis-cli PUBLISH alert-events '{"alert_id":"test123:SIGNAL_LOSS:0","alert_type":"SIGNAL_LOSS"}'
   ```

6. Verify `GET /entities/live`:
   ```bash
   curl -s --cookie "sentinel_jwt=<token>" \
     "http://localhost:3000/entities/live?bbox=-90,-180,90,180" | jq length
   ```

---

## Map mental model to code

| Concept | Where in code |
| --- | --- |
| WebSocket server | `ws` package `WebSocketServer`, attached to `server.on('upgrade', ...)` |
| JWT validation on upgrade | `requireAuth` logic applied to upgrade request headers |
| Redis subscriber | `redisSub` connection, `redisSub.subscribe('position-updates', 'alert-events')` |
| Per-connection bbox | `Map<WebSocket, BBox>` keyed by connection |
| `subscribe` message handler | Parse client message, update bbox in map |
| Position bbox filter | Check entity lat/lon against stored bbox before `ws.send()` |
| Alert broadcast | Iterate all open connections, `ws.send()` to each |
| `GET /entities/live` | Redis `SCAN entity:live:*`, parse hashes, bbox + staleness filter |

---

## Retention questions

1. Why does the API need two separate Redis connections for CP6?
2. A position update arrives for an entity at lat 51.5, lon -0.1. Client A has bbox covering London. Client B has bbox covering Tokyo. What happens?
3. The WebSocket drops mid-session. Walk through exactly what the client must do to recover, and which data source each recovery step uses.
4. The Redis `position-updates` channel receives 1000 messages per second. Every connected client receives every message. What does the bbox filter prevent?
5. Why is `GET /entities/live` a REST endpoint rather than a WebSocket message?

---

## Completion checklist

- [ ] I can explain why JWT validation happens at upgrade time and not after
- [ ] I can explain why two Redis connections are needed
- [ ] I can trace a position update from Redis pub/sub to a specific client's WebSocket frame
- [ ] I can explain the reconnect recovery sequence and which REST endpoint covers which gap
- [ ] I can verify auth enforcement using wscat with and without a valid token
- [ ] I can explain why alert events go to all clients but position updates are bbox-filtered
