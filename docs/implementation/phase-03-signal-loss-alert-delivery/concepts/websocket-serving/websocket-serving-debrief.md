# WebSocket Serving Debrief

---

## Setup

```bash
# Ensure infra is running
docker compose up -d

# Terminal 1: start the API (from services/api)
cd services/api
node_modules/.bin/tsx --env-file=.env src/index.ts
```

Expected startup output (all four lines must appear before testing):

```
{"level":"info","msg":"API listening","port":3000}
{"level":"info","msg":"ws redis subscriber ready","channels":["position-updates","alert-events"]}
{"level":"info","msg":"alert sink consumer started","brokers":["localhost:9092"],"topic":"alerts","group":"api"}
{"level":"INFO",...,"message":"[ConsumerGroup] Consumer has joined the group","groupId":"api",...}
```

The `ws redis subscriber ready` line confirms the dedicated `redisSub` connection subscribed to both channels. If it is missing, position updates and alert events will never reach WebSocket clients regardless of how many clients are connected.

---

## Experiment 1: WS upgrade auth enforcement

**Unauthenticated upgrade (no cookie):**

```bash
curl -s -o /dev/null -w "HTTP status: %{http_code}\n" \
  -H "Connection: Upgrade" -H "Upgrade: websocket" \
  -H "Sec-WebSocket-Version: 13" -H "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==" \
  http://localhost:3000/ws
# HTTP status: 401
```

**Tampered token:**

```javascript
const { WebSocket } = require('ws');
const ws = new WebSocket('ws://localhost:3000/ws', {
  headers: { Cookie: 'sentinel_jwt=eyJhbGciOiJIUzI1NiJ9.eyJ1c2VyX2lkIjoiaGFja2VyIn0.INVALIDSIGNATURE' }
});
ws.on('unexpected-response', (req, res) => {
  console.log('rejected with HTTP', res.statusCode); // 401
});
```

Both rejected with 401 before the WebSocket handshake completes. The socket is destroyed; no `connection` event fires on the server.

---

## Experiment 2: REST auth enforcement

```bash
# Without cookie: 401
curl -s -o /dev/null -w "%{http_code}\n" http://localhost:3000/alerts

# With valid JWT cookie: 200
curl -s -o /dev/null -w "%{http_code}\n" \
  --cookie "sentinel_jwt=<token>" http://localhost:3000/alerts
```

Observed: 401 / 200 as expected. Same `requireAuth` middleware used by all REST endpoints below the auth boundary in `index.ts`.

---

## Experiment 3: GET /entities/live hydration

Seeded two entities into Redis:

```bash
docker exec sentinel-redis redis-cli HSET entity:live:TEST001 \
  lat 51.5 lon -0.1 altitude_m 10150 speed_mps 220.5 course_deg 270 \
  last_seen_ms 1788039000000 entity_type AIRCRAFT entity_subtype "" callsign BAW123 on_ground false

docker exec sentinel-redis redis-cli HSET entity:live:TEST002 \
  lat 48.8 lon 2.35 altitude_m 8500 speed_mps 190.0 course_deg 90 \
  last_seen_ms 1788039100000 entity_type AIRCRAFT entity_subtype "" callsign AFR456 on_ground false
```

**Missing bbox returns 400:**

```bash
curl -s --cookie "sentinel_jwt=<token>" http://localhost:3000/entities/live
# {"error":"bbox query parameter is required"}
```

**Full globe returns both entities:**

```bash
curl -s --cookie "sentinel_jwt=<token>" \
  "http://localhost:3000/entities/live?bbox=-90,-180,90,180" | jq length
# 2
```

Empty strings from Redis (`entity_subtype: ""`) become `null` in the response. Float fields (`lat`, `lon`, `altitude_m`, etc.) are parsed from Redis string values before returning.

**Bbox filter — London bbox excludes Paris entity:**

```bash
curl -s --cookie "sentinel_jwt=<token>" \
  "http://localhost:3000/entities/live?bbox=50,-2,53,2" | jq '[.[] | {entity_id,lat,lon}]'
# [{"entity_id":"TEST001","lat":51.5,"lon":-0.1}]
# TEST002 (lat 48.8) falls outside bbox and is excluded
```

---

## Experiment 4: position-update delivery via WebSocket

Client connects with valid JWT, sends `subscribe` with full globe bbox. Redis publishes a position update. Message arrives on the client:

```
[WS] connected
[WS] sent subscribe
[REDIS] publishing position-update...  -> 1 (delivered to API subscriber)
[WS] received: {"channel":"position-updates","data":{"entity_id":"TEST001","lat":51.5,"lon":-0.1,...}}
```

Redis `PUBLISH` returned `1` — the API's `redisSub` connection is the single subscriber. The API then fans out to all matching WebSocket clients.

---

## Experiment 5: alert-event delivery via WebSocket

Redis publishes an alert event. Message arrives on all connected clients regardless of bbox:

```
[WS] received: {"channel":"alert-events","data":{"alert_id":"TEST001:SIGNAL_LOSS:1788038700000","alert_type":"SIGNAL_LOSS",...}}
```

Alert events go to every open connection. Position updates are bbox-filtered; alert events are not. An alert about any entity is relevant to all operators regardless of their current viewport.

---

## Experiment 6: bbox filter excludes out-of-viewport entity

Client subscribed to London bbox `[50, -2, 53, 2]`. Redis publishes a Tokyo entity (`lat 35.7, lon 139.7`). No message received:

```
[WS] subscribed to London bbox [50,-2,53,2]
[REDIS] publishing Tokyo position (lat=35.7, lon=139.7) — outside London bbox...  -> 1
[WS] closed — no Tokyo message received (correct)
```

The bbox check is applied per-connection inside `redisSub.on('message', ...)` before `ws.send()`. The API receives every pub/sub message and decides per-client whether to forward it.

---

## Engineering debrief

**Data flow — position update:**
Position Consumer writes an accepted position to `entity:live:{entity_id}` and publishes to `position-updates`. The API's `redisSub` connection receives the message in `redisSub.on('message', ...)`. The handler parses `lat` and `lon`, iterates `connectionBBox`, skips connections with no bbox or whose bbox excludes the entity, and calls `ws.send()` on matching clients.

**Data flow — alert event:**
Alert sink persists an alert to TimescaleDB and publishes to `alert-events`. The same `redisSub` handler receives it and calls `ws.send()` on every open connection with no bbox check.

**Data flow — upgrade:**
Browser sends HTTP upgrade request with `sentinel_jwt` cookie. `server.on('upgrade', ...)` intercepts before the handshake. `verifyToken` reads the cookie from headers and calls `jwt.verify`. If invalid, the raw TCP socket receives `HTTP/1.1 401` and is destroyed. If valid, `wss.handleUpgrade` completes the handshake and emits `connection`.

**Why two Redis connections:**
ioredis connections in subscribe mode cannot issue regular Redis commands (`HGETALL`, `SCAN`, etc.). The regular `redis` connection handles all command operations. `redisSub` is dedicated exclusively to `SUBSCRIBE position-updates alert-events`. Mixing them would cause `ERR Command not allowed in subscribe mode`.

**Main trade-off — fire-and-forget pub/sub:**
Redis pub/sub has no persistence. Messages published while the API is down are permanently lost. Recovery is via the REST hydration sequence (`GET /entities/live` + `GET /alerts`) on reconnect, not via replay. This is the accepted v1 model; Phase 08 adds cross-instance fan-out, but pub/sub replay is still not in scope.

**Reconnect recovery:**
Client WebSocket drops. Client calls `GET /entities/live?bbox=...` to re-seed the map, then `GET /alerts` to re-seed the alert panel, then opens a new WebSocket and sends `subscribe`. Position updates missed during the gap leave markers slightly stale until the next tick for that entity. Alerts missed are recovered from TimescaleDB via `GET /alerts` as long as they are still `NEW` or `ACKNOWLEDGED`.

---

## Key observations

| Concept | Observed |
| --- | --- |
| WS upgrade without cookie rejected with 401 | PASS |
| WS upgrade with tampered token rejected with 401 | PASS |
| REST endpoints return 401 without cookie | PASS |
| REST endpoints return 200 with valid JWT cookie | PASS |
| `GET /entities/live` missing bbox returns 400 | PASS |
| `GET /entities/live` full globe returns both seeded entities | PASS |
| `GET /entities/live` London bbox excludes Paris entity | PASS |
| Empty strings from Redis become `null` in response | PASS |
| Redis `PUBLISH position-updates` delivers to subscribed WS client | PASS |
| Redis `PUBLISH alert-events` delivers to all WS clients | PASS |
| Tokyo entity not delivered to London-subscribed client | PASS |
| `redisSub` logs "ws redis subscriber ready" on startup | PASS |
