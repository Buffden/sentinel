# Phase 03: Signal Loss + Alert Delivery

Build Sentinel's first complete operator-visible anomaly slice: a dark entity triggers a signal-loss alert that flows from the Alert Evaluator through Kafka to the API, persists in TimescaleDB, and appears live in the browser without a page refresh.

This phase establishes the serving path that all later alert types reuse.

Out of scope: workspace scope, multi-instance fan-out, acknowledge/resolve lifecycle, route deviation, proximity, composite correlation.

---

## Services introduced

| Service | Directory | Role |
| --- | --- | --- |
| Alert Evaluator | `services/alert-evaluator/` | Leader election; signal-loss scan; publish to `alerts` Kafka topic |
| API | `services/api/` | Alert sink; auth; REST endpoints; WebSocket serving |
| Dashboard | `services/dashboard/` | Live map; alert panel; Google OAuth login |

---

## Checkpoint progress

| Checkpoint | Scope | Status |
| --- | --- | --- |
| CP1: Leader election | Redis lease acquire/renew/release; follower standby; AbortController session isolation | Done |
| CP2: Signal-loss detection | `runScan()` — entity filter chain, episode gate, deterministic alert_id, Kafka produce; Position Consumer recovery | Done |
| CP3: Episode lifecycle | Full dark → alerted → resumed → dark again cycle; second alert with new alert_id verified | Done |
| CP4: API scaffold + auth | Express; Google OAuth; Sentinel JWT (HttpOnly cookie); 401 enforcement on REST and WebSocket upgrade | Done |
| CP5: Alert sink | Kafka consumer; `ON CONFLICT (alert_id) DO NOTHING`; WebSocket delivery; offset commit contract | Done |
| CP6: WebSocket serving | Position feed; bbox filtering; reconnect hydration | Not started |
| CP7: Dashboard | Live map; alert panel; login page; demo mode (POST /auth/demo, role claim, IP rate limit, WS expiry) | Not started |

---

## Configuration

Define these as named constants in the Alert Evaluator service. Do not hardcode numbers inline.

| Constant | Default | Description |
| --- | --- | --- |
| `SCAN_INTERVAL_MS` | 30,000 | How often the leader runs the signal-loss scan |
| `SIGNAL_LOSS_THRESHOLD_MS` | 300,000 (5 min) | Silence duration before an entity is declared lost |
| `LEADER_LEASE_TTL_MS` | 15,000 | Redis leader key TTL; must be longer than one scan cycle |
| `LEADER_RENEWAL_INTERVAL_MS` | 5,000 | How often the leader renews the lease |

Threshold is applied uniformly to all entity types in v1. Per-type thresholds are a future extension.

---

## Signal-loss semantics

`last_seen_ms` in `entity:live:{entity_id}` is set from `timestamp_ms` (source event time). It represents the last accepted position update for the entity, not the last transponder contact.

Detection rule: `now_ms - last_seen_ms >= SIGNAL_LOSS_THRESHOLD_MS`.

The Redis hash TTL (24 hours) is not the signal-loss detector. The TTL is a cleanup mechanism for entities that have permanently stopped transmitting. Signal loss is detected by the scan comparing `last_seen_ms` against the threshold.

On-ground entities (`on_ground = true`) are excluded from signal-loss detection in v1. Aircraft on the ground legitimately power down transponders. If `on_ground` is null or empty string in the Redis hash, the entity is included (unknown ground state is treated conservatively).

---

## Signal-loss episode state machine

```
TRACKING ──── silence >= threshold ───→ LOST
LOST     ──── scan detects it ────────→ ALERTED   (alert-state written, Kafka published)
ALERTED  ──── entity resumes ──────────→ RECOVERED (recent-loss written, alert-state deleted)
RECOVERED ─── silence >= threshold ───→ ALERTED   (new episode, new dark_since_ms, new alert_id)
```

Episode identity is anchored to `dark_since_ms` — the `last_seen_ms` value at the moment the entity crossed the threshold. Two distinct dark periods produce two distinct `dark_since_ms` values and therefore two distinct alert IDs.

The `alert-state:{entity_id}` hash is the episode gate. If the key exists, the episode is already alerted and no second alert is emitted for the same dark period. If the key does not exist, the entity is either tracking or recovered and a new episode may begin.

---

## What to build

### Alert Evaluator

**Leader election:**

- Acquire: `SET alert-evaluator:leader {instance_id} NX PX {LEADER_LEASE_TTL_MS}`
- Renew: Lua script — compare current value to `instance_id`, then `PEXPIRE`. Do not renew if not the owner.
- Release: Lua script — compare current value, then `DEL`. Do not delete another instance's key.
- An instance that loses the lease must stop scanning immediately. It does not emit alerts, write Redis episode state, or publish to Kafka until it re-acquires.
- The deterministic alert_id is the final duplicate-safety backstop. A brief window where two instances both believe they are leader may cause two Kafka publishes of the same alert_id; the API's idempotent DB write absorbs the duplicate.

**Signal-loss scan (leader only):**

- Run every `SCAN_INTERVAL_MS`.
- Scan all `entity:live:*` keys. For each entity:
  - Skip if `on_ground = 'true'`.
  - Skip if `last_seen_ms` is missing or empty.
  - Skip if `now_ms - last_seen_ms < SIGNAL_LOSS_THRESHOLD_MS`.
  - Skip if `alert-state:{entity_id}` already exists (episode already alerted).
  - Otherwise: compute `dark_since_ms = last_seen_ms`, write `alert-state:{entity_id}`, publish alert to Kafka.

**`alert-state:{entity_id}` hash fields:**

| Field | Value |
| --- | --- |
| `dark_since_ms` | `last_seen_ms` at detection time |
| `signal_loss_alert_id` | the emitted alert_id |
| `composite_issued` | `0` (written now; Phase 06 sets to `1`) |

No TTL. The Position Consumer deletes this key on resume.

**Signal-loss recovery (Position Consumer responsibility, implemented in Phase 03):**

When the Position Consumer writes an accepted position for an entity that has an `alert-state:{entity_id}` key:

1. Write `recent-loss:{entity_id}` hash with `dark_since_ms`, `resumed_at_ms` (source event time of resume), and `signal_loss_alert_id`.
2. Delete `alert-state:{entity_id}`.

This clears the episode gate so a future silence can produce a new alert. `recent-loss` is mainly consumed in Phase 06 (composite correlation); write it now so the data is present when needed.

---

### Canonical SIGNAL_LOSS alert contract

Deterministic alert_id: `{entity_id}:SIGNAL_LOSS:{dark_since_ms}`

Kafka payload published by the Alert Evaluator:

| Field | Type | Value |
| --- | --- | --- |
| `alert_id` | string | `{entity_id}:SIGNAL_LOSS:{dark_since_ms}` |
| `entity_id` | string | |
| `entity_type` | string | from Redis hash |
| `alert_type` | string | `SIGNAL_LOSS` |
| `priority` | string | `STANDARD` |
| `status` | string | `NEW` |
| `detected_at_ms` | number | processing time of the scan (not source event time) |
| `payload` | object | see below |

`payload` evidence:

```json
{
  "dark_since_ms": 1787634000000,
  "last_known_lat": 51.5,
  "last_known_lon": -0.1,
  "last_known_altitude_m": 10150,
  "last_known_speed_mps": 220.5,
  "last_known_course_deg": 270
}
```

All `last_known_*` fields are read from `entity:live:{entity_id}` at scan time. Null/empty string values from the Redis hash become `null` in the payload.

---

### API / Alert sink

**Auth:**

- Verify Google ID token server-side using the Google auth library (not client-side decode).
- On verification success, look up or create the user row in TimescaleDB by `google_sub`.
- Issue a Sentinel JWT signed with a server secret containing `user_id`, `email`, `exp`.
- Return the JWT in a `Secure; HttpOnly; SameSite=Strict` cookie. Do not expose it in the response body or store it in localStorage. The browser sends the cookie automatically on subsequent requests and on WebSocket upgrade (same-origin).
- REST endpoints: validate JWT from cookie on every request; return 401 on invalid or expired.
- WebSocket upgrade: validate JWT from cookie before completing the handshake; reject with 401 if invalid.

**Alert sink (Kafka consumer):**

Processing order per message:

1. Parse and validate the alert payload.
2. Idempotently persist to TimescaleDB `alerts` table using `INSERT ... ON CONFLICT (alert_id) DO NOTHING`. `alerts` is a plain PostgreSQL table, not a hypertable.
3. Publish the alert lifecycle event to Redis `alert-events`. WebSocket clients receive it via the pub/sub subscription established on connect.
4. Commit the Kafka offset.

A crash at any point before step 4 causes Kafka to redeliver. The DB insert is idempotent. The Redis publish and WebSocket delivery may happen twice — clients deduplicate by `alert_id`.

**`GET /alerts`:**

Returns all `NEW` and `ACKNOWLEDGED` alerts from TimescaleDB, ordered by `detected_at DESC`. No pagination in v1. Used for initial dashboard hydration and post-reconnect recovery.

**`GET /entities/live?bbox={minLat},{minLon},{maxLat},{maxLon}`:**

- bbox parameter order: `minLat, minLon, maxLat, maxLon` (south, west, north, east).
- Scan all `entity:live:*` Redis keys. For each entity, parse `lat` and `lon`. Exclude the entity if either is missing or empty string.
- Include only entities where `minLat <= lat <= maxLat` AND `minLon <= lon <= maxLon`.
- Exclude entities where `last_seen_ms` is older than `now_ms - (2 * SIGNAL_LOSS_THRESHOLD_MS)`.
- Cap response at 500 entities.
- Convert Redis hash empty strings to `null` in the response. Parse floats from Redis string values before returning.
- Response is an array of entity snapshots matching the WebSocket position message shape plus `on_ground` and `entity_subtype`.

**WebSocket position feed:**

- On connect: subscribe to Redis `position-updates` pub/sub.
- Forward position update messages to the client after bbox filtering using the viewport the client declared via `subscribe` message.
- On `subscribe` message from client: update the stored bbox for that connection.

**WebSocket alert feed:**

- On connect: subscribe to Redis `alert-events` pub/sub (inter-instance fan-out deferred to Phase 08; for now deliver only alerts consumed on this instance).
- Forward alert events to connected clients.

---

### Dashboard

Built last, after the API is serving both position updates and alerts.

**Initial hydration (on page load and on reconnect):**

1. `GET /entities/live?bbox=...` — seed map with current live flights in viewport.
2. `GET /alerts` — seed alert panel with current open alerts.
3. Open authenticated WebSocket and send `subscribe` with current bbox.

**Live updates (via WebSocket):**

- Position updates: merge into entity map by `entity_id`; use highest `timestamp_ms` received; never move a marker backward to an older position.
- Alert events: merge into alert list by `alert_id`; duplicate `alert_id` messages are safe to receive and must not create duplicate entries.

**Reconnect reconciliation:**

On WebSocket disconnect and reconnect, re-run the full hydration sequence (both GET calls + subscribe). This recovers any alerts or position updates missed during the disconnected window.

**Components:**

- Google OAuth login page; JWT stored in HttpOnly cookie by server.
- react-leaflet map with moving flight markers: rotated arrow SVG using `course_deg`, tooltip with `callsign`, altitude, speed, course, last seen age.
- Filter panel: airborne/ground toggle, entity subtype checkboxes, altitude range slider, callsign soft-search (dims non-matching markers).
- Alert panel: live list of open alerts; new entries appear without page refresh; deduplicated by `alert_id`.

---

## Important learning

- Leader election with a Redis TTL lease: acquire, renew, release with Lua atomicity
- Fail-closed behavior: stop scanning on any lease uncertainty
- Episode identity: `dark_since_ms` as the natural anchor for deterministic alert IDs
- Gate-first ordering: write Redis episode gate before Kafka produce
- Signal-loss recovery: Position Consumer clears the gate on accepted resume
- `recent-loss` written now for Phase 06 composite correlation
- JWT auth: Google ID token verification vs Sentinel JWT issuance
- HttpOnly cookie: why not localStorage
- Idempotent Kafka sink: `ON CONFLICT (alert_id) DO NOTHING`; replay safety
- WebSocket auth: cookie on upgrade request; same JWT middleware

---

## Bootstrap sequence

```bash
make up          # start all containers
make topics      # provision canonical Kafka topics
bash infra/scripts/migrate.sh

# Alert Evaluator
cd services/alert-evaluator
node_modules/.bin/tsx src/evaluator.ts

# Position Consumer
cd services/position-consumer
FROM_BEGINNING=false node_modules/.bin/tsx src/consumer.ts
```

---

## Required failure experiments

- Kill the leader evaluator; follower acquires the lease within one TTL window and scanning resumes.
- Repeated scans of a single dark entity emit exactly one alert per episode; `alert-state` key acts as the gate.
- Crash API after TimescaleDB write but before Kafka offset commit; replay produces `INSERT 0 0`; no duplicate durable row.
- Invalid or expired JWT rejected on REST endpoint with 401.
- Invalid or expired JWT rejected on WebSocket upgrade; connection not established.
- WebSocket client disconnects and reconnects; re-runs hydration; receives next position tick and any alerts missed during the gap.
- Entity goes dark, SIGNAL_LOSS alert emitted. Entity resumes. Entity goes dark again. A second alert with a new `dark_since_ms` and new `alert_id` is emitted.
- Same alert event delivered twice to the WebSocket client (simulated by sending the alert-events pub/sub message twice); dashboard shows exactly one logical alert entry for that `alert_id`.

---

## Exit criteria

| Criterion | |
| --- | --- |
| Alert Evaluator detects signal loss and emits a deterministic SIGNAL_LOSS alert on Kafka | |
| Leader election prevents two concurrent active scanners | |
| Alert persisted idempotently in TimescaleDB on `alert_id`; replay produces no duplicate row | |
| Signal-loss recovery clears episode state; second dark period produces second alert | |
| `GET /alerts` returns open alerts to authenticated client | |
| `GET /entities/live?bbox=...` returns live entities in viewport from Redis | |
| Authenticated WebSocket delivers new alerts without page refresh | |
| WebSocket reconnect re-hydrates from REST; no missed alerts lost permanently | |
| Dashboard: live flights visible on map with moving markers and tooltips | |
| Dashboard: operator watches flight go dark, signal loss alert appears in alert panel | |
| All checks above verifiable via CLI tools, browser DevTools, and psql; no code reading required | |

**Phase 03 exit: INCOMPLETE**

---

## Contents

| Path | Description |
| --- | --- |
| [`concepts/`](concepts/README.md) | Concept notes and checkpoint debriefs, in reading order |
| [`exit-verification.md`](exit-verification.md) | Final store inspection and exit criteria evaluation |
