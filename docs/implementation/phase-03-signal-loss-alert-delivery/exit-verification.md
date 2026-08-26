# Phase 03 Exit Verification

Final inspection of the signal loss + alert delivery pipeline and dashboard before Phase 04 begins. Records the state of every service, Kafka topic, database table, Redis key, and browser-visible output introduced or written by Phase 03.

Verified: —

---

## 1. Container health

| Container | Image | Status |
| --- | --- | --- |
| sentinel-redpanda | | |
| sentinel-timescaledb | | |
| sentinel-redis | | |
| sentinel-neo4j | | |

---

## 2. Services

| Service | Introduced in | Status |
| --- | --- | --- |
| alert-evaluator | Leader election + signal-loss scan | |
| api | Express scaffold + alert sink + WebSocket | |
| dashboard | Next.js live map + alert panel | |

---

## 3. Kafka topics written by Phase 03

| Topic | Producer | Records observed |
| --- | --- | --- |
| `alerts` | Alert Evaluator | |

---

## 4. TimescaleDB: alerts

| Check | Result |
| --- | --- |
| Row count after first dark entity detected | |
| Duplicate alert_id insert result (ON CONFLICT DO NOTHING) | |
| Distinct alert_id values equal total row count | |

---

## 5. Redis state

| Key pattern | Check | Result |
| --- | --- | --- |
| `alert-evaluator:leader` | Leader key exists with correct instance_id | |
| `alert-evaluator:leader` | TTL is refreshed on renewal; not permanent | |
| `alert-state:{entity_id}` | dark_since_ms set when entity goes dark | |
| `alert-state:{entity_id}` | Cleared on entity resume | |

---

## 6. Alert Evaluator: leader election

| Check | Result |
| --- | --- |
| Single evaluator acquires lease on start | |
| Second evaluator starts as follower | |
| Leader killed; follower acquires lease within one TTL window | |
| Repeated scans of dark entity emit exactly one alert per episode | |

---

## 7. API

| Check | Result |
| --- | --- |
| GET /alerts returns persisted alerts | |
| Authenticated WebSocket connection accepted with valid JWT | |
| Invalid/expired JWT rejected on REST | |
| Invalid/expired JWT rejected on WebSocket upgrade | |
| New alert appears over WebSocket without page refresh | |

---

## 8. Position feed

| Check | Result |
| --- | --- |
| GET /entities/live returns entities within bbox from Redis | |
| WebSocket position update received after entity moves | |
| Stale position update (older timestamp_ms) does not move marker backward | |
| WebSocket reconnect receives next tick without manual refresh | |

---

## 9. Dashboard

| Check | Result |
| --- | --- |
| Login page renders; Google OAuth flow completes | |
| Map loads with live flights after seed call | |
| Flight markers move on position update | |
| Marker tooltip shows entity_id, callsign, altitude, speed, course, last seen | |
| Airborne/ground toggle filters markers | |
| Callsign search dims non-matching markers | |
| Alert panel populates from GET /alerts on load | |
| New signal loss alert appears in alert panel over WebSocket | |
| Operator watches flight go dark and sees alert without refreshing | |

---

## 10. Failure experiments

| Experiment | Expected result | Observed result |
| --- | --- | --- |
| Kill leader evaluator; follower takes over | Follower acquires lease within one TTL; scan resumes | |
| Repeated scans of same dark entity | One alert row; subsequent scans produce INSERT 0 0 | |
| Crash API after DB write, before offset commit | Replay on restart; idempotent insert via ON CONFLICT DO NOTHING | |
| Invalid JWT on REST endpoint | 401 Unauthorized | |
| Invalid JWT on WebSocket upgrade | Connection rejected | |
| WebSocket client drops and reconnects | Next position tick received; no manual refresh needed | |

---

## 11. Exit criteria

| Criterion | Result |
| --- | --- |
| Alert Evaluator detects signal loss and emits deterministic SIGNAL_LOSS alert | |
| Leader election prevents concurrent active evaluators | |
| Alert persisted idempotently in TimescaleDB | |
| GET /alerts returns persisted alerts to authenticated client | |
| Authenticated WebSocket delivers new alert without page refresh | |
| Live flights visible on map with moving markers and tooltips | |
| Operator watches flight go dark and sees alert appear in browser | |
| Every later detector becomes operator-visible by publishing to the alerts Kafka topic | |

**Phase 03 exit: INCOMPLETE**
