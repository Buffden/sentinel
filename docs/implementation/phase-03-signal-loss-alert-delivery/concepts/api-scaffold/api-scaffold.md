# API Scaffold + Google OAuth + JWT — Design and Learning Reference

CP4 brings up the API service: Express server, Google OAuth login, Sentinel JWT issuance, and 401 enforcement on all REST and WebSocket entry points. The alert sink, REST endpoints, and WebSocket feed are built in CP5 and CP6.

---

## What this checkpoint adds

The alert pipeline is complete up to Kafka. CP4 brings up the API service boundary:

1. Express server with Google OAuth login and JWT session management.
2. 401 enforcement on all REST endpoints and WebSocket upgrade.
3. Demo mode: `POST /auth/demo`, IP rate limiting, `role: 'demo'` JWT claim, WS expiry enforcement.

---

## Concepts in plain language

### 1. Why the API owns auth

No browser talks directly to Kafka, Redis, or TimescaleDB. The API is the single boundary between operators and backend state. It issues and validates all session tokens. Nothing else does.

### 2. Google OAuth ID token vs Sentinel JWT — two different tokens

These are often confused. They are different things:

**Google ID token** — issued by Google after the user completes the OAuth consent flow. It is a JWT signed by Google's private keys. It proves the user authenticated with Google. It is short-lived and tied to Google's infrastructure. The API verifies it using Google's public keys via `google-auth-library`. After verification, it is discarded.

**Sentinel JWT** — issued by the API after verifying the Google token. Signed with the API's own secret (`JWT_SECRET` env var). Contains `user_id`, `email`, `exp`. This is what the browser sends on every subsequent request. The API verifies it using its own secret — no network call required.

The separation matters: Google's token proves "this person is who they say they are." The Sentinel JWT proves "this person has an active session in our system."

### 3. Why HttpOnly cookie, not localStorage

`HttpOnly` cookies cannot be read by JavaScript. XSS attacks that inject scripts into your page cannot steal the token. `localStorage` is readable by any script on the page.

`Secure` — only sent over HTTPS. `SameSite=Strict` — not sent on cross-site requests, preventing CSRF.

The browser sends the cookie automatically on every same-origin request, including WebSocket upgrades. No client-side token management needed.

### 4. JWT validation — what to check

A valid JWT is not just parseable — it must:

- Have a valid signature (signed with the correct `JWT_SECRET`)
- Not be expired (`exp` claim > current time)
- Contain the expected claims (`user_id`, `email`)

A tampered payload with a valid-looking structure but wrong signature must be rejected. A legitimate token that has expired must be rejected. Never skip expiry checks.

### 5. Alert sink — why Kafka → TimescaleDB

The Alert Evaluator publishes to Kafka. The API consumes from Kafka and writes to TimescaleDB. This decoupling means:

- The evaluator does not need to know the API exists.
- The API can restart and replay from the last committed offset — recovering any alerts missed while it was down.
- `ON CONFLICT (alert_id) DO NOTHING` makes the insert idempotent: replaying the same Kafka message never creates a duplicate row.

The commit order is: DB write → Redis `alert-events` publish → Kafka offset commit. A crash at any point before the offset commit causes Kafka to redeliver — the idempotent insert absorbs the duplicate DB write, and the Redis publish and WebSocket delivery may happen twice but clients deduplicate by `alert_id`.

### 6. WebSocket auth — cookie on upgrade

WebSocket upgrades are HTTP requests. The browser sends cookies on the upgrade request automatically (same-origin). The API validates the JWT from the cookie before completing the handshake. If the token is missing or invalid, the upgrade is rejected with 401 and no WebSocket connection is established.

This is the same JWT check as REST endpoints — no separate auth mechanism.

---

## Auth data flow

The browser posts the Google ID token to `POST /auth/google`. The API verifies it with Google, upserts the user record, signs a Sentinel JWT, and sets it as an HttpOnly cookie.

Every subsequent request sends that cookie automatically. The API verifies the JWT signature and expiry with its own secret, extracts `user_id`, and queries TimescaleDB.

WebSocket upgrades follow the same path: the browser sends the cookie on the upgrade request, the API validates the JWT before completing the handshake, and rejects with 401 if the token is missing or invalid.

---

## Alert sink data flow

The API consumer (group `api`) reads each message, parses it, inserts into TimescaleDB with `ON CONFLICT (alert_id) DO NOTHING`, publishes the alert lifecycle event to Redis `alert-events`, then commits the offset. WebSocket clients receive the event via the `alert-events` pub/sub subscription established on connect.

Crash between DB write and offset commit: Kafka redelivers. The second `INSERT` hits `ON CONFLICT DO NOTHING` — no duplicate row. The Redis publish and WebSocket delivery may happen twice — clients deduplicate by `alert_id`.

---

## Failure modes

| Scenario | Result |
|---|---|
| Expired JWT on REST request | 401; client must re-auth |
| Tampered JWT (bad signature) | 401; `jsonwebtoken` verify throws |
| Missing cookie | 401 |
| Expired JWT on WS upgrade | Upgrade rejected; no connection |
| API crashes after DB write, before offset commit | Kafka redelivers; `ON CONFLICT DO NOTHING`; no duplicate row |
| Google token verification fails | 401; user not logged in |

---

## Environment variables

| Variable | Purpose |
|---|---|
| `JWT_SECRET` | Signs and verifies Sentinel JWTs. Never commit. |
| `GOOGLE_CLIENT_ID` | Passed to `google-auth-library` for ID token verification. |
| `PG_URL` | TimescaleDB connection string. |
| `REDIS_URL` | Redis connection string. |
| `KAFKA_BROKERS` | Broker list for the alert sink consumer. |

---

## Manual inspection

1. Complete Google OAuth in the browser. In DevTools, go to Application → Cookies and confirm `sentinel_jwt` is marked HttpOnly, Secure, and SameSite: Strict.
2. Decode the JWT payload by copying the token from DevTools and splitting on `.` to base64-decode the middle segment. Confirm it contains `user_id`, `email`, `iat`, and `exp`, and that `exp` is in the future.
3. Hit a protected endpoint without a cookie and confirm a 401 response.
4. Hit with a garbage token value and confirm a 401 response.
5. After login, hit `GET /alerts` with the real cookie and confirm a 200 with the alert array.
6. Verify idempotent sink: query `SELECT COUNT(*), COUNT(DISTINCT alert_id) FROM alerts;` — both counts must be equal.
7. Replay test: seek the `api` consumer group to offset 0 and restart the API. Row count must not increase.

---

## Map mental model to code

| Concept | Where in code |
|---|---|
| Google token verification | `google-auth-library` `OAuth2Client.verifyIdToken()` — `auth.ts` |
| User upsert | `INSERT INTO users ... ON CONFLICT (google_sub) DO UPDATE` — `auth.ts` |
| Sentinel JWT sign | `jsonwebtoken.sign({ user_id, email }, JWT_SECRET, { expiresIn })` — `auth.ts` |
| HttpOnly cookie | `res.cookie('sentinel_jwt', token, { httpOnly: true, secure: true, sameSite: 'strict' })` |
| JWT middleware | `jsonwebtoken.verify(token, JWT_SECRET)` — `middleware/auth.ts` |
| Alert sink consumer | Kafka consumer group `api`; `eachMessage` → INSERT → Redis `alert-events` publish → commit |
| Idempotent insert | `INSERT INTO alerts ... ON CONFLICT (alert_id) DO NOTHING` |
| WS upgrade gate | Verify JWT from `req.cookies.sentinel_jwt` before `wss.handleUpgrade` |

---

## Retention questions

1. The browser sends the same `sentinel_jwt` cookie on every request automatically. Why does this not require any JavaScript token management?
2. The API crashes after writing an alert to TimescaleDB but before committing the Kafka offset. Walk through exactly what happens on restart.
3. Why does the API verify the Google ID token server-side using `google-auth-library` rather than just decoding the JWT payload in the browser?
4. A WebSocket client connects, receives 5 alerts, then disconnects and reconnects. How does it recover the alerts it may have missed?
5. Two Kafka messages with the same `alert_id` arrive at the alert sink (duplicate delivery). What happens at each step: parse, DB insert, WebSocket publish, offset commit?

---

## Completion checklist

- [ ] I can explain the difference between the Google ID token and the Sentinel JWT
- [ ] I can explain why the JWT is stored in an HttpOnly cookie rather than localStorage
- [ ] I can trace the full auth flow from browser OAuth redirect to protected API request
- [ ] I can explain the alert sink write order and what happens on crash between DB write and offset commit
- [ ] I can run `curl` commands to verify 401 on missing/invalid token
- [ ] I can verify idempotent alert persistence by replaying Kafka offsets
- [ ] I can inspect the JWT payload and confirm `exp` is in the future
