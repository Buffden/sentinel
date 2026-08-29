# Demo Mode Design

**CP4 (API):** token issuance, `role: 'demo'` claim, IP rate limiting, `requireOperator` middleware, WebSocket expiry enforcement.
**CP7 (Dashboard):** "Try Demo" button, expiry banner, redirect to sign-in after WS close `4401`.

---

Unauthenticated visitors get a 15-minute live session without a Google account. Implemented as a short-lived Sentinel JWT with a `role: 'demo'` claim — reuses all existing auth infrastructure.

---

## What demo users see

- Live map with moving flight markers (WebSocket position feed)
- Alert panel streaming signal-loss alerts
- `GET /entities/live?bbox=` and `GET /alerts`

## What demo users cannot access

- Entity investigation endpoints (Phase 09)
- Alert acknowledge/resolve (Phase 08)
- Any workspace-scoped data (Phase 07)

---

## Token contract

`POST /auth/demo` issues a Sentinel JWT with:

```json
{
  "user_id": "demo",
  "email":   "demo",
  "role":    "demo",
  "exp":     <now + 3 minutes>
}
```

Same JWT secret, same `jsonwebtoken.verify()` middleware. The `role: 'demo'` claim is the only addition to the existing JWT shape.

---

## Rate limiting

Two layers:

| Layer | Limit | Scope |
|---|---|---|
| `POST /auth/demo` | 1 token per IP per hour | `express-rate-limit`, keyed by IP |
| Demo WebSocket connections | 10 concurrent max | In-memory counter in WS upgrade handler |

The per-IP hourly limit prevents bulk token farming. The concurrent WS cap prevents a single demo deployment from being overwhelmed.

---

## Session expiry

The demo JWT has a 3-minute `exp`. Two enforcement points:

1. **REST**: JWT middleware rejects expired tokens with 401 on every request — same as operator tokens.
2. **WebSocket**: validate JWT on upgrade. Additionally, when the connection has been open for 3 minutes, close it with code `4401` and a `"demo session expired"` reason. The client shows a "Demo expired — try again" prompt.

No refresh tokens for demo sessions. The user hits `POST /auth/demo` again (subject to the hourly rate limit).

---

## Implementation additions to CP4

On top of the base API scaffold, demo mode requires:

- `POST /auth/demo` route — issue demo JWT, apply IP rate limiter
- `requireAuth` middleware reads `role` from JWT; passes for both `'demo'` and operator tokens
- `requireOperator` middleware — rejects `role: 'demo'`; applied to restricted routes
- WS upgrade handler: store token `exp` on the socket object; close with `4401` when expired
- Concurrent demo connection counter (module-level integer; increment on upgrade, decrement on close)

---

## Dashboard

"Try Demo" button on the login page alongside "Sign in with Google". On click: `POST /auth/demo` → cookie set → redirect to map. When the WS closes with `4401`, overlay a banner: "Demo session ended. Sign in with Google for full access."
