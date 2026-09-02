# CP7l: Demo Expiry UX — Verification

Covers how this checkpoint was verified without waiting 3 real minutes for a demo session to
expire, and confirms the existing CP7f/CP7i wiring already satisfied the exit proof — this
checkpoint needed no code changes.

---

## No new code — a verification checkpoint

Everything CP7l's exit proof requires already existed:

- `DemoExpiredBanner` (`shared/ui/`) — dormant until shown, per its own comment.
- `liveSocket.ts`'s `onClose` handler fires `demoExpiredListeners` on close code 4401 and
  deliberately does not schedule a reconnect for that code (CP7i).
- `onDemoExpired` is threaded from `dashboard/page.tsx` → `Workspace` → `MapWidget` (via
  Dockview `params`) → `useLiveFeed`, matching `ws-separation-flow.puml`'s documented sequence.
- The banner's "Sign in" link points at `/login`, which is a client-side redirect stub to `/`
  (the real landing page) — `services/dashboard/src/app/login/page.tsx` — an existing pattern,
  not something added for this checkpoint.

The job here was proving the whole chain actually fires correctly end to end, not building it.

---

## Verifying a 3-minute expiry without waiting 3 minutes

`DEMO_JWT_EXPIRES_IN` (`api/config.ts`) is a hardcoded `'3m'`, not env-configurable — by design,
per its own comment ("operational but not tuned via env in v1"). Rather than edit source to
shorten it for testing (and risk forgetting to revert it) or actually wait three real minutes,
a JWT with a short `exp` was signed directly using the same secret the API already has in
`services/api/.env`, then set as the `sentinel_jwt` cookie via Playwright's
`context.addCookies()` before navigating — this is exactly the cookie the real
`POST /auth/demo` endpoint would have issued, just with a shorter lifetime. The server's own
`wsServer.ts` schedules the 4401 close from the JWT's own `exp` claim at connection time
(`payload.exp * 1000 - Date.now()`), so this exercises the real server-side timer, not a mock.

**First attempt used a 5 s expiry and failed for an uninteresting reason**: Next.js's dev-mode
cold compile of `/dashboard` on first navigation took long enough that the token had already
expired before the page finished loading, so the *initial* auth check (a different code path —
`dashboard/page.tsx`'s mount-time `GET /api/healthz-auth`, unrelated to the WS-mid-session flow
this checkpoint cares about) redirected away before the dashboard ever rendered. A 20 s expiry
gave enough headroom to confirm landing on `/dashboard` successfully first, *then* wait out the
remaining time to observe the mid-session expiry — the actual CP7l scenario.

---

## What was proven

With the browser confirmed on `/dashboard` before expiry: the banner appeared with the correct
text and a working "Sign in" link at the moment of expiry; clicking it landed back on `/` with
both the demo and Google sign-in options available — a genuinely recoverable end state, not a
dead end. Two console 401s during this window are expected (other components' API calls
correctly detecting the now-expired session, same as any other 401 path in this app) and not a
bug.

---

## Manual inspection

Sign a short-lived demo token and set it as a cookie to see this without a real wait:

```bash
node -e "
const jwt = require('jsonwebtoken');
console.log(jwt.sign({ user_id: 'demo', email: 'demo', role: 'demo' }, '<JWT_SECRET from api/.env>', { expiresIn: '20s' }));
"
```

Set it as `sentinel_jwt` (httpOnly) via browser DevTools or a Playwright `context.addCookies`
call, navigate to `/dashboard`, wait ~20 s past issuance, watch DevTools' Network → WS tab for
the close and the banner appearing in the UI at the same moment.
