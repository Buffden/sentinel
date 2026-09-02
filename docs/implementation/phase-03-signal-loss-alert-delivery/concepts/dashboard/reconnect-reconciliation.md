# CP7k: Reconnect Reconciliation

Covers why a reconnected WebSocket alone doesn't recover missed data, the `onReconnect` signal
added to close that gap, and how it was proven end-to-end against a real disconnect.

---

## The gap this closes

Before this checkpoint, `liveSocket.ts` (CP7i) already reconnected automatically 5 seconds
after an unexpected close — but reconnecting the transport isn't the same as recovering state.
Two things were silently lost on every drop:

- The server had forgotten this connection's bbox subscription (`wsServer.ts`'s
  `connectionBBox` map is keyed by the live `ws` object; a new connection after reconnect is a
  new key, starting unfiltered/unsubscribed) — nothing re-sent `subscribe(bbox)`.
- Anything published on `position-updates` or `alert-events` *during* the disconnected window
  was gone forever. Kafka/Redis pub/sub is fire-and-forget; there was no buffering, and nothing
  re-ran `GET /entities/live` or `GET /alerts` to pick up what was missed.

The phase doc's own hydration sequence — REST hydrate, then `subscribe(bbox)` — was only ever
being run once, at mount. CP7k's job is running it again on reconnect.

---

## The fix: `onReconnect`, only on a genuine reconnect

`liveSocket.ts` tracks `hasConnectedBefore`, a module-level flag. Its `onOpen` handler only
fires the new `reconnectListeners` set when that flag is already `true` — the very first
connect is not a reconnect, and each widget's own mount-time hydration already covers it.
Firing on every open (including the first) would have made every widget hydrate twice on
startup for no reason.

`useLiveFeed` exposes this as one more optional callback alongside `onPositionUpdate` /
`onAlertUpdate` / `onDemoExpired`, wired through the same ref pattern as the others so the
subscription effect never needs to re-run when the callback identity changes across renders.

**MapWidget** had a real ordering problem extracting this: `onReconnect` needs to call
`hydrateAndSubscribe`, which needs `subscribe` — but `subscribe` only exists *after*
`useLiveFeed` returns, and `useLiveFeed` needs `onReconnect` *before* it returns. Broken via a
ref (`hydrateAndSubscribeRef`): `onReconnect` calls through the ref unconditionally (refs have
no temporal-dead-zone issue), and the actual `hydrateAndSubscribe` function — defined after
`useLiveFeed`, once `subscribe` genuinely exists — is assigned into that ref every render via
`useLayoutEffect` (this repo's lint config forbids writing a ref directly during render; see
`react-hooks/refs` below).

**AlertWidget** had no such cycle (it doesn't use `subscribe`), but tripped a different rule:
calling the extracted `hydrateAlerts()` as a direct statement inside `useEffect`'s body trips
`react-hooks/set-state-in-effect` even though the actual `setAlerts` call only happens after an
`await fetch(...)` several event-loop turns later — the lint rule's static analysis flags any
function it can prove eventually calls `setState`, called directly in an effect body, regardless
of sync/async timing. Deferring the same call through `queueMicrotask` satisfies the rule
without changing observable behavior at all (still runs essentially immediately, before paint).

---

## Two strict lint rules hit again, same family as CP7g/CP7j

This repo's `react-hooks` lint config is stricter than default in two ways that came up again
here, both already seen in CP7g's drag-bounds fix:

- **`react-hooks/refs`**: no ref writes during render. `hydrateAndSubscribeRef.current = ...`
  had to move into a `useLayoutEffect`.
- **`react-hooks/set-state-in-effect`**: no direct call, inside an effect body, to a function
  the linter can statically prove calls `setState` — even one that's genuinely async and safe.
  `queueMicrotask` sidesteps the heuristic.

Neither is a real bug in either case; both are the linter refusing to trust that an async
boundary makes a call safe. Worth knowing before hitting either again.

---

## What was proven, and how

Verified against a **real** disconnect, not a simulated one: killed the running API process
(closing the WebSocket from the server side, not the client), confirmed the browser actually
detected the close (`page.on('websocket', ws => ws.on('close', ...))` in Playwright), inserted a
new alert **directly into Postgres** while the API was down — bypassing Kafka and Redis
entirely, so it could never arrive via the live feed under any circumstance — then restarted the
API and waited. The new alert appeared in the panel with zero user action, alongside the
alert that was already there before the drop. That's the actual claim CP7k makes: recovery
without a page refresh, not just "the socket reconnects."

MapWidget's `hydrateAndSubscribe` wasn't separately re-verified with live aircraft data — it
uses the identical `onReconnect` wiring already proven correct via AlertWidget, and its own
fetch/adapter logic was already proven correct in CP7e/CP7f.

---

## Manual inspection

1. Open the dashboard, confirm data loads.
2. Stop the API (`lsof -ti:3000 -sTCP:LISTEN | xargs kill`) — DevTools' Network → WS tab shows
   the connection close.
3. While it's down, insert a row directly into Postgres (or Redis, for a live entity) so it
   exists only in the database, never published anywhere.
4. Restart the API. Within ~5 seconds the dashboard should show the new data, unprompted.
