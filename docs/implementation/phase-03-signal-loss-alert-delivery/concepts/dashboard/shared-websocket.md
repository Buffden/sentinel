# CP7i: Live Alert Feed and the Shared WebSocket Singleton

Covers why alerts couldn't just call `useLiveFeed()` the same way `MapWidget` does, the
page-level connection singleton that fixed it, and the two wire-shape differences between REST
and WebSocket alert delivery.

---

## What was built

Alerts now appear in the panel without a page refresh — new `alert-events` messages merge into
the same `Map<string, Alert>` CP7h's REST hydration already populates, keyed by `alert_id`.

---

## The problem: two independently-mounted panels, one required connection

`MapWidget` and `AlertWidget` are both rendered by Dockview as separate top-level panel
components (`Workspace.tsx` registers `map-widget` directly; `AlertWidget` is nested inside the
`widget-panel` component). Neither is a parent or child of the other — there is no JSX tree to
prop-drill a shared value through between them.

`useLiveFeed()` (as CP7f left it) opened a fresh `new WebSocket(...)` on every call. If
`AlertWidget` had called it independently for `alert-events`, that would have been a second
connection to the same endpoint — explicitly forbidden by this doc's own WebSocket-separation
rule: "One WebSocket per page — two connections would double position-update delivery." With
two sockets, position updates would have doubled too, not just alerts.

## The fix: a page-level, reference-counted singleton

`shared/realtime/liveSocket.ts` owns the actual connection now, not `useLiveFeed`. It exposes
`acquireLiveSocket(url)`, returning a subscription handle with `onFrame`, `onDemoExpired`, and
`release`. The module-level connection opens on the *first* caller and closes on the *last*
one's `release()` — reference counted, not tied to any single component's mount/unmount.
Reconnect (5 s delay, no reconnect after demo-expiry close code 4401) is centralized here too:
if each `useLiveFeed()` instance tried to manage its own reconnect timer against a shared
connection, two mounted instances would race each other into opening duplicate reconnections
after a drop.

`useLiveFeed()` itself is now a thin subscriber: it calls `acquireLiveSocket()` on mount,
registers its own frame handler, and calls `release()` on unmount. Its public shape barely
changed — `onPositionUpdate` and `onDemoExpired` still work exactly as CP7f left them, and it
gained one more optional callback, `onAlertUpdate`, alongside them. This is not a new hook: the
dashboard's own architecture doc already named `features/live-feed` as owning "position-updates,
alert-events, subscribe bbox" — extending the one hook, not adding a second, was always the
intended shape.

---

## Two wire-shape differences from the REST path

The `alert-events` frame is the Alert Evaluator's original Kafka message, republished verbatim
by the API's alert sink — not the same shape `GET /alerts` returns:

- `detected_at_ms` is a number straight from Kafka (`Date.now()` at scan time). The REST
  endpoint's `detected_at` is an ISO 8601 string, because Postgres's `TIMESTAMPTZ` column
  serializes that way through `res.json()`.
- The WS frame has no `updated_at`, `acknowledged_at`, or `resolved_at` at all — those are
  DB-only columns that don't exist until the alert is persisted; the original Kafka message
  never carried them.

This mirrors the split that already existed for positions (`WireEntityDto` for REST vs.
`WsPositionData` for WS in `useLiveFeed.ts`) — same reasoning, second occurrence: `parseAlertFrame`
is a distinct function from `entities/alert/adapter.ts`'s `wireToAlert`, not a shared one.

---

## Manual inspection

Open the dashboard in a browser, open DevTools' Network → WS tab, confirm exactly one
connection to the API appears regardless of how many widgets are mounted. Publish a message
directly to Redis to simulate what the API's alert sink would send:

```
docker exec sentinel-redis redis-cli PUBLISH alert-events '{"alert_id":"...","entity_id":"...","entity_type":"aircraft","alert_type":"SIGNAL_LOSS","priority":"STANDARD","status":"NEW","detected_at_ms":1700000000000,"payload":{"dark_since_ms":1700000000000}}'
```

The alert panel should show the new entry within a second, no refresh, badge count incremented.
