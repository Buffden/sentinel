# Workspace Reconnect Flow — Checkpoint Debrief (CP4)

Real evidence for CP4, checked on 2026-09-13. See [`workspace-reconnect-flow.md`](workspace-reconnect-flow.md) for the mental model this checkpoint implements.

---

## Automated tests

`liveSocket.test.ts` — the generation-guard race is the one thing in this checkpoint actually worth a rigorous test, since it's the one place a subtle bug could silently double a WebSocket connection in production:

```
Test Files  1 passed (1)
     Tests  5 passed (5)
```

Specifically proven, against a controlled fake `WebSocket` driven by hand, not a real browser socket (this is pure client-side control flow, not a distributed-systems guarantee needing real infra):

- `forceReconnect` opens the new connection immediately, not after the normal 5-second delay
- the old connection's own (later-firing) close event does **not** schedule a duplicate reconnect — the exact race this checkpoint's generation counter exists to prevent, reproduced and confirmed fixed, not just reasoned about in prose
- `onReconnect` listeners fire once, from the forced reconnect, exactly like a real drop-and-recover would
- calling `forceReconnect` before any connection has ever opened is a safe no-op
- the pre-existing automatic-reconnect-after-a-drop behavior is unchanged (still waits the full 5 seconds)

`workspaceApi.test.ts`:

```
Test Files  1 passed (1)
     Tests  5 passed (5)
```

`getWorkspaceScope` correctly distinguishes a real scope, no-workspace-yet (`404` → `null`), and a genuine error (throws). `saveWorkspaceScope` calls `forceReconnect` exactly once on a successful save and never calls it on a rejected one.

Full dashboard suite: **30/30 passing**, no regressions from refactoring `useLiveFeed.ts` to share `getWsUrl()` instead of duplicating the URL-resolution literal.

---

## What this proves, and what it doesn't yet

Proves: the actual hard part of this checkpoint — the reconnect race a naive "close then open" implementation would have — is real, reproduced under test, and fixed. The dashboard dev server also hot-reloaded the change with a clean compile, so the code is at least syntactically and structurally sound in the real Next.js/Turbopack environment, not just under Vitest.

Does **not** yet prove: this from an actual click in the browser. `saveWorkspaceScope` is not yet reachable from anywhere in the UI — there is no scope editor to click (that's CP5), and unlike CP1-CP3's raw REST endpoints, this checkpoint's logic lives inside an ES module with no global/console entry point, so there's no equivalent of the `fetch(...)` console commands used to verify earlier checkpoints live. Genuine end-to-end proof — an operator edits their scope, the connection visibly cycles, the new scope takes effect — necessarily waits for CP5's UI to exist. This checkpoint delivers exactly the plumbing that UI will call.
