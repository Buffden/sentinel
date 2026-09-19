# Alert Lifecycle UI — Checkpoint Debrief

Evidence for the Phase 08 frontend checkpoint, checked on 2026-09-19. See [`alert-lifecycle-ui.md`](alert-lifecycle-ui.md) for the mental model this checkpoint implements, and `mockups/alert-lifecycle-controls.svg` for the approved design it was built against.

---

## Automated checks

- `tsc --noEmit` (dashboard): clean, no type errors introduced by `entities/alert/api.ts` or `AlertWidget.tsx`.
- `eslint` (dashboard): zero warnings in either touched file (the 1090 pre-existing warnings in the lint run are all in a generated file, `public/maplibre-gl-worker.mjs`, unrelated to this checkpoint).
- Full dashboard test suite: **30/30 passing**, unchanged — this checkpoint added no new pure logic worth a unit test; it's a form bound to `patchAlertStatus`, which is a thin wrapper with the same shape as the already-tested `saveWorkspaceScope`.

## Real click-through, actually done

No browser extension was available this session, so verification used a scripted headless Chrome driven directly over the Chrome DevTools Protocol (no Puppeteer/Playwright installed — raw WebSocket JSON-RPC against `--remote-debugging-port`), against the real dev stack: `make up`, the real API (`tsx --env-file=.env src/index.ts`), and the real dashboard (`next dev`). A real operator user, a matching `user_workspaces` scope, and a real `NEW` `SIGNAL_LOSS` alert were seeded directly into Postgres; a JWT cookie was minted and injected via `Network.setCookie`, then the page was driven and screenshotted at each step:

1. **Collapsed list**: `ALERTS 1`, the seeded alert `UI92XM` showing a blue `NEW` badge next to `SIGNAL LOSS` — matches the mockup's collapsed-badge design exactly.
2. **Expanded, NEW**: clicking the card revealed the detail panel with a `STATUS: NEW` badge row and, scrolled into view, both `Acknowledge` (blue outline) and `Resolve` (green outline) buttons.
3. **Clicked Acknowledge**: the button list on the page changed from `Acknowledge | Resolve` to `Resolve` only — confirming the click actually fired the PATCH and the component re-rendered from the real response, not a stub. Screenshot confirms the badge changed to amber `ACKNOWLEDGED` and only the green `Resolve` button remains.
4. **Clicked Resolve** (on a fresh page load, re-confirming the durable `ACKNOWLEDGED` state persisted from step 3): the alert panel changed to `ALERTS 0` / `No open alerts` — the resolved card disappeared from the list immediately, exactly the mockup's approved RESOLVED-disappears behavior, not a lingering badge.

Every step used the real running API, real Postgres, and real Redis fan-out — not a mock. All seeded fixtures (`user_workspaces`, the `users` row, the `ui-check-*` alert) were deleted afterward, and the temporary headless-Chrome/dev-server processes were stopped.

## What this proves

The full operator-visible path works end to end through the real dashboard: click → `PATCH /alerts/:alert_id` → durable Postgres transition → response applied to UI state → badge and button set update correctly → a resolved alert leaves the visible list. This is the first Phase 08 checkpoint (frontend or backend) verified through an actual rendered browser rather than `curl`/`psql`/test-suite evidence alone.

## What it doesn't cover

- The 403 (demo session) and 409 (illegal transition, e.g. racing a composite) paths were exercised by the backend's own integration tests (CP2/CP3/CP4), not re-clicked through this browser session — the frontend code path for both is a straightforward render-nothing / apply-the-returned-row branch, not independently exercised live here.
- Cross-instance convergence (CP4's own guarantee) was proven at the backend level with two real API instances; this session ran a single API instance, so it did not re-demonstrate a second browser tab on a second instance converging. That guarantee doesn't change based on which client triggered the transition.
