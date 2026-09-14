# Workspace Scope Controls — Checkpoint Debrief (CP5)

Evidence for CP5, checked on 2026-09-13. See [`workspace-scope-controls.md`](workspace-scope-controls.md) for the mental model this checkpoint implements, and `mockups/workspace-scope-editor.svg` for the approved design it was built against.

---

## Automated checks

- `tsc --noEmit`: clean, no type errors introduced by `WorkspaceScopeControl.tsx`, `WorkspaceScopeModal.tsx`, or the `workspaceApi.ts`/`useLiveFeed.ts` changes.
- Full dashboard test suite: **30/30 passing**, unchanged from before this checkpoint — CP5 added no new pure logic worth a unit test (the modal is a form bound directly to already-tested `workspaceApi.ts` functions and already-tested `forceReconnect`), and the dashboard has no component-testing setup (no jsdom/Testing Library) to exercise the rendered UI itself.
- The live Next.js dev server (Turbopack) hot-reloaded every change with a clean compile — `✓ Compiled` after each edit, no build errors — and continued serving `/dashboard` at `200` throughout, including to the real operator session that had been open since CP1's testing.

---

## What this proves, and what it doesn't yet

Proves: the code is type-correct, builds cleanly in the real dev server, and every function it calls (`getWorkspaceScope`, `saveWorkspaceScope`, `getRegions`, `forceReconnect`) was already proven correct in isolation by CP1-CP4's own test suites.

Does **not** yet prove: that the UI actually works when clicked. This is the one CP5 genuinely cannot self-certify the way CP1-CP4 could through console `fetch(...)` calls — there is no console equivalent for "does the modal render correctly, does the region dropdown populate, does selecting Custom reveal the bounds inputs, does the map/alert feed visibly change after a save." That is a real click-through, and per this project's own rule that UI changes must be exercised in a browser before being called complete, it belongs to the developer, not to an automated check. The completion checklist's own last item says so directly.

## What to actually click through

1. Refresh the dashboard (or it may already be live via Turbopack hot-reload). A small new icon should appear in the top nav, left of the profile icon.
2. Click it — the modal should open showing the real currently-saved scope (SF Bay Area custom bounds, `UNSCHEDULED_PROXIMITY`, from earlier CP2 testing).
3. Change the region to something else (e.g. "France"), check a different alert type combination, and Save.
4. Confirm: the modal closes, and the alert feed / map should reflect the new scope shortly after (the WebSocket reconnects per CP4 — worth watching the Network tab's WS frames or just noticing the visible alert set change).
5. Optionally: use `psql` to confirm the `user_workspaces` row actually changed, the same way CP1's manual check worked.
6. If you want to see the genuine first-time-setup path, that requires a workspace with no saved row at all — not safe to fake against your own real account's row without deleting it, so this one is easiest to reason about from the code and the mockup rather than reproduced live right now.

---

## Live click-through, actually done

Confirmed live on 2026-09-13, and it surfaced two real problems worth recording, not just a clean pass:

1. **First pass:** functionally correct — icon appeared, modal opened with the real saved scope, editing and saving worked. But native `<input type="checkbox">` and `<select>` render with the browser's own light-mode widget chrome (a filled blue circle for a checked box in Safari), which looked jarring against the dark theme. Not a logic bug, a real aesthetic gap the mockup's flat SVG shapes didn't expose. Fixed by replacing both with custom-rendered controls (a `Checkbox` component matching the mockup's square-with-checkmark, a styled `<select>` with `appearance: none` and a custom chevron), plus more panel depth (elevated background, drop shadow, larger radius).
2. **Second pass, a real functional bug:** only the alert type already checked from the saved scope appeared checked; clicking any other row did nothing visible. Root cause: the alert-type row was structured as a `<div onClick={toggle}>` wrapping a `<Checkbox onChange={toggle}>` — a native `<button>` click always bubbles to its parent, so every click fired `toggleAlertType` twice (on, then immediately back off), a net no-op. Fixed by removing the duplicate handler from `Checkbox` and leaving the row `div` as the single source of truth. This is exactly the kind of bug a mockup or a type-checker cannot catch — it only showed up by actually clicking multiple checkboxes in the real browser, which is the entire reason this checkpoint's own completion checklist required a real click-through rather than accepting automated evidence alone.

CP5 is now confirmed working end to end through the real dashboard.
