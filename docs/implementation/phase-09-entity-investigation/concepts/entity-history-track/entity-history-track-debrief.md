# Entity History Track — Checkpoint Debrief (FE-CP2)

Evidence for FE-CP2, checked on 2026-09-21. See [`entity-history-track.md`](entity-history-track.md) for the mental model this checkpoint implements.

---

## Automated checks

- `tsc --noEmit` (dashboard): clean.
- `eslint` (dashboard, full `src`): clean -- caught two real issues mid-implementation (see below).
- Full dashboard test suite: **43/43 passing** -- the prior 38 (all unmodified) plus 5 new for the `entities/entity-history` adapter: full-point adaptation, `altitude_m: null` preserved rather than coerced, and the `isValidWireHistoryPointDto` guard's accept/reject cases.

## Two real lint catches during implementation

1. **`react-hooks/set-state-in-effect`**: the first version reset `state` to `'loading'` synchronously at the top of the fetch effect, needed because (unlike FE-CP1's `entityId`) the window preset genuinely changes during this tab's lifetime. Fixed by extracting the fetch into `HistoryPoints`, remounted via `key={entityId-fromMs-toMs}` on every window change -- a fresh mount's own initial state already covers the reset, so there is nothing to imperatively reset.
2. **`react-hooks/purity`**: reading `Date.now()` directly during render (to compute the no-anchor fallback window end) was flagged as an impure call. Fixed by capturing it once via a lazy `useState(() => Date.now())` initializer.

Both are real anti-patterns the rule correctly caught, not false positives worked around.

## Real browser verification (not just the test suite)

Reused FE-CP1's driver setup (Playwright + injected demo cookie, avoiding the real rate limit). Against the real running API + dashboard + Postgres:

1. Opened a real alert's entity from `AlertWidget`, clicked into the HISTORY tab: the window selector showed "last 2h," and the panel correctly displayed "Window ends at the alert that opened this panel" -- confirming `anchorMs` was actually threaded through from the alert's real `detected_at`, not just present in the type signature.
2. **Discovered `position_history` was completely empty in the dev database at verification time** (checked directly via `psql`) -- the panel correctly showed "No position history in this window," a real, confirmed-correct empty state, not a bug needing a workaround.
3. Widened the window to the maximum preset (24h): still correctly empty, confirming the preset-change refetch itself works (no stale state, no crash) even when the result stays empty.
4. **To verify the chart-rendering path itself** (not just the empty state), manually seeded 5 real `position_history` rows for the same entity via `psql`, spanning a climb-then-descent altitude profile (5,000 m → 12,000 m → 4,000 m) within the alert's own 2-hour window. Re-ran the browser check: the chart rendered correctly -- a real SVG polyline tracing the seeded altitude profile, a gray dot at the earliest point, a green dot at the most recent, correct UTC time labels at both ends, and the correct altitude range summary (4000–12000 m). Screenshot confirmed visually, not just asserted by a selector count.
5. Switched back to the OVERVIEW tab afterward: rendered correctly, confirming tab-switching doesn't corrupt either tab's own state.
6. Zero console/page errors across the entire run.
7. All manually-seeded `position_history` rows deleted afterward (confirmed via a follow-up `count(*)` query); both API and dashboard server PIDs confirmed via `lsof` and explicitly killed; ports confirmed free.

## What this proves, and what it doesn't yet

Proves: the `anchorMs` threading works end to end from a real alert click through to the History tab's actual window, both the empty-window and populated-chart render paths work against real backend data, the keyed-remount fix correctly handles window changes without stale or corrupted state, and switching between Overview and History preserves both tabs' own state correctly.

Does not prove on its own: the map-overlay track (explicitly out of scope, its own follow-on) or the Relationships tab (FE-CP3, Neo4j graph pivot, not yet built).
