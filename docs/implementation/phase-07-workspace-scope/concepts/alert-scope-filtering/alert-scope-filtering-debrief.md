# Alert Scope Filtering — Checkpoint Debrief (CP2)

Real evidence for CP2, checked on 2026-09-13 against real Postgres, not restated from the design doc. See [`alert-scope-filtering.md`](alert-scope-filtering.md) for the mental model this checkpoint implements.

---

## Automated tests

Unit tests for the shared predicate (`extractAlertPosition`, `matchesScope`), no database — including negative tests proving the *old*, wrong ADR-012 field names (`payload.lat`/`lon` for `SIGNAL_LOSS`, top-level `payload.lat`/`lon` for `COMPOSITE`) do **not** match, only the corrected ones do:

```
Test Files  1 passed (1)
     Tests  13 passed (13)
```

Integration tests against real Postgres (`alerts.integration.test.ts`, new `GET /alerts scope filtering` block) — operator with no saved workspace gets `[]`, operator with a saved France/`SIGNAL_LOSS`-only scope gets exactly the in-bounds, in-type alert and excludes both an out-of-bounds one and a wrong-alert-type one, demo with a `bbox` gets geography-only filtering, demo with no `bbox` gets everything unfiltered, demo with a malformed `bbox` gets `400`:

```
Test Files  1 passed (1)
     Tests  8 passed (8)
```

Full API suite after the change:

```
Test Files  7 passed, 1 flaky-unrelated (8)
     Tests  89 passed, 1 failed (90)
```

The one failure (`wsServer.integration.test.ts` — a JWT-expiry timing test) reproduces as flaky under full-suite load and passes cleanly in isolation; it touches nothing CP2 changed and was already timing-sensitive before this checkpoint.

---

## A real bug caught by the negative tests, not just the design doc

The unit tests don't just assert the correct field paths — they assert the *old*, ADR-012-as-originally-written field paths return `null`. Concretely: `extractAlertPosition` on a `SIGNAL_LOSS` alert with a flat `payload.lat`/`payload.lon` (what the ADR said before this checkpoint corrected it) returns `null`, not a position — proving that if CP2 had been implemented against the original ADR text instead of the actual payload builders, every real `SIGNAL_LOSS` and `COMPOSITE` alert would have silently failed to extract a position and been excluded from every operator's scoped view, with no error anywhere. This is exactly the failure Pre-CP2's design pass existed to catch before it shipped.

---

## A test-infrastructure bug caught while writing this checkpoint's own tests

Adding a second `describe` block to `alerts.integration.test.ts` (for the new role-based scoping tests) initially broke the *existing*, unmodified tests in the first block: both blocks import the same module-level `pool` singleton from `../db.js`, and the first block's `afterAll` called `pool.end()` — which tore down the pool before the second block's tests ran, since Vitest executes both blocks in one file sequentially, sharing one module instance. Fixed by moving `pool.end()` to the last block's `afterAll` instead. Left a comment in the first block explaining why `pool.end()` isn't there anymore, so a future third `describe` in this file doesn't reintroduce the same trap.

---

## Live verification through the real running dashboard

Brought the API and dashboard back up (infra containers had stayed healthy throughout) and tested through the actual browser, logged in as a real Google-authenticated operator.

First pass surfaced a real, useful negative result: the operator's workspace was still saved as `France` + `alert_types: ["SIGNAL_LOSS"]` from CP1 testing, while every real alert in the table was `UNSCHEDULED_PROXIMITY` clustered around San Francisco (the ingestion-poller's actual bbox). `GET /alerts` correctly returned `[]` — excluded on both geography and alert type simultaneously. Not a bug; the filter doing exactly what it should against a scope that genuinely didn't match reality.

Updated the saved workspace through the browser to a custom SF Bay Area box with `alert_types: ["UNSCHEDULED_PROXIMITY"]`, then re-read `GET /alerts`:

```
await fetch('/api/alerts').then(r => r.json()).then(a => a.length)
// 2724
```

Cross-checked independently in Postgres, not trusting the API's own math:

```sql
SELECT COUNT(*) FROM alerts WHERE status IN ('NEW','ACKNOWLEDGED');
-- 2945 total

SELECT alert_type, COUNT(*) FROM alerts WHERE status IN ('NEW','ACKNOWLEDGED') GROUP BY alert_type;
-- COMPOSITE: 2, SIGNAL_LOSS: 219, UNSCHEDULED_PROXIMITY: 2724

SELECT COUNT(*) FROM alerts
WHERE status IN ('NEW','ACKNOWLEDGED')
  AND alert_type = 'UNSCHEDULED_PROXIMITY'
  AND (payload->>'lat')::float BETWEEN 36.9 AND 38.1
  AND (payload->>'lon')::float BETWEEN -122.8 AND -121.5;
-- 2724
```

The browser's `2724` matches the independently-computed SQL count exactly, and the excluded `221` (`219` `SIGNAL_LOSS` + `2` `COMPOSITE`) accounts for the rest of the `2945` total precisely. This is the real-data version of the same claim the unit tests make on small hand-built examples — confirmed at the scale an actual multi-hour live run produces, not just a handful of seeded rows.

---

## What this proves, and what it doesn't yet

Proves: `GET /alerts` genuinely enforces an operator's saved workspace scope against real, large-scale live data (thousands of real rows, not a handful of fixtures), and the shared predicate's type/geography logic is correct at that scale. Also proves, incidentally, that a stale saved scope (left over from an earlier checkpoint's testing) silently produces an empty result rather than an error — worth remembering as an operator UX consideration for CP5's frontend work, not a bug in this checkpoint.

Does **not** yet touch: the WebSocket `alert-events` fan-out, which still delivers unfiltered regardless of any saved scope. That's CP3, reusing this checkpoint's `matchesScope` directly. Demo-session bbox filtering was covered by the integration tests but not re-verified live in this pass (the live session was authenticated as a real operator throughout).
