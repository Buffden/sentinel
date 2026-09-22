# Alert Detail and Richer Filtering — Checkpoint Debrief (CP5)

Evidence for CP5, checked on 2026-09-21. See [`alert-detail-and-richer-filtering.md`](alert-detail-and-richer-filtering.md) for the mental model this checkpoint implements.

---

## Automated checks

- `tsc --noEmit` (api): clean.
- Full API test suite: **145/145 passing** -- the prior 137 (all unmodified, confirming the default-unchanged guarantee) plus 8 new:
  - `?status=RESOLVED,SUPERSEDED` overrides the default and returns only those statuses;
  - an unknown `?status=` value returns `400`;
  - `?entity_id=` matches an alert where the id is primary or counterparty;
  - `GET /alerts/:alert_id` 404s for an unknown id;
  - returns a `RESOLVED` alert directly (not restricted to `NEW`/`ACKNOWLEDGED` like the list);
  - 404 for an operator with no saved workspace, even for a real alert;
  - 404 for an operator when the alert's payload position is outside their saved bounds;
  - 200 for an operator when the alert is inside their saved scope.

## Real manual verification (not just the test suite)

With the real dev stack running (server PID confirmed via `lsof -iTCP:3000` at both startup and teardown):

1. Seeded one real `RESOLVED` and one real `NEW` alert for the same entity via `psql`. `curl`'d `GET /alerts` with no params as a real demo session: the `NEW` one appeared, the `RESOLVED` one did not -- the exact pre-CP5 default, confirmed unchanged against real data, not just the test suite.
2. `curl`'d `GET /alerts?status=RESOLVED`: only the resolved one came back.
3. `curl`'d `GET /alerts?entity_id=manual-cp5-entity&status=NEW,RESOLVED`: both came back, confirming the entity filter matches independent of the status override.
4. `curl`'d `GET /alerts/manual-cp5-resolved` directly: the full real row, `200`.
5. `curl`'d `GET /alerts?status=BOGUS`: real `400`.
6. Inserted a real operator with no saved workspace via `psql`, minted a real JWT: `curl`'d the same real resolved alert by id -- real `404`, fail-closed even though the alert plainly exists.
7. All manually-inserted rows deleted afterward; server process confirmed stopped via `lsof`.

## What this proves, and what it doesn't yet

Proves: both new query params are correctly additive (verified against real data, not assumed from the diff), invalid input is rejected loudly, and the by-id alert read shares the exact same fail-closed scope guarantee every other by-id endpoint in this phase already has -- now covering `alerts` as well as `entities`.

Does not prove on its own: any frontend investigation UI. All five of Phase 09's backend checkpoints (CP1-CP5) are now complete; the phase's own vertical-slice exit criterion (an operator investigating through the dashboard, not direct datastore access) still requires the frontend checkpoint(s), which follow the mockup -> approval -> implementation gate per `CLAUDE.md` and have not started.
