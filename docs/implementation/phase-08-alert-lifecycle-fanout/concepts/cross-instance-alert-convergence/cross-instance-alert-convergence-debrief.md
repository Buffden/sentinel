# Cross-Instance Alert Convergence — Checkpoint Debrief (CP4)

Evidence for CP4, checked on 2026-09-19 (backfilled after the fact — see `concepts/README.md`'s documentation-debt note). See [`cross-instance-alert-convergence.md`](cross-instance-alert-convergence.md) for the mental model this checkpoint implements.

---

## First run: caught the real bug

The crash-injection test (`a publish failure after the DB commit leaves the transition durable; a client retry republishes it with no duplicate write`) initially failed with `Test timed out in 15000ms` and an unhandled rejection: `Error: simulated crash before publish` originating from `redis.publish` inside the PATCH route. The request never returned any response at all — not a `500`, nothing. This is the exact "hangs instead of failing cleanly" symptom CP1–CP3's own debugging thought it had already fixed.

Root cause, confirmed by reading Express 4's own handler-dispatch behavior: an `async` route handler's rejected promise is never automatically forwarded to error-handling middleware. The existing `index.ts` middleware was syntactically fine and completely inert, because nothing ever called `next(err)`.

## The fix, and re-verification

`services/api/src/shared/asyncHandler.ts` wraps a handler so its rejection reaches `next(err)` explicitly. Applied to every async handler in the service (`alerts.ts` — both `GET` and `PATCH`, `workspace.ts` — both `POST` and `PUT`, `entitiesLive.ts` — `GET`), not only the one under test.

After the fix, the full suite: **10/10 test files, 109/109 tests passing**, including both new CP4 tests:

- `an ACK through instance A converges on a client connected only to instance B`: passed on the first run, before the bugfix, since it doesn't touch the error path.
- `a publish failure after the DB commit leaves the transition durable; a client retry republishes it with no duplicate write`: failed before the fix (hung, then unhandled-rejection), passed cleanly after — confirming `500` is returned immediately on the simulated crash, the DB row is durably `ACKNOWLEDGED` despite the failed publish, and a retried `PATCH` republishes via the idempotent-replay path with `acknowledged_at`/`acknowledged_by` unchanged from the first (failed-publish) attempt.

Also confirmed: after this fix, the full backend suite was re-run against a clean environment (no leftover dev-server process consuming the real `alerts` Kafka topic under the real `api` consumer group — an unrelated cross-talk issue from a live manual-verification session that had been left running, diagnosed and stopped before the final clean run).

## What this proves

Both of the phase file's "Key Experiment" guarantees hold against real Postgres, real Redis, and two independently-isolated in-process API instances: cross-instance convergence via Redis pub/sub alone, and crash-before-publish recovery via the durable-commit + idempotent-retry design CP1–CP3 built. It also proves the service's async error handling is now actually correct everywhere, not just believed to be.
