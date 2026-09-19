# Alert Lifecycle Write Path — Checkpoint Debrief (CP1–CP3)

Evidence for CP1–CP3, checked on 2026-09-19 (backfilled after the fact — see `concepts/README.md`'s documentation-debt note). See [`alert-lifecycle-write-path.md`](alert-lifecycle-write-path.md) for the mental model these checkpoints implement.

---

## Automated checks

- `tsc --noEmit` (api): clean at every commit in this range.
- Full API test suite at the end of this range: **107/107 passing**, including:
  - the full transition matrix (`NEW→ACKNOWLEDGED`, `NEW→RESOLVED`, `ACKNOWLEDGED→RESOLVED`, both idempotent-replay cases, both terminal-rejection cases) against real Postgres;
  - `an operator ACK that wins the lock race still lets a concurrent COMPOSITE supersede it afterward` — real concurrent `transitionAlert`/`persistCompositeAlert` calls against the same row, not a mocked lock;
  - `two concurrent transitionAlert calls for the same alert_id serialize under the advisory lock` — a raw two-client `pg_advisory_xact_lock` test proving the second caller genuinely blocks until the first commits;
  - HTTP-level auth (403 for demo), validation (400), not-found (404), conflict (409 with the real row), and a real Redis `SUBSCRIBE` confirming the publish actually happens.

## Real manual verification (not just the test suite)

With the real dev stack running (`make up`, real API process, real Postgres/Redis), a full end-to-end script (`.manual-check.mjs`, deleted afterward) exercised the real running server:

1. **Before**: a real `NEW` row, no `acknowledged_at`/`acknowledged_by`.
2. **PATCH #1** (`NEW → ACKNOWLEDGED`): `200`, the row updated correctly in Postgres, and the exact same shape landed on the real `alert-events` Redis channel within the same request.
3. **PATCH #2** (replayed, same target): still `200`, still republished to `alert-events` — but `acknowledged_at`/`updated_at` were byte-identical to PATCH #1's result, confirming the idempotent-replay path performs zero extra writes, not just "doesn't error."

All manual fixtures (test user, test alert) were deleted afterward.

## What this proves, and what it doesn't yet

Proves: the durable transition, the lock discipline against a concurrent Kafka-driven writer, and the fan-out publish all work correctly against real infrastructure, not mocks — both under automated test and under a live manual run against the actual running service.

Does not prove on its own: that this converges across *multiple* API instances, or that a crash between commit and publish is recoverable in practice, not just in theory. Both of those are CP4's own scope — see `cross-instance-alert-convergence-debrief.md`, which also records the real async-error-handling bug this checkpoint's own code depended on without knowing it was broken.
