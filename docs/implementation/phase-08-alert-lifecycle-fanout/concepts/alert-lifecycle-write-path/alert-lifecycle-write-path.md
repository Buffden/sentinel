# Alert Lifecycle Write Path — Design and Learning Reference

Plain language first, then technical depth, then the code. Use this to understand, inspect, and defend CP1–CP3 (the durable `PATCH` transition and its fan-out).

---

## What this checkpoint is, and deliberately isn't

CP1 resolved the design before any code: how a `PATCH /alerts/:alert_id` request — a second, independent writer to the `alerts` table — coexists safely with the Kafka consumer's own writer (`compositeSupersession.ts`, built in Phase 06 for composite supersession). CP2 implemented that contract. CP3, publishing the resulting row to `alert-events` on every transition, was built in the same pass as CP2 rather than sequentially — a `PATCH` that persists a transition but never tells anyone is a broken intermediate state, not a useful one, so shipping them separately would have meant landing broken behavior first.

This checkpoint does not touch the frontend at all (see `alert-lifecycle-ui/` for that), and it does not change anything about how the *first* alert insert works — that path (Phase 03's `alertSink.ts` persisting on Kafka consumption) is untouched.

---

## Concepts in plain language

### Why a second writer needs the same lock a Kafka consumer already uses

Postgres row locks only protect a single statement. Two separate transactions — one from `PATCH`, one from the Kafka consumer processing a `COMPOSITE` — can each read a row's current state, then both write, producing a result neither one actually intended (e.g., an alert left `ACKNOWLEDGED` with `superseded_by` also set). `compositeSupersession.ts` already solved this for its own writers with `pg_advisory_xact_lock(1001, hashtext(alert_id))`, an application-level mutex scoped to one `alert_id` for the whole transaction. `transitionAlert` (CP2) takes the exact same lock before reading or writing anything, so the two writers always serialize — whichever locks first commits first, and the second always sees the first's committed result before deciding its own outcome.

### Why the transition matrix has an idempotent-replay branch, not just legal/illegal

An operator can double-click, or a client can retry after a timeout without knowing if the first attempt landed. If the requested target status already matches the current status, `transitionAlert` does nothing to the database — no write, no bumped `updated_at` — but still returns (and the caller still republishes) the canonical current row. This isn't the same as "success" and "failure": it's a third outcome, tested explicitly, because collapsing it into "success" would make a replay silently re-run the `UPDATE`, and collapsing it into "failure" would make retrying a lost request look like an error the operator has to react to.

### Why a `409` response carries the real row, not just an error message

`RESOLVED → ACKNOWLEDGED` (illegal — terminal) and a composite superseding an alert moments before the operator's click both produce `invalid_transition`. Returning only `{"error": "..."}` would leave the caller's UI stuck showing stale buttons for a state that no longer exists. Returning the alert's actual current row lets the caller converge to reality — this is the same `PublishedAlert` shape used everywhere else, on every path, not a special error format.

### Why the fan-out publish happens even on the idempotent no-op path

If an earlier attempt's publish was lost (crash between commit and publish, or a dropped connection), the only way to recover is for a retry to try again — and a retry that hits the idempotent-replay branch must still publish, or the lost notification is lost forever even though the durable state was always correct. This is the same "republish canonical state on every delivery, never just what this attempt changed" discipline `compositeSupersession.ts` already established for Kafka redelivery, applied here for HTTP retry instead.

### Why this surfaced a real, unrelated bug (and what that means for how it was fixed)

Testing this path with a real FK violation (a test fixture bug, not a product bug) meant a thrown error inside the `PATCH` handler's async function had nowhere to go — Express 4 does not forward a rejected promise to error-handling middleware unless something explicitly calls `next(err)`. The first attempt at a fix (an error-handling middleware registered in `index.ts`) looked plausible but was never actually exercised against a real thrown error in this checkpoint's own tests, so it shipped silently broken; CP4's crash-injection test caught it for real (see `cross-instance-alert-convergence.md`). The real fix — a shared `asyncHandler` wrapper applied to every async route in the service — belongs to that checkpoint's writeup, not this one, since this checkpoint's own code was correct; the surrounding request-handling gap it depended on was not.

---

## Map to code

| Concept | Where it lives |
| --- | --- |
| Lock/row/publish primitives, reused not duplicated | `services/api/src/sink/compositeSupersession.ts` (`lockAlertId`, `rowToPublishedAlert`, `AlertRow`, `ALERT_SUPERSESSION_LOCK_NAMESPACE`, exported for this checkpoint) |
| Transition matrix, idempotent replay, illegal-transition handling | `services/api/src/routes/alertLifecycle.ts` (`transitionAlert`) |
| `PATCH /alerts/:alert_id` route, operator-only gating, publish-on-every-success-path | `services/api/src/routes/alerts.ts` |
| Corrected `alert-events` wire contract (no `type`/`event_type` field — the CP1 documentation finding) | `docs/DATA_MODEL.md`'s `alert-events` and WebSocket alert-event sections |
| Lock-serialization proof against a real concurrent `COMPOSITE` | `services/api/src/routes/alertLifecycle.integration.test.ts` |
| HTTP-level auth/validation/publish proof | `services/api/src/routes/alerts.integration.test.ts`'s `PATCH /alerts/:alert_id` describe block |

---

## Retention questions

1. Why does `transitionAlert` take the exact same advisory lock `compositeSupersession.ts` uses, instead of a lock of its own?
2. Walk through what happens if an operator's ACK and a COMPOSITE's supersession both target the same alert at the same instant.
3. Why is a same-status replay a third outcome (`idempotent`), not folded into either `applied` or an error?
4. Why does a `409` response body carry the alert's real row instead of just an error string?
5. Why did the DATA_MODEL.md `alert-events` envelope documentation need correcting before this checkpoint's code was written, rather than after?

---

## Completion checklist

- [ ] I can explain why two independent writers to `alerts` need a shared lock, not just careful ordering
- [ ] I can trace the transition matrix from memory: which (current, target) pairs are legal, idempotent, or illegal
- [ ] I can explain why publishing on an idempotent no-op isn't wasted work
- [ ] I can explain why the API's actual `alert-events` wire shape has no `type`/`event_type` field, contrary to what the docs said before this checkpoint
- [ ] I have run the lock-race integration tests myself and read what they actually prove, not just that they pass
