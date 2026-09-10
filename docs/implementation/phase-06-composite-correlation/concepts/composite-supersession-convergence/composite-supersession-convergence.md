# Composite Supersession Convergence: Design and Learning Reference

Plain language first, then technical depth, then the code. Use this to understand, inspect, and defend CP5B.

---

## The one question this checkpoint answers, and the ones it deliberately doesn't

CP5B answers exactly: *when a `COMPOSITE` alert and the individual alert it supersedes can arrive at the API in either order, how does the durable database end up in the identical correct state either way, without ever fabricating data?* It does not implement the `alert-events` envelope retrofit (`ALERT_CREATED`/`ALERT_STATUS_CHANGED`/`ALERT_SUPERSEDED`) that `DATA_MODEL.md` documents but neither `alertSink.ts` nor `wsServer.ts` has ever implemented, that's a pre-existing, cross-cutting gap affecting every alert type, not specific to composite correlation. It does not touch the dashboard, and it does not implement Phase 08's operator ACK/Resolve endpoints.

This checkpoint replaces ADR-010's original assumption (the referenced alert already exists when `COMPOSITE` arrives) with a design that converges regardless of arrival order, resolved and reviewed across several rounds before any code was written (`DATA_MODEL.md`'s "Pre-CP5B: composite supersession convergence protocol").

---

## Concepts in plain language

### Why a placeholder row was rejected

The first candidate design considered was: when `COMPOSITE` arrives before its referenced alert exists, insert a placeholder row for it directly as `SUPERSEDED`. This was rejected because `COMPOSITE`'s own Kafka message cannot honestly supply several of the referenced alert's `NOT NULL` canonical fields: `detected_at` would have to borrow `COMPOSITE`'s own processing timestamp (a genuinely different event), and `SIGNAL_LOSS`'s own payload evidence (callsign, last-known position) simply isn't present in `COMPOSITE`'s payload at all. If the real message never arrives, that fabricated row is wrong forever, not merely absent. A missing row is honest; a wrong one is not.

### Why the pending-supersession table, not a Redis marker

The accepted design is one small, purely additive table:

```sql
CREATE TABLE IF NOT EXISTS pending_alert_supersessions (
    referenced_alert_id TEXT PRIMARY KEY,
    composite_alert_id  TEXT NOT NULL REFERENCES alerts (alert_id),
    created_at          TIMESTAMPTZ NOT NULL
);
```

A cross-system marker (a Redis key, say) was considered and rejected: it can't share a transaction with the composite's own Postgres commit. A crash between "commit the composite" and "write the Redis marker" (or the reverse) could leave the two systems disagreeing, reopening the exact bug this table exists to close. Keeping the pending record in the *same* database, the *same* transaction, as the composite insert gives true atomicity a cross-system design cannot offer without a two-phase-commit protocol this stack has no reason to build.

### Why a per-alert_id advisory lock, when Postgres already has row locking

Row-level locking only protects rows that exist. The out-of-order case is defined by the referenced row *not* existing yet, so a plain `UPDATE` (0 rows) followed by an existence-check `SELECT` isn't serialized against a concurrent transaction inserting that exact row in between; both sides can independently, correctly, observe "doesn't exist yet" from a stale snapshot. `pg_advisory_xact_lock($namespace, hashtext($alert_id))` manufactures the missing lock target: every alert, individual or composite, acquires the lock on its own `alert_id` before touching anything about it, and a `COMPOSITE` message acquires one per entry in `supersedes_alert_ids`, sorted, before touching any of them (the standard rule for a transaction that ever takes more than one lock, avoiding a cross-transaction deadlock cycle). The lock and every statement it protects have to run on the *same* checked-out `pg.PoolClient`, an advisory-transaction lock is scoped to the physical connection that took it, so `withTransaction` in `db.ts` exists specifically to make that impossible to get wrong.

### Why ownership conflicts are symmetric at two levels

Two different composites both claiming to supersede the same individual alert should be structurally impossible, the Alert Evaluator's own claim protocol guarantees a loss episode is claimed by at most one composite. So when it's detected, it's treated as an invariant violation, not a routine branch, the same fail-closed posture as `CandidateDecisionConflictError`/`CompositeFinalizeInvariantError` upstream. This check exists at two levels that mirror each other: at the pending-row level (a conflict-aware `ON CONFLICT ... DO UPDATE ... WHERE` guard that only treats a *matching* `composite_alert_id` as idempotent), and again at the row level once the referenced alert actually exists (comparing its current `superseded_by` before accepting a replay as idempotent). Either level throws `AlertSupersessionInvariantError` on a mismatch, rolling back the whole transaction, including the new composite's own insert. That's deliberate: a composite whose supersession can't be honestly recorded shouldn't be left half-applied either.

### Why post-commit publication is never "what this attempt changed"

A `COMPOSITE` message can require multiple Redis publishes after one DB commit (the composite row, every alert it actually supersedes). If the first publish succeeds and a later one throws, the Kafka offset never commits and the message redelivers, but the redelivered transaction is now idempotent by construction and may mutate nothing at all. Deriving "what to publish" from "what this attempt's SQL statements touched" would silently drop the un-published event forever, the DB would be correct, the WebSocket stream would have quietly lost an event. Every row a message concerns is instead captured (`RETURNING *`, or a plain `SELECT` when it already existed as-is) inside the transaction, before commit, while its lock is still held, and every captured row is republished unconditionally on every delivery attempt, whether or not that specific attempt did any work. Duplicates are expected and already accepted by the existing WebSocket contract; a lost lifecycle event is not.

### Why a late-arriving alert publishes its real persisted state, never the raw Kafka bytes

Before this checkpoint, `alertSink.ts` republished the incoming Kafka message's raw bytes verbatim. That's no longer sound once a message's *persisted* state can legitimately differ from its *received* state, exactly what happens when a pending supersession consumes a `SIGNAL_LOSS` message that arrived carrying `status: 'NEW'` but is persisted as `SUPERSEDED`. Every publish now derives from the row actually read back from Postgres, not from the message that triggered it.

### The one thing this checkpoint explicitly does not guarantee

CP5B guarantees durable DB convergence and replay-safe publication. It does **not** guarantee ordering between Redis publishes originating from two *different*, separately committed transactions, a `SIGNAL_LOSS` message's own transaction publishes independently of a later `COMPOSITE` message's transaction, on its own schedule. A client can genuinely observe a stale `NEW` event arrive after it already rendered that same alert as `SUPERSEDED`. CP5C's job (not this checkpoint's) is to make lifecycle merging monotonic, never let a terminal or superseded state regress on top of an already-rendered later one, with `GET /alerts` as the durable reconciliation source rather than trusting WebSocket arrival order.

---

## Map to code

| Concept | Where |
| --- | --- |
| Pending-supersession table | `infra/migrations/009_pending_alert_supersessions.sql` |
| Transaction helper (single `PoolClient`, `BEGIN`/`COMMIT`/`ROLLBACK`) | `withTransaction`, `services/api/src/db.ts` |
| Advisory lock namespace + acquisition | `ALERT_SUPERSESSION_LOCK_NAMESPACE`, `lockAlertId`, `services/api/src/sink/compositeSupersession.ts` |
| Composite persistence (sorted locks, convergent update-or-pend, symmetric invariant checks) | `persistCompositeAlert`, same file |
| Individual alert persistence (pending consumption, never fabricated fields) | `persistIndividualAlert`, same file |
| Conflict-aware pending upsert | `upsertPendingSupersession`, same file |
| The invariant-violation error | `AlertSupersessionInvariantError`, same file |
| Canonical publish shape, derived from the DB row | `PublishedAlert`, `rowToPublishedAlert`, same file |
| Dispatch by alert type, publish-after-commit orchestration | `persistAlert`, `startAlertSink`'s `eachMessage`, `services/api/src/sink/alertSink.ts` |
| The design this implements | `DATA_MODEL.md`'s "Pre-CP5B: composite supersession convergence protocol" |

---

## Retention questions

1. Why does a placeholder row for the not-yet-arrived alert require fabricating data, specifically, which fields?
2. Why can't the pending-supersession record live in Redis instead of Postgres?
3. Walk through, concretely, why `UPDATE` (0 rows) followed by a `SELECT` isn't safe against a concurrent transaction inserting that exact row, and how the advisory lock closes that gap.
4. Why must a `COMPOSITE` message acquire its locks in sorted order, when an individual alert message only ever needs one lock?
5. What's the difference between the pending-row ownership check and the row-level ownership check, and why does the design need both rather than just one?
6. Why does post-commit publication have to be "the row's current state, always," rather than "the rows this call's SQL statements touched"?
7. What does CP5B *not* guarantee about WebSocket delivery, and whose job is it to compensate for that?

---

## Completion checklist

- [ ] I can explain why a placeholder row was rejected, specifically which canonical fields it would have to fabricate
- [ ] I can trace the exact race that makes plain row-locking insufficient for the out-of-order case
- [ ] I can explain why the advisory lock and its protected queries must share one `PoolClient`
- [ ] I can explain both levels of the ownership-conflict check and why a mismatch rolls back the composite's own insert too
- [ ] I can explain why publish-after-commit must republish current state unconditionally, not "what changed this attempt"
- [ ] I can explain the WebSocket-ordering guarantee this checkpoint does not make, and why that's CP5C's problem to solve, not this one's
- [ ] I ran the integration suite and the manual inspection myself and can interpret both
