# Composite Supersession Convergence Debrief

CP5B, commit `799c79b`.

---

## Setup

```bash
make up
make migrate
make topics
cd services/api
```

---

## Experiment 1: automated suite against real Postgres, Redis, and Redpanda

```bash
npx tsc --noEmit
npx vitest run
```

```text
Test Files  5 passed (5)
     Tests  48 passed (48)
```

21 tests in `alertSink.integration.test.ts` (5 baseline persistence tests, 11 Pre-CP5B convergence tests, 3 advisory-locking tests, 2 full-consumer-loop tests), against the existing suite (all still passing, none touched in behavior). Full run repeated 3 consecutive times with no flakiness after fixing a real cross-file interference bug found while first running the whole suite (see below).

| Test | Proves |
| --- | --- |
| SIGNAL_LOSS then COMPOSITE | Normal order: referenced row transitions `NEW -> SUPERSEDED` |
| COMPOSITE then SIGNAL_LOSS | Out-of-order: the referenced alert lands directly as `SUPERSEDED`, never observably `NEW` |
| never fabricates the late-arriving alert's canonical fields | `detected_at`/`payload`/`entity_type` come from the real message, not the composite's own |
| ACKNOWLEDGED -> SUPERSEDED | Normal order, `ACKNOWLEDGED` also transitions |
| RESOLVED remains terminal | Never retroactively superseded, only the composite itself publishes |
| already SUPERSEDED by the same composite | Idempotent replay |
| already SUPERSEDED by a different composite (existing row) | `AlertSupersessionInvariantError`, whole transaction rolls back, including the second composite's own insert |
| pending supersession owned by a different composite | Same invariant failure, one step earlier |
| COMPOSITE replay | Idempotent even when the retry's own SQL statements affect 0 rows |
| late SIGNAL_LOSS replay | Idempotent after being consumed via the pending path, never reverts to `NEW` |
| referenced alert that never arrives | Valid, harmless pending row; no fabricated alert row ever created |
| `pg_advisory_xact_lock` genuinely blocks | Direct proof: a second transaction on the same `alert_id` provably waits |
| real concurrent arrival converges | `Promise.all` race between the composite and individual paths, no dangling claim either way |
| sorted multi-lock acquisition, no deadlock | Two referenced ids, three concurrent transactions, completes within a bounded timeout |
| Redis publish failure on the second of two publishes | Not committed; redelivery republishes both durable events even though the retry performs no new DB transition |

**A real bug caught while first running this suite.** A fresh consumer group's initial offset (`fromBeginning: false`) is only resolved once the group has actually joined and been assigned its partition; `consumer.run()` resolves once the run loop starts, not once that join completes. Producing immediately after `startAlertSink()` raced the group actually being ready, intermittently missing the first message entirely (roughly 80% failure rate under repeated runs). Fixed by polling real group membership (`admin.describeGroups`, the same pattern the Alert Evaluator's own ADR-005 tests already use) before producing, rather than guessing at a fixed delay.

**A second real issue, not a logic bug**: running the full API test suite showed `wsServer.integration.test.ts` intermittently failing with "unexpected message received", traced to Vitest's default cross-file parallelism, `alertSink`'s tests (now legitimately publishing more `alert-events` messages than before) running concurrently in a separate worker against the same real Redis instance `wsServer`'s own tests listen on unfiltered. Confirmed by running `wsServer.integration.test.ts` alone (10/10 pass, reliably). Fixed with `fileParallelism: false` in `vitest.config.ts`, a one-line config change, not a code change.

---

## Experiment 2: manual inspection against real Postgres

Ran `persistAlert`/`persistCompositeAlert`/`persistIndividualAlert` directly via `tsx` against real Postgres, not test doubles:

**Order 1 (SIGNAL_LOSS first):**

```text
after SIGNAL_LOSS insert: { alert_id: 'demo-alert-f20a...', status: 'NEW', superseded_by: null }
after COMPOSITE: { alert_id: 'demo-alert-f20a...', status: 'SUPERSEDED', superseded_by: 'demo-composite-39dd...' }
```

**Order 2 (COMPOSITE first, out-of-order):**

```text
referenced alert row before it ever arrives (should not exist): []
pending_alert_supersessions row: {
  referenced_alert_id: 'demo-alert-1121...',
  composite_alert_id: 'demo-composite-627f...',
  created_at: 2026-09-10T04:23:33.990Z
}
referenced alert after arriving late: { alert_id: 'demo-alert-1121...', status: 'SUPERSEDED', superseded_by: 'demo-composite-627f...' }
pending row consumed: []
```

**Invariant violation (a different composite claiming the same referenced alert):**

```text
threw as expected: AlertSupersessionInvariantError - alert demo-alert-d198... is already superseded
  by demo-composite-9039..., cannot also be superseded by demo-composite-56eb...
```

| Check | Expected | Observed |
| --- | --- | --- |
| Order 1: referenced row transitions NEW -> SUPERSEDED | yes | PASS |
| Order 2: no row exists for the referenced alert before it arrives, no placeholder | yes | PASS |
| Order 2: pending row records the correct composite_alert_id | yes | PASS |
| Order 2: referenced alert lands directly as SUPERSEDED on arrival | yes | PASS |
| Order 2: pending row is consumed (deleted) on arrival | yes | PASS |
| A different composite claiming an already-superseded alert throws, does not silently overwrite | yes | PASS |

Database confirmed clean afterward (`SELECT * FROM alerts WHERE alert_id LIKE '%demo%'` and the equivalent on `pending_alert_supersessions` both returned zero rows).

---

## Engineering debrief

**Data flow:** `persistAlert` dispatches by `alert_type`. `COMPOSITE` locks every referenced `alert_id` (sorted), inserts itself idempotently, then for each referenced id either updates it directly (if it exists and is `NEW`/`ACKNOWLEDGED`), recognizes an idempotent replay or invariant violation (if it exists and is already `SUPERSEDED`), leaves it alone (if `RESOLVED`), or records a pending supersession (if it doesn't exist yet). Any other alert type locks its own id, consumes a pending entry if one exists, and persists directly, all inside one transaction via `withTransaction`. `startAlertSink` publishes every row `persistAlert` returns, unconditionally, after commit, then commits the Kafka offset last.

**Trade-off:** the pending-supersession table is small and purely additive, no change to `alerts`' existing constraints, but it does mean an out-of-order `COMPOSITE` leaves a real row lingering in a second table until its referenced alert eventually arrives (or forever, if it never does, which is the accepted, honest failure mode, a missing row rather than a wrong one).

**Failure behaviour:** an ownership conflict, at the pending-row level or the row level, throws `AlertSupersessionInvariantError` and rolls back the entire transaction, including the conflicting composite's own insert, rather than partially applying a corrupted supersession. A Redis publish failure partway through a multi-publish fan-out leaves the Kafka offset uncommitted; redelivery republishes every row the message concerns from its current durable state, not just the one that previously failed, even when the retry's own SQL statements touch nothing.

## Manual inspection commands

```bash
cd services/api
npx vitest run src/sink/alertSink.integration.test.ts
```

```bash
docker exec sentinel-timescaledb psql -U sentinel -d sentinel -c "SELECT alert_id, status, superseded_by FROM alerts WHERE alert_id = '<id>';"
docker exec sentinel-timescaledb psql -U sentinel -d sentinel -c "SELECT * FROM pending_alert_supersessions;"
docker exec sentinel-redpanda rpk topic consume alerts -n 1
```

## Knowledge-check questions

1. Why would a placeholder row for a not-yet-arrived `SIGNAL_LOSS` have to fabricate its `detected_at`, specifically?
2. Trace the exact interleaving that makes `UPDATE` (0 rows) then `SELECT` unsafe without the advisory lock, and explain precisely how the lock closes it.
3. Why does an ownership conflict roll back the composite's own insert too, rather than just skipping the one conflicting referenced id?
4. Why can't "publish what this SQL statement's `RETURNING` clause returned" be the rule for what gets published after commit?
5. What did the consumer-group-readiness bug and the cross-file Vitest interference bug have in common, and how are they different from a correctness bug in the persistence logic itself?

## Optional manual tweak

Seed a `RESOLVED` alert, then process a `COMPOSITE` referencing it in the out-of-order direction (composite first), and confirm by hand that no pending-supersession row is ever created for it, only an already-existing row's status is checked before deciding whether a pending entry applies.

## Next

CP5C: a low-fidelity SVG mockup, developer approval, then a minimal `COMPOSITE`/`SUPERSEDED` dashboard presentation, rendering `supersedes_alert_ids` generically and implementing the monotonic lifecycle-merge CP5B's own design explicitly requires. Not started as part of this checkpoint.

---

## Key observations

| Concept | Observed |
| --- | --- |
| Automated suite | 48/48 PASS across the full API service, repeated runs with no flakiness after both real bugs were fixed |
| Real output inspection | Both arrival orders, the invariant-violation throw, and clean final DB state all observed directly against real Postgres |
| No fabricated data | The out-of-order referenced alert never exists as a row until its real message arrives; only a pending-table entry exists in the meantime |
| Advisory locking | Proven as genuine mutual exclusion (a real blocked transaction), not merely inferred from correct outcomes |
| Replay-safe publication | A simulated mid-fan-out Redis failure, followed by redelivery, republished every durable event even though the retry's own DB work was a no-op |
