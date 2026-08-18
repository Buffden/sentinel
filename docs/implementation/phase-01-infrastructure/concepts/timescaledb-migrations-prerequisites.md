# Phase 01 — Checkpoint 2 Prerequisite Notes

Checkpoint 2 applies Sentinel's canonical PostgreSQL/TimescaleDB schema through explicit SQL migrations.

---

## 1. What Checkpoint 2 Does

```text
TimescaleDB running
  → apply SQL migrations
  → tables / indexes / constraints created
  → position_history converted to hypertable
  → verify schema
```

---

## 2. Transactions in DDL

PostgreSQL DDL is transactional. Wrap each migration file in:

```sql
BEGIN;
-- statements
COMMIT;
```

If any statement fails the whole transaction rolls back. No partial migration remains. This is unlike MySQL where DDL commits immediately.

---

## 3. Why `psql -v ON_ERROR_STOP=1` Matters

By default `psql` continues executing after a failed statement. `ON_ERROR_STOP=1` makes it exit immediately with a non-zero code, so the shell runner stops instead of silently continuing past a broken migration.

---

## 4. What a Migration Is

A versioned SQL file applied in numeric order:

```text
001_extensions.sql
002_position_history.sql
003_users.sql
```

Checkpoint 2 uses plain SQL files without a migration framework. Without a history table there is no record of which versions were applied — `make reset` is the recovery path if something goes wrong.

---

## 5. Idempotency vs Migration Tracking

**Idempotency**: re-running a statement does not corrupt the result — achieved with `CREATE TABLE IF NOT EXISTS`, `CREATE INDEX IF NOT EXISTS`, `if_not_exists => TRUE` on TimescaleDB functions.

**Migration tracking**: a ledger recording which versions have been applied. Checkpoint 2 has idempotency but not tracking. Do not confuse the two.

---

## 6. PostgreSQL Extensions

TimescaleDB is a PostgreSQL extension. Activating it:

```sql
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
```

This must be migration 001. Nothing that depends on TimescaleDB functions can run before it.

---

## 7. What a Hypertable Is

A hypertable is a normal PostgreSQL table that TimescaleDB partitions internally by time. Application code uses standard `INSERT` and `SELECT`. TimescaleDB manages the underlying chunks transparently.

`position_history` is partitioned by `observed_at` only — not by `entity_id` or `geo_cell`.

---

## 8. Hypertable Uniqueness Rule

A unique index on a hypertable must include the partitioning column. Since Sentinel partitions by `observed_at`:

| Constraint | Valid? | Reason |
| --- | --- | --- |
| `UNIQUE (entity_id, observed_at)` | Yes | Partition column `observed_at` is included |
| `UNIQUE (entity_id)` | No | TimescaleDB rejects: partition column missing |

TimescaleDB cannot enforce global uniqueness across independent time chunks unless the partition column participates in the constraint.

---

## 9. Chunk Interval and Retention

Sentinel uses a **1-day chunk interval** — TimescaleDB creates one internal partition per day. Retention is **30 days** — old chunks are dropped automatically, which is far more efficient than a row-level DELETE.

Both are set in the migration and should be verified after apply.

---

## 10. Self-Referential Foreign Key

`alerts.superseded_by` references `alerts.alert_id` on the same table. A composite alert supersedes individual alerts in one transaction. Inserting the composite first and then updating the individual alerts means a standard FK works without deferral, as long as the transaction follows that order.

Checkpoint 2 only needs to create the constraint. The transaction ordering is a Phase 06 concern.

---

## 11. Migration Order

Referenced objects must exist before dependent ones:

| Migration | Tables | Dependency |
| --- | --- | --- |
| `001` | extensions | none |
| `002` | `position_history` | needs TimescaleDB extension from 001 |
| `003` | `users` | none |
| `004` | `user_workspaces` | FK to `users` (003) |
| `005` | `route_references`, `route_reference_points` | none |
| `006` | `alerts` | FK to `users` (003) |

---

## 12. Explicit Execution

Migrations run via `make migrate`, not automatically on `docker compose up`. Keeping infrastructure startup and schema changes separate makes both visible and debuggable.

---

## 13. Clean Reset Workflow

```text
make reset → make up → make migrate
```

If this always produces an identical schema, Checkpoint 2 is complete.

---

## 14. What to Inspect After Migrations

Do not trust only "script exited 0." First bring up the stack and run migrations:

```bash
make up        # start all containers, wait for healthy
make migrate   # apply migrations 001-006 in order
```

Then run the verification queries in [`scripts/timescaledb-schema/verify-schema.sql`](../../../../scripts/timescaledb-schema/verify-schema.sql).

---

## 15. Failure Experiment

Run [`scripts/timescaledb-schema/failure-experiment.sql`](../../../../scripts/timescaledb-schema/failure-experiment.sql) directly in psql to observe transaction rollback. Then confirm `rollback_experiment` does not exist. This proves the transaction aborted cleanly rather than leaving a half-applied schema.

---

## 16. Concepts Deferred

Do not study these for Checkpoint 2:

- Kafka: partitions, offsets, consumer groups, delivery semantics
- Redis: all data structures and patterns
- Neo4j: Cypher, constraints, graph modeling
- Advanced TimescaleDB: compression, continuous aggregates, distributed hypertables
- Application behavior: UUID generation policy, alert lifecycle, user deletion cascade rules

---

## 17. Knowledge Check

1. Why does `BEGIN ... COMMIT` protect a migration, and why do we also need `ON_ERROR_STOP=1`?
2. Why must `UNIQUE (entity_id, observed_at)` include `observed_at` on a hypertable?
3. Why must `003_users.sql` run before `006_alerts.sql`?
4. What is the difference between idempotent SQL and a migration history ledger?
5. What does `make reset → make up → make migrate` prove?
