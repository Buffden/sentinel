# TimescaleDB Schema and Migrations Debrief

## Schema created

| Table | Notes |
| --- | --- |
| `position_history` | Hypertable, partitioned by `observed_at` (1-day chunks) |
| `users` | |
| `user_workspaces` | FK to `users` |
| `route_references` | |
| `route_reference_points` | FK to `route_references` |
| `alerts` | Self-referential FK (`superseded_by`), FK to `users` |

---

## Migration flow

`migrate.sh` globs `infra/migrations/[0-9]*.sql` in lexicographic order, which matches the numeric prefix dependency chain (006 depends on 003 which must already exist). Each file is piped via `docker exec -i` into psql inside the running container -- no local psql required.

---

## Transaction and failure guarantee

Two-layer protection:

| Layer | Mechanism | Effect |
| --- | --- | --- |
| DB | Each migration wrapped in `BEGIN/COMMIT` | SQL error causes full rollback of that file's DDL; no partial schema changes land |
| Shell | psql receives `-v ON_ERROR_STOP=1`; `migrate.sh` uses `set -euo pipefail` | psql exits non-zero on any SQL error; script aborts immediately and remaining migrations do not run |

Failure experiment confirmed both layers:

| Observation | Result |
| --- | --- |
| psql exit code on `SELECT 1/0` | Code 3 (non-zero) |
| `rollback_experiment` table after failed run | Not present |

---

## TimescaleDB result

`position_history` was converted to a hypertable partitioned by `observed_at` with 1-day chunk intervals. The 30-day retention policy is registered.

Immediately after migration, before any inserts, `num_chunks = 0`. TimescaleDB creates a chunk only when the first row is inserted into the corresponding time range. Inserting a test row and then deleting it removes the row but does not remove the chunk -- the chunk continues to exist as an empty partition. Zero rows and zero chunks are different facts: zero rows means no data was written; zero chunks means no insert has ever occurred in that time range.

The unique index `(entity_id, observed_at)` includes the partitioning column, satisfying TimescaleDB's requirement that unique indexes on hypertables must cover the partition dimension. This index also enables `ON CONFLICT (entity_id, observed_at) DO NOTHING` for idempotent position writes.

---

## Idempotency result

Double-insert experiment:
- First insert: `INSERT 0 1`
- Second insert (same `entity_id, observed_at`): `INSERT 0 0` -- silently discarded
- Row count remained 1

This is the at-least-once Kafka guarantee made safe: the Position Consumer can re-deliver a telemetry event and the idempotent write ensures no duplicate row lands.

---

## Clean rebuild result

`make reset` -> `make up` -> `make migrate` on a completely blank database produced identical output. All 6 migrations applied in order, `IF NOT EXISTS` guards ensured no errors on re-run, and the schema matches the first run exactly. The pipeline is reproducible.

---

## Deferred

- Kafka topic creation
- Redis key-space design
- Neo4j constraints (`MERGE` idempotency)
- Application code -- nothing written yet
- `position_history` chunk inspection: zero chunks until the first insert lands; inserting and deleting a test row creates a chunk but does not remove it on delete

---

## Open architectural questions

One existing constraint confirmed: Neo4j Community Edition does not support uniqueness constraints on relationship properties, which affects how `PROXIMITY` edge episode identity is enforced. The `MERGE` approach on node and relationship structure is the workaround; the Correlation Worker ADR already accounts for this.

---

## Knowledge check

Before moving to Kafka topic provisioning, you should be able to answer these without looking:

1. Why must the unique index on `position_history` include `observed_at`?
2. What does `ON_ERROR_STOP=1` change about psql's default behavior?
3. Why is `superseded_by` a plain FK and not `DEFERRABLE`?
4. What happens to chunks when the 30-day retention policy fires?
5. What does `num_chunks = 0` tell you about how TimescaleDB creates chunks?
6. If you add a `007_foo.sql` migration and `make migrate` has already been run, what happens on re-run?

The last question points at what a proper migration framework like Flyway or golang-migrate adds: a `schema_versions` table that tracks which migrations have already been applied. The simple ordered-glob approach used here relies on `IF NOT EXISTS` guards as the safety net instead.
