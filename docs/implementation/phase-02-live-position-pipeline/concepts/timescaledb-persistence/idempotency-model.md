# Idempotency Model

---

## Two tables, two identity keys

The persistence layer introduces two idempotent writes per Kafka message. Each uses the identity of the logical fact being stored, not a shared universal key.

| Table | Identity key | What it represents |
| --- | --- | --- |
| `raw_events` | `(source_topic, source_partition, source_offset)` | A specific Kafka record |
| `position_history` | `(entity_id, observed_at)` | A canonical position fact for an entity at a point in source event time |

Both use `ON CONFLICT ... DO NOTHING`. A replayed message produces the same key values and the duplicate insert is silently dropped.

---

## Why partition is mandatory in the raw_events key

Kafka offsets are only unique within a partition. Offset 42 on partition 0 and offset 42 on partition 1 are two completely different records. If the key were just `(source_topic, source_offset)`, one of those records would be incorrectly treated as a duplicate of the other.

This was verified directly. With two rows in `raw_events`:

```
source_topic  source_partition  source_offset
adsb.raw      0                 42
adsb.raw      1                 42
```

Both rows exist. Replaying either produces `INSERT 0 0` for that specific row. The other row is unaffected.

Even though `adsb.raw` currently has one partition, the key must include `source_partition` now. Adding it later would require a migration that could silently corrupt existing idempotency checks.

---

## ON CONFLICT DO NOTHING behavior

`INSERT 0 0` means zero rows inserted — the unique constraint was violated and the row was discarded without error. The existing row is unchanged.

One side effect: `BIGSERIAL` advances even when a row is not inserted. If a replay attempt fails the unique constraint, the sequence gap is visible in the `id` column. This is expected Postgres behavior and carries no correctness implication.

---

## Crash recovery

The offset commit is the last action for every message. Two crash scenarios exist:

**Crash before writes complete:**
Kafka redelivers. Both inserts run from scratch. No prior state to conflict with. Rows are inserted normally.

**Crash after writes, before offset commit:**
Kafka redelivers. Both inserts attempt again. Both hit `ON CONFLICT DO NOTHING`. `INSERT 0 0` for each. Offset is then committed. Final state is identical to a clean first run.

In both cases, replay produces the same database state. This is the at-least-once transport guarantee combined with idempotent durable effects — which gives the same correctness as exactly-once without requiring Kafka transactions.

---

## The two writes are not in a single transaction

The persistence layer issues two separate `INSERT` statements. A crash between them leaves `raw_events` with a row and `position_history` without one.

On replay:
- The `raw_events` insert hits `ON CONFLICT DO NOTHING`.
- The `position_history` insert succeeds.

Both tables end up consistent. `raw_events` is an archive and not a correctness dependency, so a temporary gap between the two writes is acceptable.

---

## position_history identity: (entity_id, observed_at)

`observed_at` is derived from source event time: `to_timestamp(timestamp_ms / 1000.0)`. It is never processing time.

Using source event time as the idempotency anchor means:
- Replaying the same Kafka message always produces the same `observed_at`.
- Two different Kafka messages for the same entity at the same event-time second collapse to one row. This is correct: they represent the same physical moment for that entity.

**Multi-provider note:** if two providers report the same `entity_id` at the same Unix second, they produce identical `(entity_id, observed_at)` values and only one row survives. Conflict/precedence between providers is a future ADR. The constraint is correct for the current single-provider (OpenSky) scope.
