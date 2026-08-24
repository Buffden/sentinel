# TimescaleDB Persistence

Concept notes and debrief for idempotent `raw_events` and `position_history` writes.

| File | What's inside |
| --- | --- |
| [raw-events-design.md](raw-events-design.md) | Why `raw_events` exists, its schema, why it is not a hypertable, and the JSONB payload contract |
| [idempotency-model.md](idempotency-model.md) | The two identity keys, `ON CONFLICT DO NOTHING`, and crash-recovery replay |
| [write-order-and-availability.md](write-order-and-availability.md) | Why `raw_events` is written first on every path and what that means for availability |
| [timescaledb-persistence-debrief.md](timescaledb-persistence-debrief.md) | Observed outputs from running the persistence and idempotency experiments |
