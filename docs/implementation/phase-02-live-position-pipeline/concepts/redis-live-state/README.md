# Redis Live State

Concept notes and debrief for the Redis live state implementation: ephemeral entity snapshots with a monotonic timestamp guard.

| File | What's inside |
| --- | --- |
| [live-hash-design.md](live-hash-design.md) | `entity:live:{entity_id}` hash structure, field list, TTL policy, and why hash over other Redis types |
| [monotonic-guard.md](monotonic-guard.md) | Why a Lua guard is required, how it prevents concurrent consumer races, the `>=` rejection rule |
| [write-order-and-availability.md](write-order-and-availability.md) | Where Redis fits in the write sequence, availability dependency, stale vs error distinction, replay idempotency |
| [redis-live-state-debrief.md](redis-live-state-debrief.md) | Observed outputs from running the live state and stale-event experiments |
