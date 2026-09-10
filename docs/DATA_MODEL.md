# Data Model

Canonical schemas for Sentinel's persistent stores, Redis state, and Kafka contracts. Field names and identity rules here are authoritative for implementation.

---

## Global Time and Replay Rules

- Source telemetry time is carried as `timestamp_ms`.
- Episode anchors, replay guards, correlation windows, and deterministic identities use source event time.
- Composite correlation (see [Composite eligibility rule](#composite-eligibility-rule)) anchors to `dark_since_ms` for both an active-dark and a recently-resumed signal-loss episode. A Redis key's TTL is retention, never the eligibility boundary itself — retention bounds may outlive the true source-time eligibility window; the explicit `dark_since_ms` comparison is the sole authority for whether a candidate qualifies.
- Operational audit timestamps such as database `created_at` / `updated_at` may use processing time.
- Kafka processing is at-least-once. Durable writes must therefore be idempotent.
- Historical backfill uses a separate consumer group/mode and suppresses ephemeral live side effects.

---

## TimescaleDB

### `position_history` — hypertable

Partitioned on `observed_at` only. Chunk interval: 1 hour. Retention: 48 hours.

Schema below reflects target state after migration 007 (applied at CP5).

| Column | Type | Nullable | Description |
| --- | --- | --- | --- |
| `entity_id` | TEXT | No | ICAO hex, MMSI, or synthetic entity ID |
| `entity_type` | TEXT | No | `aircraft`, `vessel`, `satellite`, `ground_vehicle`, or `unknown` |
| `observed_at` | TIMESTAMPTZ | No | `to_timestamp(timestamp_ms / 1000.0)`; hypertable time column |
| `timestamp_ms` | BIGINT | No | Source event time in Unix ms |
| `geo_cell` | TEXT | Yes | H3 cell at `HISTORY_H3_RESOLUTION`; indexed query column, not partition dimension. Nullable: CP5 persists NULL; CP7 owns H3 computation and populates this column. |
| `lat` | DOUBLE PRECISION | No | Decimal degrees |
| `lon` | DOUBLE PRECISION | No | Decimal degrees |
| `altitude_m` | REAL | Yes | Preferred altitude (geo ?? baro); metres; null for vessels. Renamed from `altitude` in migration 007. |
| `source` | TEXT | No | `adsb`, `ais`, `satellite`, or `synthetic` |
| `provider` | TEXT | Yes | `opensky`, `aishub`, etc. |
| `baro_altitude_m` | REAL | Yes | Barometric altitude; metres |
| `geo_altitude_m` | REAL | Yes | GNSS altitude; metres |
| `speed_mps` | REAL | Yes | Ground speed; m/s |
| `course_deg` | REAL | Yes | Direction of movement; degrees clockwise from north |
| `heading_deg` | REAL | Yes | Vessel heading; degrees; null for aircraft |
| `vertical_rate_mps` | REAL | Yes | Climb/descent rate; m/s; null for vessels |
| `on_ground` | BOOLEAN | Yes | Surface-position indicator; null for vessels |
| `last_contact_ms` | BIGINT | Yes | Last transponder contact; Unix ms; null for AIS |
| `navigation_status` | TEXT | Yes | Normalized string enum (see below); null for aircraft |
| `rate_of_turn` | REAL | Yes | ROT; null for aircraft |
| `callsign` | TEXT | Yes | Callsign trimmed |
| `entity_subtype` | TEXT | Yes | Normalized class: `fixed_wing`, `rotorcraft`, `cargo`, etc. |
| `provider_category` | TEXT | Yes | Original provider classification verbatim |
| `squawk` | TEXT | Yes | 4-digit transponder code; null for AIS |
| `spi` | BOOLEAN | Yes | Special position identification; null for AIS |
| `position_source` | SMALLINT | Yes | 0=ADS-B 1=ASTERIX 2=MLAT 3=FLARM; null for AIS |
| `position_accuracy` | BOOLEAN | Yes | AIS high/low accuracy flag; null for ADS-B |
| `destination` | TEXT | Yes | DEST; null for ADS-B |
| `eta` | TEXT | Yes | ETA string; null for ADS-B |
| `draught_m` | REAL | Yes | Vessel draught; metres; null for aircraft |

Constraints/indexes:

- unique `(entity_id, observed_at)`; duplicates use `ON CONFLICT DO NOTHING`;
- `(entity_id, observed_at DESC)`;
- `(geo_cell, observed_at DESC)`.

TimescaleDB does **not** create separate chunks by `geo_cell`. H3 narrows rows inside time chunks through the index.

### `raw_events`

Plain PostgreSQL table (not a hypertable) on the TimescaleDB instance. Applied at CP5 via migration 008.

| Column | Type | Nullable | Description |
| --- | --- | --- | --- |
| `id` | BIGSERIAL PK | No | Surrogate key |
| `entity_id` | TEXT | Yes | ICAO hex, MMSI, or synthetic entity ID |
| `source` | TEXT | No | `adsb`, `ais`, etc. |
| `provider` | TEXT | Yes | `opensky`, `aishub`, etc. |
| `source_topic` | TEXT | No | Kafka topic the record arrived on |
| `source_partition` | INTEGER | No | Kafka partition number |
| `source_offset` | BIGINT | No | Kafka offset within the partition |
| `received_at` | TIMESTAMPTZ | No | Processing time of this write |
| `source_event_time` | TIMESTAMPTZ | Yes | `to_timestamp(timestamp_ms / 1000.0)` |
| `payload` | JSONB | No | Provider JSON object for valid records; JSONB string scalar for parse_error records |

Unique constraint: `(source_topic, source_partition, source_offset)`. Offsets are only unique within a partition, so the partition column is mandatory for correct idempotency. Replaying a message produces the same `(topic, partition, offset)` triple and is rejected by `ON CONFLICT DO NOTHING`.

Index: `(entity_id, received_at DESC)` for per-entity raw payload lookup.

`received_at` is processing time (audit). `source_event_time` is provider event time.

`raw_events` has no FK or guaranteed correlation key to `position_history`. `entity_id` + `source_event_time` may support best-effort investigation but are not guaranteed unique: `parse_error` and `no_position` records carry a null `source_event_time`, and two records for the same entity at the same event second are possible. The authoritative Kafka identity is `(source_topic, source_partition, source_offset)`.

Availability dependency: successful `raw_events` archival is required before offset commit. If the insert fails, the offset must not be committed. Kafka will redeliver the message and the insert will be retried idempotently via `ON CONFLICT DO NOTHING`.

### `route_references`

| Column | Type | Nullable |
| --- | --- | --- |
| `route_id` | TEXT PK | No |
| `entity_id` | TEXT | No |
| `route_name` | TEXT | No |
| `corridor_threshold_metres` | REAL | No |
| `source` | TEXT | No |
| `created_at` | TIMESTAMPTZ | No |

Index `entity_id`.

### `route_reference_points`

| Column | Type | Nullable |
| --- | --- | --- |
| `route_id` | TEXT | No |
| `sequence_no` | INTEGER | No |
| `lat` | DOUBLE PRECISION | No |
| `lon` | DOUBLE PRECISION | No |

Primary key `(route_id, sequence_no)`. Adjacent points define route segments. Route deviation uses minimum point-to-segment distance.

### `alerts`

Plain PostgreSQL table on the TimescaleDB instance; not a hypertable.

| Column | Type | Nullable | Description |
| --- | --- | --- | --- |
| `alert_id` | TEXT PK | No | Deterministic logical alert identity; rules below |
| `entity_id` | TEXT | No | Primary entity |
| `counterparty_entity_id` | TEXT | Yes | Second entity for proximity/composite alerts |
| `entity_type` | TEXT | No | `aircraft` or `vessel` |
| `alert_type` | TEXT | No | `SIGNAL_LOSS`, `ROUTE_DEVIATION`, `UNSCHEDULED_PROXIMITY`, `COMPOSITE` |
| `priority` | TEXT | No | `STANDARD` or `ELEVATED` |
| `status` | TEXT | No | `NEW`, `ACKNOWLEDGED`, `RESOLVED`, `SUPERSEDED` |
| `superseded_by` | TEXT | Yes | FK to composite `alert_id` when system-superseded |
| `payload` | JSONB | No | Type-specific evidence |
| `detected_at` | TIMESTAMPTZ | No | Persistence/detection processing timestamp |
| `updated_at` | TIMESTAMPTZ | No | Last lifecycle update |
| `acknowledged_at` | TIMESTAMPTZ | Yes | Operator acknowledgement time |
| `acknowledged_by` | UUID | Yes | FK to user |
| `resolved_at` | TIMESTAMPTZ | Yes | Resolution time |
| `resolved_by` | UUID | Yes | FK to user |

Canonical deterministic `alert_id` rules:

```text
SIGNAL_LOSS
{entity_id}:SIGNAL_LOSS:{dark_since_ms}

ROUTE_DEVIATION
{entity_id}:ROUTE_DEVIATION:{episode_start_ms}

UNSCHEDULED_PROXIMITY
{pair_key}:UNSCHEDULED_PROXIMITY:{episode_start_ms}

COMPOSITE
{pair_key}:COMPOSITE:{dark_since_ms}
```

`pair_key = min(entity_a_id, entity_b_id):max(entity_a_id, entity_b_id)`.

These type-specific identities avoid collisions between two simultaneous pair incidents involving the same primary entity.

`priority` mapping by `alert_type`: `SIGNAL_LOSS` and `UNSCHEDULED_PROXIMITY` are `STANDARD`. `COMPOSITE` is `ELEVATED`, reflecting US-06's intent that correlating a signal-loss episode with an unscheduled-proximity episode produces "one elevated incident," not two disconnected standard-priority signals. `ROUTE_DEVIATION` is not yet implemented; its priority is undecided.

Lifecycle:

- operator path: `NEW → ACKNOWLEDGED → RESOLVED`, with optional `NEW → RESOLVED`;
- system composite replacement: `NEW → SUPERSEDED` or `ACKNOWLEDGED → SUPERSEDED`;
- `RESOLVED` and `SUPERSEDED` are terminal;
- a recurring anomaly creates a new deterministic episode/window identity rather than reopening a terminal row.

When a COMPOSITE is persisted, the API atomically inserts it and marks referenced active individual alerts (`NEW` or `ACKNOWLEDGED`) `SUPERSEDED`. A resolved alert is not retroactively superseded.

**Supersession must converge regardless of which message the API consumes first.** The Alert Evaluator's signal-loss scan writes `alert-state` to Redis before its own `SIGNAL_LOSS` Kafka publish completes, and the proximity-candidate consumer runs concurrently: it is possible for a `COMPOSITE` referencing a `SIGNAL_LOSS` `alert_id` to reach the API before that `SIGNAL_LOSS` message does (the producer has no idempotence guarantee on send-order). A plain `UPDATE ... WHERE alert_id = X` in that case affects zero rows, and the late-arriving `SIGNAL_LOSS` would then persist as `NEW` forever, never superseded. Whichever order the two messages arrive in, the API must reach the identical durable state: the referenced alert row exists with `status = SUPERSEDED` and `superseded_by` set to the composite's `alert_id`. Resolved below (Pre-CP5B), before any CP5B code depends on it.

### Pre-CP5B: composite supersession convergence protocol

**a. A small additive table records a supersession that has to wait for its target to exist.** Placeholder rows were rejected: `alerts` has several `NOT NULL` canonical columns (`detected_at` chief among them) that a `COMPOSITE` message cannot honestly supply for the individual alert it references, `COMPOSITE`'s own `detected_at_ms` is a different processing-time event than the original alert's detection, and `SIGNAL_LOSS`'s own payload evidence (callsign, last-known position) isn't carried in `COMPOSITE`'s payload at all. A placeholder would either fabricate these or require a fragile conditional upsert to backfill them later, and if the real message never arrives, the fabricated row is wrong forever rather than simply absent. A cross-system (Redis) pending marker was also rejected: it can't share a transaction with the composite's own Postgres commit, so a crash between the two systems reopens the exact convergence bug this exists to close. The accepted design is one small, purely additive table, consulted only on the side that needs to wait:

```sql
CREATE TABLE IF NOT EXISTS pending_alert_supersessions (
    referenced_alert_id TEXT PRIMARY KEY,
    composite_alert_id  TEXT NOT NULL REFERENCES alerts (alert_id),
    created_at           TIMESTAMPTZ NOT NULL
);
```

`referenced_alert_id` is deliberately not an FK (the row it names doesn't exist yet by definition); `composite_alert_id` is, and is always satisfiable, the composite row is inserted earlier in the same transaction that creates a pending entry referencing it.

**b. A per-`alert_id` transaction-scoped advisory lock closes the race Postgres row-locking cannot.** Row-level locking only protects rows that exist; the out-of-order case is defined by the referenced row not existing yet, so `UPDATE` (0 rows) followed by a existence-check `SELECT` is not serialized against a concurrent transaction inserting that exact row in between, and can independently reach the same "doesn't exist yet" conclusion that's already stale by the time it's acted on. Every alert persisted, individual or composite, acquires `pg_advisory_xact_lock($namespace, hashtext($alert_id))` on its own `alert_id` before reading or writing anything about that id; a `COMPOSITE` message additionally acquires one per entry in `supersedes_alert_ids`, sorted, before touching any of them (the standard deadlock-avoidance rule for a transaction that takes more than one lock). The lock and every statement it protects run on one checked-out `pg.PoolClient`, never the bare pool, an advisory-transaction lock is scoped to the connection that took it. The lock releases automatically at `COMMIT`/`ROLLBACK`, no manual unlock, no leak on crash.

```ts
const ALERT_SUPERSESSION_LOCK_NAMESPACE = 1001; // reserves this int4 space; nothing else in the codebase uses pg_advisory_xact_lock today
```

**c. Transaction sequence, referenced alert already exists when `COMPOSITE` arrives:**

```text
BEGIN;
  lock every referenced alert_id (sorted);
  INSERT composite ON CONFLICT (alert_id) DO NOTHING RETURNING *;   -- capture row
  UPDATE alerts SET status='SUPERSEDED', superseded_by=$composite_id, updated_at=now()
    WHERE alert_id=$ref_id AND status IN ('NEW','ACKNOWLEDGED')
    RETURNING *;                                                     -- capture row if matched
  -- 0 rows: SELECT * FROM alerts WHERE alert_id=$ref_id; see (e) for what this branch does
COMMIT;
publish the captured composite row, then each captured referenced row;
```

**Transaction sequence, `COMPOSITE` arrives before the referenced alert exists:**

```text
BEGIN;
  lock every referenced alert_id (sorted);
  INSERT composite ON CONFLICT (alert_id) DO NOTHING RETURNING *;    -- capture row
  UPDATE ... WHERE alert_id=$ref_id AND status IN ('NEW','ACKNOWLEDGED') RETURNING *;
  -- 0 rows -> SELECT * FROM alerts WHERE alert_id=$ref_id;
  --   absent -> conflict-aware pending upsert, see (e); nothing to publish for this id yet
COMMIT;
publish the captured composite row; publish any referenced rows already SUPERSEDED by it;

-- the referenced alert, arriving later (possibly much later, or redelivered):
BEGIN;
  lock this alert_id;
  DELETE FROM pending_alert_supersessions WHERE referenced_alert_id=$alert_id
    RETURNING composite_alert_id;
  -- row -> INSERT ... status='SUPERSEDED', superseded_by=$returned_composite_id ...
  --   ON CONFLICT (alert_id) DO NOTHING RETURNING *;  (fallback SELECT on redelivery)
  -- no row -> INSERT ... status='NEW' (the message's own status) ... RETURNING *;  (fallback SELECT)
COMMIT;
publish the captured row's actual persisted state, never the raw incoming Kafka bytes,
  a pending-consumed row is SUPERSEDED, not the NEW the message carried;
```

**d. Post-commit publication republishes canonical current state on every delivery, never "what this attempt changed."** A `COMPOSITE` message can require multiple Redis publishes after one DB commit (the composite row, each referenced row it supersedes); if publish 1 succeeds and publish 2 throws, the Kafka offset never commits and the message redelivers, but the redelivered transaction is now idempotent by construction and may mutate nothing at all, so deriving "what to publish" from "what this attempt mutated" would silently drop the un-published event forever. Every row this message concerns is instead captured (via `RETURNING *`, or a plain `SELECT` when it already existed as-is) inside the transaction, before `COMMIT`, while its lock is still held; after commit, every captured row is published unconditionally, on every delivery attempt, whether or not that attempt itself changed anything. Duplicates are expected and already accepted (`DATA_MODEL.md`'s WebSocket contract requires idempotent-by-`alert_id` rendering); a lost lifecycle event is not.

**e. Ownership conflicts are idempotent only when the existing owner matches; a different owner is an invariant failure, symmetrically at both the pending-row level and the row level.** Two different composites both claiming to supersede the same individual alert should be structurally impossible (a signal-loss episode can be claimed by at most one composite, per the Alert Evaluator's own claim protocol), so encountering it is an invariant violation, not a routine outcome, the same fail-closed posture as `CandidateDecisionConflictError`/`CompositeFinalizeInvariantError` upstream:

```sql
-- pending-row conflict:
INSERT INTO pending_alert_supersessions (referenced_alert_id, composite_alert_id, created_at)
VALUES ($1, $2, now())
ON CONFLICT (referenced_alert_id) DO UPDATE
  SET composite_alert_id = EXCLUDED.composite_alert_id
  WHERE pending_alert_supersessions.composite_alert_id = EXCLUDED.composite_alert_id
RETURNING referenced_alert_id;
-- a row comes back: fresh insert, or a redelivery of the same composite (idempotent).
-- no row comes back: a different composite_alert_id is already pending for this id, throw.
```

The same rule applies one step later, at the row itself: when the existence-check `SELECT` in (c) finds the referenced alert already `SUPERSEDED`, it must also compare `superseded_by`. Equal to this composite's own `alert_id`: idempotent replay, capture and publish as usual. Set to a *different* `alert_id`: the same invariant violation as the pending-row case, throw and roll back (releasing the locks) rather than silently leaving the earlier, conflicting supersession in place or overwriting it. `RESOLVED` rows are never subject to this check at all, they are excluded by the `UPDATE`'s own `WHERE status IN ('NEW','ACKNOWLEDGED')` and are never touched or published by composite processing, terminal per the existing lifecycle rule above.

**f. CP5B guarantees durable DB convergence and replay-safe publication; it does not guarantee ordering between Redis publishes originating from different, separately committed API transactions.** A `SIGNAL_LOSS` message's own transaction commits and publishes independently of a later `COMPOSITE` message's transaction; the two publishes happen on their own schedules, and Redis pub/sub plus WebSocket delivery carry no cross-message ordering guarantee (`DATA_MODEL.md`'s existing at-least-once, duplicate-safe delivery model already accepts this for a single alert's own lifecycle, this extends it across two related alerts). A client can therefore observe a stale `NEW` event for the referenced alert arriving *after* it has already rendered that same alert as `SUPERSEDED`, if the two underlying transactions' publishes happen to interleave that way. **CP5C must make alert lifecycle merging monotonic**: a client-side merge that never lets a terminal or superseded status regress on top of an already-rendered later state, keyed by `alert_id` and comparing the incoming status against what's currently rendered, not simply "last write wins" by arrival order. `GET /alerts` (REST) remains the durable reconciliation source of truth; WebSocket delivery is a live, best-effort stream on top of it, not the thing a client should trust for absolute ordering.

Indexes:

- `(entity_id, detected_at DESC)`;
- `(counterparty_entity_id, detected_at DESC)`;
- `(status, detected_at DESC)`;
- `(alert_type, detected_at DESC)`.

### `users`

| Column | Type | Nullable |
| --- | --- | --- |
| `user_id` | UUID PK | No |
| `google_sub` | TEXT UNIQUE | No |
| `email` | TEXT | No |
| `last_login_at` | TIMESTAMPTZ | No |
| `created_at` | TIMESTAMPTZ | No |

### `user_workspaces`

| Column | Type | Nullable |
| --- | --- | --- |
| `user_id` | UUID PK/FK | No |
| `scope` | JSONB | No |
| `updated_at` | TIMESTAMPTZ | No |

Canonical scope contains geographic bounds plus `entity_types` and `alert_types` filters.

---

## Neo4j

### Node `Entity`

Properties: `id` (unique), `type`, optional `name`.

### Edge `PROXIMITY_EVENT`

One edge per proximity episode.

| Property | Description |
| --- | --- |
| `idempotency_key` | `{pair_key}:{episode_start_ms}` |
| `episode_start_ms` | Source event time for encounter start |
| `last_seen_ms` | Latest source event time confirming encounter |
| `min_distance_metres` | Closest observed distance |
| `lat`, `lon` | Midpoint at detection |
| `distance_at_detection` | Distance at episode start |

Correlation Worker writes with `MERGE` on `idempotency_key`.

### Edge `KNOWN_ASSOCIATE`

Represents a pre-existing expected relationship such as same fleet or scheduled pairing.

The **Correlation Worker** checks this edge before publishing `proximity.candidates`. Known-associate encounters may be recorded as evidence, but they do not become unscheduled-proximity candidates.

Properties include `established_at` and `relationship_type`. v1 seeds these manually; no runtime service creates them.

The Alert Evaluator does not read Neo4j in the current v1 contract.

---

## Redis

### `entity:live:{entity_id}` — hash

Fields written by the Position Consumer (CP6):

```text
lat               lon               altitude_m        entity_type
last_seen_ms      live_geo_cell     speed_mps         course_deg
heading_deg       vertical_rate_mps on_ground         navigation_status
callsign          entity_subtype    provider
```

- Writer: Position Consumer.
- Readers: Alert Evaluator, Correlation Worker, API.
- TTL: 24h safety net, deliberately longer than signal-loss timing.
- Timestamp guard: stale/out-of-order telemetry cannot replace a newer `last_seen_ms` state. All fields update together under the same guard.

### `geo-cell:{h3_cell_id}` — sorted set

Member=`entity_id`, score=`last_seen_ms`.

Position Consumer update sequence:

1. read previous `live_geo_cell` before replacing the live hash;
2. if the cell changed, `ZREM geo-cell:{old_cell} {entity_id}`;
3. `ZADD geo-cell:{new_cell} {last_seen_ms} {entity_id}`.

Correlation Worker reads the incoming entity cell plus computed k-ring using `ZRANGEBYSCORE` with a freshness lower bound. No TTL is required; stale members age out logically by score.

### `alert-state:{entity_id}` — hash

Fields:

- `dark_since_ms`;
- `signal_loss_alert_id`;
- `composite_issued` (`0|1`);
- `composite_claim_candidate_id` — the claiming proximity candidate's identity (`{pair_key}:{episode_start_ms}`), or absent. See [Composite claim and decision protocol](#composite-claim-and-decision-protocol).

Writer/reader: Alert Evaluator only, for the `composite_*` fields — see the protocol section for why plain unconditional writes are no longer sufficient once these fields exist. No TTL. Position Consumer transitions this into `recent-loss` when the entity resumes (see below).

### `recent-loss:{entity_id}` — hash

Fields: `dark_since_ms`, `resumed_at_ms`, `signal_loss_alert_id`, `composite_issued` (`0|1`), `composite_claim_candidate_id`.

- Writer: Position Consumer on first accepted resume position (`dark_since_ms`, `resumed_at_ms`, `signal_loss_alert_id`, carrying forward whatever `composite_issued`/`composite_claim_candidate_id` `alert-state` already held — see [Composite claim and decision protocol](#composite-claim-and-decision-protocol) for why this handoff cannot be a plain read-then-write once these fields are mutable); Alert Evaluator for `composite_*` field updates thereafter.
- Reader/consumer: Alert Evaluator.
- TTL: `COMPOSITE_CORRELATION_WINDOW_MS`, counted from `resumed_at_ms`. This is a **retention bound**, not the eligibility window itself — see [Composite eligibility rule](#composite-eligibility-rule). `resumed_at_ms` is evidence for the COMPOSITE payload; it is not an input to the eligibility computation.

A qualifying composite sets `composite_issued=1` on whichever hash currently holds the episode. **The key is not deleted.** Existing TTL retention removes it naturally, same as an unconsumed episode — see [Composite claim and decision protocol](#composite-claim-and-decision-protocol) for why eager deletion on consumption is a correctness bug, not just untidy state.

### Composite eligibility rule

Given a `proximity.candidates` event, the Alert Evaluator checks both pair members independently for a qualifying signal-loss episode — active (`alert-state:{entity_id}`) or recently closed (`recent-loss:{entity_id}`). Both hashes carry `dark_since_ms`; eligibility is computed from that field alone, with **one formula for both representations**:

```text
gap_ms = candidate.episode_start_ms - loss.dark_since_ms

qualifies iff:
0 <= gap_ms <= COMPOSITE_CORRELATION_WINDOW_MS
```

`recent-loss`'s Redis TTL (`COMPOSITE_CORRELATION_WINDOW_MS` counted from `resumed_at_ms`) is retention, not this eligibility check. Because the Position Consumer only creates `recent-loss` on an accepted resume position, `resumed_at_ms >= dark_since_ms` always, so the key's expiry (`resumed_at_ms + COMPOSITE_CORRELATION_WINDOW_MS`) is always at or after the true eligibility deadline (`dark_since_ms + COMPOSITE_CORRELATION_WINDOW_MS`) — a genuinely eligible candidate can never find the key already expired. The reverse is not guaranteed: after a long dark interval, `recent-loss` can still exist past its true gap-based deadline. Key existence alone does not mean the candidate qualifies; the `dark_since_ms` comparison above is the sole authority.

**Both entities qualifying.** A signal-loss episode on either pair member may independently qualify. One composite anchors to exactly one `dark_since_ms`, so when both members qualify, exactly one episode is selected by deterministic temporal tie-break:

```text
winner = the qualifying episode with the smallest gap_ms;
         ties broken by the lexicographically smaller entity_id
```

Only the selected episode is consumed (`composite_issued` set to `1`; the underlying key is not deleted — see below). The non-selected member's signal-loss episode remains independently active/recent and may still qualify for a different composite — it is not swallowed by losing this tie-break.

This section describes CP2's **read-only eligibility snapshot** only — deciding which episode, if any, currently looks eligible. It says nothing about how that snapshot is safely turned into a durable claim and a published alert without racing a concurrent claimant or losing the composite on crash. That is the [Composite claim and decision protocol](#composite-claim-and-decision-protocol) below.

### Composite claim and decision protocol

CP2's eligibility resolution (above) is a snapshot, not a lock: Redis state can change between resolving eligibility and acting on it. Turning a resolved winner into a durably claimed, published `COMPOSITE` — without losing it on crash, without a concurrent claimant stealing it, and without Kafka redelivery silently reclassifying the same candidate — requires three additional pieces of state, resolved as a design decision (Pre-CP3A) before any of them were implemented.

**Canonical claim identity is the proximity episode, not the pair.** A `pair_key` alone is insufficient: the same pair can produce multiple distinct proximity episodes over time (`{pair_key}:{episode_start_ms}` is already the proximity episode's own identity, per [Neo4j's `PROXIMITY_EVENT`](#edge-proximity_event) and the Correlation Worker's episode state). If a loss-episode claim were keyed only by `pair_key`, a second, later, genuinely different proximity episode for the same pair would be indistinguishable from a Kafka redelivery of the first. Every claim in this protocol is therefore identified by:

```text
candidate_id = {pair_key}:{episode_start_ms}
```

**1. Loss-episode claim** — "which candidate, if any, won this signal-loss episode?" Lives on whichever of `alert-state`/`recent-loss` currently holds the episode, as `composite_claim_candidate_id` (a `candidate_id`, or absent) alongside the existing `composite_issued` flag:

```text
CLAIM (Lua, atomic):
  revalidate dark_since_ms still matches the resolved snapshot
  composite_issued == '0'
  composite_claim_candidate_id is empty OR equals this candidate_id
  -> atomically set composite_claim_candidate_id = candidate_id

FINALIZE (Lua, atomic, only after Kafka publish confirmed):
  composite_claim_candidate_id still equals this candidate_id
  -> set composite_issued = '1'
```

A claim is fenced against a *different* candidate (a different pair racing for the same loss episode), but is resumable by the *same* candidate on redelivery — CLAIM is idempotent for its own `candidate_id`, not a one-shot gate. This is why CLAIM must run before publish (fencing) and FINALIZE only after publish is confirmed (crash-safety): a naive "claim then publish" design permanently loses the composite if the process crashes between the two, because the claim burns the episode's only chance with no retry path — unlike signal-loss (which the docs already accept losing one alert to on crash) and unlike proximity candidates (which get a "next ping" retry that a one-shot `proximity.candidates` delivery has no equivalent of). **Implemented as of CP3B** — see [`composite-episode-claim`](implementation/phase-06-composite-correlation/concepts/composite-episode-claim/composite-episode-claim.md) for `claimCompositeEpisode`/`finalizeCompositeEpisode`, real concurrent-claimer proof, and the representation-independent search across `alert-state`/`recent-loss`. Not yet wired into `handleProximityCandidate` — that is CP5A.

**2. `alert-state` → `recent-loss` handoff must become one atomic transfer, not read-then-write.** CP1's handoff (`recent-loss-handoff` concept) was safe when the hash held only static episode evidence. Once `composite_claim_candidate_id`/`composite_issued` are mutable, a `HGETALL` read followed by a separate `MULTI` write racing an Alert Evaluator CLAIM can silently erase a claim recorded between the read and the `EXEC`:

| Step | Actor | Operation | Result |
| --- | --- | --- | --- |
| 1 | Position Consumer | `HGETALL alert-state` | reads `composite_claim_candidate_id` empty |
| 2 | Alert Evaluator | `CLAIM` (Lua) | `alert-state.composite_claim_candidate_id = X` |
| 3 | Position Consumer | `MULTI(HSET recent-loss from step 1, ..., DEL alert-state) EXEC` | `recent-loss` written from the stale step-1 snapshot — candidate `X`'s claim, written at step 2, never arrives |

The handoff must become a single Lua script that reads current field values and writes them into `recent-loss` (including whatever `composite_*` fields are present at that instant) inside the same atomic step that deletes `alert-state`, so Redis's own command serialization — not application-level timing — determines whether a concurrent CLAIM lands before or after the handoff. **This is a real consequence of adding mutable coordination fields to these hashes, not a refactor** — CP1's original `MULTI`/`EXEC` (`HSET` + `PEXPIRE` + `DEL` from already-read values) was not sufficient once claim fields exist. **Implemented as of CP3A** — see [`atomic-signal-loss-handoff`](implementation/phase-06-composite-correlation/concepts/atomic-signal-loss-handoff/atomic-signal-loss-handoff.md) for the Lua script, its sequence diagram of this exact race, and real-Redis proof that a seeded claim survives the transfer unchanged.

**3. Candidate decision record** — "what did we decide for this exact proximity candidate?", independent of loss-episode state. This closes a second, symmetric redelivery bug beyond the one loss-episode claiming alone fixes:

```text
COMPOSITE published, crash before the proximity.candidates offset commits
  -> redelivery finds the loss episode already composite_issued=1 (or claimed by this candidate_id)
  -> without a decision record, naive re-resolution says "not eligible"
  -> wrongly publishes UNSCHEDULED_PROXIMITY alongside the already-published COMPOSITE

UNSCHEDULED_PROXIMITY published because no loss episode existed yet,
crash before the proximity.candidates offset commits
  -> the signal-loss scan runs in between, opening alert-state for the same entity
  -> redelivery now finds a qualifying episode
  -> wrongly publishes COMPOSITE too
```

Both directions produce two different alert types/IDs for one proximity candidate — not a harmless idempotent duplicate, since `{pair_key}:UNSCHEDULED_PROXIMITY:{episode_start_ms}` and `{pair_key}:COMPOSITE:{dark_since_ms}` are deliberately different deterministic identities and the API has no way to know they refer to the same encounter.

```text
alert-decision:{pair_key}:{episode_start_ms} -- hash

Fields:
  decision              COMPOSITE | UNSCHEDULED_PROXIMITY
  selected_entity_id    (COMPOSITE only)
  dark_since_ms         (COMPOSITE only)
  signal_loss_alert_id  (COMPOSITE only)
  loss_source           ACTIVE | RECENT (COMPOSITE only)
  resumed_at_ms         (COMPOSITE only, RECENT source)
```

Once a decision record exists for a `proximity.candidates` message, that message's processing **replays the recorded decision** rather than re-resolving CP2 against current (possibly changed) Redis state. Replaying a `COMPOSITE` decision is not just rebuilding and republishing the alert: it also calls FINALIZE for the decision's selected episode (idempotent if some earlier attempt already finalized it, required if it didn't), the same as a fresh `COMPOSITE` decision does. Lifecycle for CP5A: created before the Kafka publish it protects; **retained**, not deleted, after that message's own Kafka offset commits. See Pre-CP5A(d) for why immediate post-commit deletion was rejected and what CP5A does instead.

```text
create decision -> publish output -> finalize claim -> commit input offset
(no deletion step in CP5A's own runtime path, see Pre-CP5A(d))
```

A crash before the offset commits leaves the decision record in place, replay-safe. Once the offset commits, CP5A leaves the record exactly where it is: it is never consulted again for this `candidate_id` under normal operation, but nothing in CP5A actively removes it either. Reclamation is a separate, deferred concern, not part of this checkpoint; see Pre-CP5A(d).

**`readCandidateDecision`/`writeCandidateDecisionIfAbsent` implemented as of CP3C**, see [`candidate-decision-record`](implementation/phase-06-composite-correlation/concepts/candidate-decision-record/candidate-decision-record.md) for the write-once atomic create, real concurrent-conflicting-write proof, and why a conflict throws rather than silently overwriting. Only wiring this into `handleProximityCandidate` is unbuilt; that is CP5A's job, resolved below. Deletion is explicitly out of CP5A's scope, not merely unbuilt, see Pre-CP5A(d).

**Pre-CP5A resolutions** (design decided here, before CP5A's code depends on them; not yet implemented):

**a. Any CLAIM failure decides `UNSCHEDULED_PROXIMITY`, with no fallback to the other pair member.** `claimCompositeEpisode` collapses three distinct Lua outcomes (`CLAIMED_BY_OTHER`, `ALREADY_ISSUED`, `NO_EPISODE`) into a single `false`, and all three are reachable here even though CP2 already confirmed eligibility, because CP2's read and CLAIM's atomic check are two separate Redis round trips: CP2 is a snapshot, not a lock. Whichever of the three reasons caused the failure, the outcome is the same: this candidate resolves as `UNSCHEDULED_PROXIMITY`. It does not retry against the non-selected pair member's episode, even if that episode independently qualifies. The deterministic tie-break already picked a single winner over a Redis snapshot, and re-deciding after losing a race would make the final correlation depend on concurrency timing rather than the accepted source-time tie-break. `claimCompositeEpisode` does not need to expose which of the three reasons applied; all three collapse to this identical outcome.

**b. A `CandidateDecisionConflictError` reachable after this process's own CLAIM already mutated Redis must release that claim, then adopt and execute the canonical decision, never simply propagate.** This is reachable under the same leader-overlap ADR-005 already accepts (worked race below), not merely hypothetical. When `writeCandidateDecisionIfAbsent` throws, the caller must: (1) if it holds a live, unfinalized claim on some episode as a result of its own CP2 resolution, release it; (2) adopt `err.existing` (the canonical decision the error already carries) and process it exactly like the standard existing-decision replay path: build the alert from `err.existing`, publish, and if `err.existing.decision === 'COMPOSITE'`, **call FINALIZE for that decision's episode**, even though this process was not the one that originally wrote it. Claim and finalize ownership is keyed by `candidate_id`, not by which process instance calls the Lua: both processes racing here are handling the same Kafka message and therefore the same `candidate_id`, so FINALIZE is exactly as legitimate for the adopting process as it would have been for the original writer, idempotent either way if the original writer already finalized it too.

Release is a new representation-independent Lua primitive, symmetric to CLAIM/FINALIZE, searching both `alert-state`/`recent-loss` the same way:

```text
RELEASE (Lua, atomic, best-effort cleanup after a lost decision race):
  find the episode matching this expected dark_since_ms, same
    representation-independent search as CLAIM/FINALIZE
  if no matching episode is found -> nothing to release, treat as success
  if composite_issued == '1' -> leave untouched, report (should not happen:
    this candidate never reached FINALIZE on this episode)
  if composite_claim_candidate_id != candidate_id -> leave untouched, report
    (should not happen: this candidate is the one that placed this claim)
  otherwise -> atomically clear composite_claim_candidate_id back to empty
```

All three preconditions (matching `dark_since_ms`, `composite_issued == '0'`, `composite_claim_candidate_id == candidate_id`) must hold before RELEASE clears anything; it never overwrites a claim it doesn't recognize as its own caller's. Its own result does not gate whether the caller proceeds to adopt `err.existing`: RELEASE is best-effort cleanup of this process's own stray state, not a precondition for convergence, so the caller adopts and processes `err.existing` regardless of what RELEASE reports, logging a warning if RELEASE did not actually clear anything.

*Worked race* (replaces two earlier, invalid versions of this example: the first had a third candidate FINALIZE episode A before CLAIM(A) subsequently succeeded, and the second had a third candidate FINALIZE episode A after P1 already held A's claim; both are impossible, since CLAIM re-validates `composite_issued` in real time and a held claim fences every other `candidate_id` from ever completing a claim-to-finalize cycle against the same episode). This version needs no third candidate at all, only `recent-loss`'s own TTL:

- P1 processes `candidate_id = X` while `recent-loss:A` still exists. CP2 selects A (the `RECENT`-source episode). P1 calls `CLAIM(A, X)`, which succeeds: nobody else is contesting A, so no other `candidate_id` is involved. P1 is then delayed, not crashed, before it reaches its own `writeCandidateDecisionIfAbsent` call: a GC pause, a slow Redis round trip, or exactly the kind of stall that causes a lease to be lost in the first place.
- While P1 is delayed, `recent-loss:A`'s TTL elapses and the key expires.
- P2 processes the same `candidate_id X` (redelivered during the leader-overlap window ADR-005 already accepts, while P1 is still alive and has not yet written anything). P2's CP2 resolution no longer finds A at all (the key is gone), so it resolves against whatever else qualifies: entity B (`CLAIM(B, X)` succeeds, nobody contests B either, giving `COMPOSITE/B`) or nothing (`UNSCHEDULED_PROXIMITY/X`).
- P1 (still holding its live, unfinalized claim on A) now reaches its own write: `COMPOSITE/A`. Whichever of P1's `COMPOSITE/A` or P2's decision reaches `writeCandidateDecisionIfAbsent` second hits a genuine conflict: two different decisions for the same `candidate_id X`, neither one ever having attempted a second CLAIM on an already-issued episode.

The loser here is always P1 in this construction (it is the one delayed), and it always holds a live, unfinalized claim on A (`alert-state:A`/`recent-loss:A`'s claim fields, or, having expired, possibly nothing left to release at all if the key vanished entirely before P1 ever attempts the release). Per (b), P1 must attempt to release its claim on A regardless (the release primitive is a no-op if the key is already gone) before adopting whichever decision won, and must FINALIZE that adopted decision if it is `COMPOSITE`.

**c. FINALIZE's `NO_EPISODE` outcome is representation-independent, not limited to decisions recorded as `RECENT`.** A decision frozen with `loss_source='ACTIVE'` (`resumed_at_ms=null`) at write time can still resolve to `NO_EPISODE` at FINALIZE time: the entity can resume between decision-write and FINALIZE, CP3A's atomic handoff moves the claim from `alert-state` to `recent-loss` (preserving `composite_claim_candidate_id`/`composite_issued`/`dark_since_ms` byte for byte), and if FINALIZE is delayed long enough after that handoff (a crash-and-redelivery gap exceeding `recent-loss`'s TTL), the key can expire before FINALIZE ever runs. What matters for `NO_EPISODE` is the representation FINALIZE actually finds *at call time* (searching both `alert-state` and `recent-loss`, exactly as CLAIM already does), never the `loss_source` frozen in the decision record, which only describes the representation true *at decision-write time*.

FINALIZE's caller must handle its three outcomes differently, not collapse them to `boolean`:

```text
FINALIZE SUCCESS      -> proceed normally
FINALIZE NO_EPISODE   -> log a warning; still safe to commit. The key is gone
                          entirely, so no other candidate can reuse or corrupt
                          it either; the alert already published stands as the
                          only evidence.
FINALIZE NOT_CLAIMED  -> invariant violation; throw, do not commit. The episode
                          still exists but ownership no longer matches this
                          candidate_id, so silently proceeding risks permitting
                          a second composite over the same episode.
```

**d. Immediate post-commit deletion is not safe under the leader-overlap ADR-005 already accepts, and a TTL does not fix it if the key is still deleted explicitly.** Consider: P1 and P2 both read decision `X` as absent (the same overlap as the worked race above). P1 races ahead through write, publish, finalize, commit, and *deletes* decision `X`. P2, already in flight and merely slower (not crashed, not stale, just behind), reaches its own `writeCandidateDecisionIfAbsent(X, ...)` call *after* P1's delete. `writeCandidateDecisionIfAbsent`'s Lua checks `EXISTS` first: if the key is genuinely gone, P2's write is treated as a fresh creation, not a redelivery replay and not a conflict, since there is nothing left to compare against. If P2's independently-computed decision differs from P1's (exactly the divergence in the worked race above), P2 proceeds to build and publish a second, differently-identified alert for the same encounter: the exact double-alert corruption this whole mechanism exists to prevent, now reachable through premature deletion rather than through Kafka redelivery. An earlier draft of this resolution tried to fix this with a TTL while still deleting the key immediately after commit in the common case; that does not work. A TTL only helps if the key is still *present* for P2 to find; explicitly deleting it the moment P1 commits removes it immediately regardless of what TTL was set, so "best-effort delete, TTL as backstop" gives zero actual protection whenever the delete succeeds, which is the common case, not the rare one.

**CP5A's actual resolution: do not delete `alert-decision:*` records at all, and do not add a TTL either.** Once an offset commits, CP5A leaves the record in place, permanently, for this checkpoint. This keeps the fix entirely within CP5A's own scope (Alert Evaluator only) and avoids a second, half-solved dependency: introducing a TTL whose correctness would depend on `proximity.candidates`' own topic `retention.ms`, which is not currently an explicit, documented value, and which Kafka enforces at the segment level rather than as an exact per-record expiry, meaning any TTL derived from it today would be a guess dressed up as a bound. The accepted trade-off for CP5A is that `alert-decision:*` keys accumulate in Redis without bound over the service's lifetime, one per `proximity.candidates` message ever processed. That is a known, deliberate limitation of this checkpoint, not an oversight: reclaiming these records safely is deferred to a later checkpoint (GC or production-hardening scope), once `proximity.candidates` has an explicit, documented retention/replay contract this record's own lifetime can be soundly bounded against. `deleteCandidateDecision` is not part of CP5A's implementation surface.

**Invariants established by this protocol:**

1. One loss episode can be claimed by at most one canonical proximity candidate (`{pair_key}:{episode_start_ms}`), never a bare `pair_key`. **Implemented (CP3B)** — proven with real concurrent different-candidate claims against Redis; exactly one wins.
2. A candidate's alert-type decision is sticky across Kafka redelivery, in both directions (COMPOSITE cannot flip to UNSCHEDULED_PROXIMITY or vice versa on replay). **Implemented (CP3C)** — the write-once record and its conflict detection exist and are tested; not yet wired into the redelivery path itself, since nothing calls `handleProximityCandidate` with this logic yet (CP5A).
3. The `alert-state` → `recent-loss` handoff preserves claim/finalize state atomically — a single Lua transfer, not a read-then-`MULTI`-write. **Implemented (CP3A).**
4. `recent-loss`'s TTL governs eligibility retention, never Kafka replay memory. **Implemented**: CLAIM and FINALIZE never touch any TTL, and neither does the decision record itself. For CP5A, the decision record is never deleted and carries no TTL, it is retained indefinitely once written, and reclaiming it is explicitly out of scope until `proximity.candidates` has a documented retention/replay contract (Pre-CP5A(d)).
5. The same candidate may resume its own pending claim; a different candidate may never steal it. **Implemented (CP3B)** — proven directly: the winning candidate's own retry succeeds, a losing candidate's finalize attempt fails.
6. Any CLAIM failure resolves the candidate as `UNSCHEDULED_PROXIMITY`, never a retry against the other pair member (Pre-CP5A(a)); a decision-write conflict after a successful CLAIM releases that claim and adopts the canonical decision, finalizing it if `COMPOSITE` (Pre-CP5A(b)); FINALIZE's `NO_EPISODE`/`NOT_CLAIMED` outcomes are handled differently, not collapsed to `boolean` (Pre-CP5A(c)); decision records are retained indefinitely after commit, never deleted, in CP5A, reclamation deferred until `proximity.candidates` has an explicit retention contract (Pre-CP5A(d)). **Design decided (Pre-CP5A); not yet implemented.**

### `deviation-state:{entity_id}` — hash

Fields: `count`, `episode_start_ms`, `last_processed_ms`, `alert_emitted`.

Writer/reader: Alert Evaluator. Safety TTL `DEVIATION_STATE_TTL_MS`; explicit delete on `IN_RANGE`.

### `proximity-episode:{pair_key}` — hash

Fields: `episode_start_ms`, `last_seen_ms`, optional `candidate_published`.

Writer/reader: Correlation Worker. TTL `PROXIMITY_EPISODE_GAP_MS`.

- unscheduled pair: `candidate_published=0` before Kafka publish and `1` after success;
- known associate: field omitted because no candidate is ever published.

### `alert-evaluator:leader` — string

Value=`instance_id`.

Acquire: `SET NX PX`. Renewal/release must compare current ownership before `PEXPIRE` / `DEL`.

### `position-updates` — pub/sub

Publisher: Position Consumer. Subscribers: API instances.

Payload (subset of `position.normalized` — only what the API needs to push a map update):

| Field | Type | Notes |
| --- | --- | --- |
| `entity_id` | string | |
| `entity_type` | string | |
| `timestamp_ms` | number | source event time |
| `lat` | number | |
| `lon` | number | |
| `altitude_m` | number \| null | |
| `speed_mps` | number \| null | |
| `course_deg` | number \| null | |
| `callsign` | string \| null | |
| `live_geo_cell` | string | H3 at `LIVE_H3_RESOLUTION`; used for viewport cell filtering |

Published only on accepted live-state writes. Stale/equal-timestamp events are not published. Full canonical fields are in `position.normalized` for services that need them.

### `alert-events` — pub/sub

Publisher: API. Subscribers: all API instances.

Canonical envelope:

```json
{
  "type": "ALERT_CREATED | ALERT_STATUS_CHANGED | ALERT_SUPERSEDED",
  "payload": {}
}
```

Redis pub/sub and WebSocket delivery are at-least-once from the client's perspective; duplicate lifecycle messages are allowed and must be safe.

---

## Kafka Event Schemas

### `adsb.raw` / `ais.raw`

Provider-fidelity records. Position Consumer owns parsing/normalization.

### `position.normalized`

Canonical fields only. No raw provider payload. Published by Position Consumer (CP8); consumed by Correlation Worker and Deviation Detector.

| Field | Type | Notes |
| --- | --- | --- |
| `entity_id` | string | icao24 or MMSI |
| `entity_type` | string | `aircraft` \| `vessel` \| `satellite` \| `ground_vehicle` \| `unknown` |
| `timestamp_ms` | number | source event time |
| `lat` | number | |
| `lon` | number | |
| `speed_mps` | number \| null | |
| `course_deg` | number \| null | |
| `heading_deg` | number \| null | vessels only |
| `source` | string | `adsb` \| `ais` \| `satellite` \| `synthetic` |
| `provider` | string \| null | `opensky` \| `aishub` \| etc. |
| `altitude_m` | number \| null | preferred altitude; null for vessels |
| `baro_altitude_m` | number \| null | |
| `geo_altitude_m` | number \| null | |
| `vertical_rate_mps` | number \| null | |
| `on_ground` | boolean \| null | |
| `last_contact_ms` | number \| null | ADS-B only |
| `navigation_status` | string \| null | normalized enum; AIS only |
| `rate_of_turn` | number \| null | AIS only |
| `callsign` | string \| null | |
| `entity_subtype` | string \| null | normalized class |
| `provider_category` | string \| null | original provider value verbatim |
| `squawk` | string \| null | ADS-B only |
| `spi` | boolean \| null | ADS-B only |
| `position_source` | number \| null | ADS-B only |
| `position_accuracy` | boolean \| null | AIS only |
| `destination` | string \| null | AIS only |
| `eta` | string \| null | AIS only |
| `draught_m` | number \| null | AIS only |
| `history_geo_cell` | string | H3 at `HISTORY_H3_RESOLUTION` |
| `live_geo_cell` | string | H3 at `LIVE_H3_RESOLUTION` |

### `deviation.candidates`

Published on every eligible synthetic-entity ping by the stateless Deviation Detector.

Fields: `entity_id`, `timestamp_ms`, `status` (`OUT_OF_RANGE|IN_RANGE`), `current_position`, optional `nearest_segment_index`, optional `deviation_metres`.

### `proximity.candidates`

Published once per **new unscheduled** proximity episode. Known associates are already filtered upstream.

Fields:

- `pair_key`;
- `entity_a_id` (lexicographically smaller);
- `entity_b_id`;
- `episode_start_ms`;
- `lat`, `lon` midpoint;
- `distance_at_detection`.

This event already contains everything required to trigger proximity/composite evaluation; the Alert Evaluator does not perform a second Neo4j known-associate check.

### `alerts` — Kafka topic

Published by Alert Evaluator; consumed by API.

| Field | Type |
| --- | --- |
| `alert_id` | string |
| `entity_id` | string |
| `counterparty_entity_id` | string \| null |
| `entity_type` | string |
| `alert_type` | string |
| `priority` | string |
| `detected_at_ms` | number |
| `payload` | object |

Payload evidence:

- SIGNAL_LOSS: `dark_since_ms`, `callsign`, last known location;
- ROUTE_DEVIATION: route segment/deviation data and sustained count;
- UNSCHEDULED_PROXIMITY: `pair_key`, counterparty, location, distance, `episode_start_ms`;
- COMPOSITE: nested signal-loss + proximity evidence, correlation window, `supersedes_alert_ids`.

### `adsb.dlq` / `ais.dlq`

Fields: `raw_payload`, `rejection_reason`, `source_topic`, `source_offset`, `consumer_id`, operational `timestamp_ms`.

---

## API / WebSocket Client Contracts

These are the shapes the browser actually receives. They are derived from upstream canonical schemas but are not identical to them — the API transforms and namespaces before sending.

### `GET /entities/live?bbox={minLat},{minLon},{maxLat},{maxLon}`

Seeds the map on page load. Returns all entities whose current `lat`/`lon` fall within the bbox, read from Redis `entity:live:{entity_id}` hashes.

Response: array of entity snapshots.

| Field | Type | Source |
| --- | --- | --- |
| `entity_id` | string | Redis hash |
| `entity_type` | string | Redis hash |
| `timestamp_ms` | number | `last_seen_ms` from Redis hash |
| `lat` | number | Redis hash |
| `lon` | number | Redis hash |
| `altitude_m` | number \| null | Redis hash |
| `speed_mps` | number \| null | Redis hash |
| `course_deg` | number \| null | Redis hash |
| `callsign` | string \| null | Redis hash |
| `on_ground` | boolean \| null | Redis hash |
| `entity_subtype` | string \| null | Redis hash |
| `live_geo_cell` | string | Redis hash |

### `GET /alerts`

Returns persisted alerts from TimescaleDB. Fields match the `alerts` table schema. Filtered by the authenticated user's workspace scope.

### WebSocket — position update message

Forwarded from `position-updates` Redis pub/sub after viewport filtering. The `type` field namespaces position updates from alert events on the same connection.

```json
{
  "type": "position",
  "entity_id": "abc123",
  "entity_type": "aircraft",
  "timestamp_ms": 1787634583000,
  "lat": 51.5,
  "lon": -0.1,
  "altitude_m": 10150,
  "speed_mps": 220.5,
  "course_deg": 270,
  "callsign": "BA100",
  "live_geo_cell": "87194ad33ffffff"
}
```

Clients must render the position with the highest `timestamp_ms` received for a given `entity_id`. Out-of-order messages (possible due to at-least-once pub/sub) must not move a marker backward.

### WebSocket — alert event message

Forwarded from `alert-events` Redis pub/sub. Clients append to the alert list; duplicate `alert_id` messages must be safe (idempotent render).

```json
{
  "type": "alert",
  "event_type": "ALERT_CREATED | ALERT_STATUS_CHANGED | ALERT_SUPERSEDED",
  "alert_id": "abc123:SIGNAL_LOSS:1787634000000",
  "alert_type": "SIGNAL_LOSS",
  "entity_id": "abc123",
  "counterparty_entity_id": null,
  "priority": "STANDARD",
  "status": "NEW",
  "detected_at_ms": 1787634583000,
  "payload": {}
}
```

### WebSocket — subscribe message (client to API)

Client sends this on connect and on every viewport pan/zoom to scope the live position stream.

```json
{
  "type": "subscribe",
  "bbox": { "minLat": 49.0, "minLon": -8.0, "maxLat": 61.0, "maxLon": 2.0 }
}
```

API uses the bbox to filter which `position-updates` messages it forwards to this connection.

---

## Canonical Idempotency Identities

Do not use one universal key shape for every write. Use the identity of the logical fact being stored:

| Fact | Identity |
| --- | --- |
| Position history | `(entity_id, observed_at)` derived from source `timestamp_ms` |
| Redis live state | `entity_id` plus monotonic `last_seen_ms` guard |
| Proximity episode / Neo4j evidence | `{pair_key}:{episode_start_ms}` |
| Signal-loss alert | `{entity_id}:SIGNAL_LOSS:{dark_since_ms}` |
| Route-deviation alert | `{entity_id}:ROUTE_DEVIATION:{episode_start_ms}` |
| Unscheduled-proximity alert | `{pair_key}:UNSCHEDULED_PROXIMITY:{episode_start_ms}` |
| Composite alert | `{pair_key}:COMPOSITE:{dark_since_ms}` |

These identities provide deterministic replay behavior without claiming exactly-once transport.
