# Normalization Schema Plan

---

## Ingestion pipeline design

Each external provider gets its own raw Kafka topic:

```text
adsb.raw
ais.raw
satellite.raw    (future)
```

The Position Consumer maps provider-specific fields into one stable canonical schema.
The raw Kafka message value is archived to `raw_events` separately — it never flows
through the canonical record.

```text
adsb.raw ─────────────────────────────────→ raw_events   (full provider JSON)
   ↓
normalize
   ↓
canonical position
   ├──→ position_history                   (canonical columns, queryable)
   ├──→ Redis live state + geo-cell index
   └──→ position.normalized                (canonical fields only)

ais.raw ──────────────────────────────────→ raw_events
   ↓
same canonical pipeline
```

In v1 the Position Consumer does both writes. A separate Raw Archiver consumer is the
clean long-term shape but is not a v1 requirement.

Adding a new source = new raw topic + new adapter. The downstream canonical pipeline
does not change.

---

## Field taxonomy decisions

### `source` vs `provider`

`source` retains its existing meaning — the telemetry class, not the company:

```text
source: 'adsb' | 'ais' | 'satellite' | 'synthetic'
```

A new field `provider` captures who sent it:

```text
provider: 'opensky' | 'aishub' | 'flightradar24' | 'spire' | 'synthetic' | null
```

This handles the case where two providers both supply ADS-B data. `source` drives
rule logic. `provider` drives data quality and provenance.

`DATA_MODEL.md` currently defines `source` as `adsb | ais | synthetic`. This plan
extends that to include `satellite`. The existing values are not changed.

### `entity_type`

Coarse physical category. Stable and drives routing logic — which rules apply,
whether altitude is expected, whether `on_ground` is meaningful:

```text
'aircraft' | 'vessel' | 'satellite' | 'ground_vehicle' | 'unknown'
```

Current repo only guarantees `aircraft | vessel`. `satellite`, `ground_vehicle`,
and `unknown` are deliberate future extensions, not assumed to exist in v1.

### `entity_subtype` and `provider_category`

Two separate fields:

- `entity_subtype` — broad normalized class, source-neutral. Used for display and
  future rule refinement. Values do not change when the provider changes.
  - aircraft: `fixed_wing | rotorcraft | uav | lighter_than_air | unknown`
  - vessel: `cargo | tanker | passenger | tug | sailing | fishing | unknown`
  - satellite: `leo | geo | meo | unknown`

- `provider_category` — original provider classification preserved verbatim as a
  string. For ADS-B this is the OpenSky `category` integer cast to string. For AIS
  this is the ship type code. Kept so information is not lost in normalization.

Example for a helicopter:

```text
entity_type:      'aircraft'
entity_subtype:   'rotorcraft'
provider_category: '7'          (OpenSky category code for rotorcraft)
```

Example for a heavy commercial jet:

```text
entity_type:      'aircraft'
entity_subtype:   'fixed_wing'
provider_category: '5'          (OpenSky category code for heavy)
```

### Altitude fields

Three separate fields instead of collapsing to one, to preserve both raw measurements
and the computed preference:

```text
altitude_m      — geo_altitude_m ?? baro_altitude_m (preferred for downstream)
baro_altitude_m — barometric; raw from provider
geo_altitude_m  — GNSS; raw from provider
```

All null for vessels. Existing `position_history.altitude` column renamed to
`altitude_m` in migration 007 to match the explicit-units convention applied across
all canonical fields: `altitude_m`, `speed_mps`, `vertical_rate_mps`, `course_deg`,
`heading_deg`, `draught_m`.

### `navigation_status` — normalized enum, not provider integer

The canonical schema uses a normalized string enum. The raw AIS numeric `NAVSTAT`
code is preserved verbatim inside `raw_events.payload` — it does not go in any
canonical column.

```text
navigation_status:
  'under_way_engine'
  | 'anchored'
  | 'not_under_command'
  | 'restricted'
  | 'constrained_by_draught'
  | 'moored'
  | 'aground'
  | 'fishing'
  | 'under_way_sailing'
  | 'sart_active'
  | 'unknown'
  | null
```

AIS NAVSTAT → canonical mapping:

| NAVSTAT | Canonical |
| --- | --- |
| 0 | `under_way_engine` |
| 1 | `anchored` |
| 2 | `not_under_command` |
| 3 | `restricted` |
| 4 | `constrained_by_draught` |
| 5 | `moored` |
| 6 | `aground` |
| 7 | `fishing` |
| 8 | `under_way_sailing` |
| 14 | `sart_active` |
| 15 / other | `unknown` |

null for aircraft (ADS-B has no equivalent field).

This principle applies consistently: **canonical field = normalized meaning;
`raw_events.payload` = exact provider value**. Downstream rule logic reads the
normalized string. If a future provider uses different codes, only the adapter changes.

---

## Canonical schema

Real-time layers (Redis, Neo4j, alerts, correlation, API) depend only on this
schema. They never read `raw_events`.

The canonical `NormalizedPosition` TypeScript type does **not** contain a `raw`
field. The raw payload is carried separately in the consumer as the original Kafka
message string and written to `raw_events` independently.

### Universal movement core

| Field | Type | ADS-B source | AIS source | Notes |
| --- | --- | --- | --- | --- |
| `entity_id` | string | `icao24` | `MMSI` | stable cross-source identity |
| `entity_type` | string | `'aircraft'` | `'vessel'` | coarse physical category |
| `timestamp_ms` | number | `time_position * 1000` | `TIME`/`TSTAMP` in ms | source event time |
| `lat` | number | `latitude` | `LATITUDE` | |
| `lon` | number | `longitude` | `LONGITUDE` | |
| `speed_mps` | number \| null | `velocity` | `SOG * 0.514444` | knots→m/s for AIS |
| `course_deg` | number \| null | `true_track` | `COG` | direction of movement |
| `heading_deg` | number \| null | null | `HEADING` | vessel heading; aircraft does not report separately |
| `source` | string | `'adsb'` | `'ais'` | telemetry class |
| `provider` | string \| null | `'opensky'` | `'aishub'` | data provider |

### Decision-support fields

| Field | Type | ADS-B source | AIS source | Notes |
| --- | --- | --- | --- | --- |
| `altitude_m` | number \| null | `geo_altitude ?? baro_altitude` | null | preferred altitude; null for vessels |
| `baro_altitude_m` | number \| null | `baro_altitude` | null | barometric; preserved separately |
| `geo_altitude_m` | number \| null | `geo_altitude` | null | GNSS; preserved separately |
| `vertical_rate_mps` | number \| null | `vertical_rate` | null | climb/descent |
| `on_ground` | boolean \| null | `on_ground` | null | surface-position indicator |
| `last_contact_ms` | number \| null | `last_contact * 1000` | null | critical for signal-loss detection |
| `navigation_status` | string \| null | null | normalized from `NAVSTAT` | see enum above; null for aircraft |
| `rate_of_turn` | number \| null | null | `ROT` | unusual manoeuvre detection |
| `callsign` | string \| null | `callsign` | `CALLSIGN` | both sources have this |
| `entity_subtype` | string \| null | derived from `category` | derived from `TYPE` | normalized class |
| `provider_category` | string \| null | `category` as string | `TYPE` as string | original provider value |
| `squawk` | string \| null | `squawk` | null | emergency/security rules (7500/7600/7700) |
| `spi` | boolean \| null | `spi` | null | special position identification |
| `position_source` | number \| null | `position_source` | null | 0=ADS-B 1=ASTERIX 2=MLAT 3=FLARM |
| `position_accuracy` | boolean \| null | null | `PAC` | AIS high/low accuracy flag |
| `destination` | string \| null | null | `DEST` | route/deviation rules |
| `eta` | string \| null | null | `ETA` | route/deviation rules |
| `draught_m` | number \| null | null | `DRAUGHT` | vessel draught |
| `history_geo_cell` | string | computed | computed | H3 at `HISTORY_H3_RESOLUTION` |
| `live_geo_cell` | string | computed | computed | H3 at `LIVE_H3_RESOLUTION` |

### Fields going to `raw_events` only

These are not in `NormalizedPosition` and not in `position_history`. The only copy
lives in `raw_events.payload`.

| Field | Source | Reason |
| --- | --- | --- |
| `origin_country` | ADS-B | derived metadata, not analytical |
| `time_position` | ADS-B | canonical uses `timestamp_ms` |
| `fetched_at_ms` | ADS-B | processing time; audit only |
| `sensors` | ADS-B | receiver IDs; poller already skips |
| `NAVSTAT` (raw integer) | AIS | canonical uses normalized `navigation_status` string |
| `IMO` | AIS | ship registry identifier; belongs in entity_metadata (see below) |
| `NAME` | AIS | vessel name; belongs in entity_metadata |
| `A` `B` `C` `D` | AIS | hull dimensions |
| `DEVICE` | AIS | positioning device type |

### Example canonical records

These show the canonical object that flows into position_history, Redis, and
position.normalized. No raw payload embedded.

Aircraft:

```json
{
  "entity_id": "4ca87c",
  "entity_type": "aircraft",
  "timestamp_ms": 1787500000000,
  "lat": 51.4, "lon": -0.4,
  "speed_mps": 215.4, "course_deg": 273.2, "heading_deg": null,
  "source": "adsb", "provider": "opensky",
  "altitude_m": 10452, "baro_altitude_m": 10363, "geo_altitude_m": 10452,
  "vertical_rate_mps": -3.2, "on_ground": false,
  "last_contact_ms": 1787500001000,
  "navigation_status": null, "rate_of_turn": null,
  "callsign": "BAW123", "entity_subtype": "fixed_wing", "provider_category": "5",
  "squawk": "7700", "spi": false, "position_source": 0, "position_accuracy": null,
  "destination": null, "eta": null, "draught_m": null,
  "history_geo_cell": "85196823fffffff", "live_geo_cell": "871968221ffffff"
}
```

Vessel:

```json
{
  "entity_id": "311733000",
  "entity_type": "vessel",
  "timestamp_ms": 1787500000000,
  "lat": 18.013, "lon": -63.046,
  "speed_mps": 7.56, "course_deg": 48.7, "heading_deg": 49,
  "source": "ais", "provider": "aishub",
  "altitude_m": null, "baro_altitude_m": null, "geo_altitude_m": null,
  "vertical_rate_mps": null, "on_ground": null,
  "last_contact_ms": null,
  "navigation_status": "moored", "rate_of_turn": 0,
  "callsign": "C6FZ7", "entity_subtype": "passenger", "provider_category": "60",
  "squawk": null, "spi": null, "position_source": null, "position_accuracy": true,
  "destination": "PHILIPSBURG", "eta": "08-24 06:00", "draught_m": 7.2,
  "history_geo_cell": "85696823fffffff", "live_geo_cell": "876968221ffffff"
}
```

---

## Poller fix required before persistence

Two changes needed in `services/ingestion-poller/src/poller.ts`:

1. Add `extended=1` to the OpenSky URL to receive `category`:

```text
current:  /api/states/all?lamin=...
required: /api/states/all?extended=1&lamin=...
```

2. Update `mapStateVector` to capture `category` at **index 17** (only present when
   `extended=1`). Without this, `entity_subtype` and `provider_category` will always
   be null for aircraft even with the URL fix.

---

## Redis `entity:live:{entity_id}` hash — expanded fields

Current fields: `lat`, `lon`, `altitude`, `entity_type`, `last_seen_ms`, `live_geo_cell`.

Rename `altitude` → `altitude_m` to match canonical convention.

Add fields needed for live rules and UI:

```text
altitude_m         (renamed from altitude)
speed_mps
course_deg
heading_deg
vertical_rate_mps
on_ground
navigation_status
callsign
entity_subtype
provider
```

All fields update under the same monotonic `last_seen_ms` timestamp guard. Stale
events cannot overwrite newer state for any field.

`DATA_MODEL.md` Redis section must be updated to reflect the expanded hash definition.

---

## Database

### `position_history` — migration 007

Two operations:

**Rename existing column:**

```sql
ALTER TABLE position_history
  RENAME COLUMN altitude TO altitude_m;
```

**Add new canonical columns:**

```sql
ALTER TABLE position_history
  ADD COLUMN IF NOT EXISTS provider          TEXT,
  ADD COLUMN IF NOT EXISTS baro_altitude_m   REAL,
  ADD COLUMN IF NOT EXISTS geo_altitude_m    REAL,
  ADD COLUMN IF NOT EXISTS speed_mps         REAL,
  ADD COLUMN IF NOT EXISTS course_deg        REAL,
  ADD COLUMN IF NOT EXISTS heading_deg       REAL,
  ADD COLUMN IF NOT EXISTS vertical_rate_mps REAL,
  ADD COLUMN IF NOT EXISTS on_ground         BOOLEAN,
  ADD COLUMN IF NOT EXISTS last_contact_ms   BIGINT,
  ADD COLUMN IF NOT EXISTS navigation_status TEXT,
  ADD COLUMN IF NOT EXISTS rate_of_turn      REAL,
  ADD COLUMN IF NOT EXISTS callsign          TEXT,
  ADD COLUMN IF NOT EXISTS entity_subtype    TEXT,
  ADD COLUMN IF NOT EXISTS provider_category TEXT,
  ADD COLUMN IF NOT EXISTS squawk            TEXT,
  ADD COLUMN IF NOT EXISTS spi               BOOLEAN,
  ADD COLUMN IF NOT EXISTS position_source   SMALLINT,
  ADD COLUMN IF NOT EXISTS position_accuracy BOOLEAN,
  ADD COLUMN IF NOT EXISTS destination       TEXT,
  ADD COLUMN IF NOT EXISTS eta               TEXT,
  ADD COLUMN IF NOT EXISTS draught_m         REAL;
```

`navigation_status` is TEXT (stores the normalized string enum, not the raw NAVSTAT
integer). Existing indexes and constraints are unaffected. All new columns are nullable.

### `raw_events` — migration 008

Regular PostgreSQL table on the TimescaleDB instance. **Not a hypertable.**

Reason: the idempotency key `(source_topic, source_partition, source_offset)` does
not include a time column. Making it a hypertable would require adding `received_at`
to the unique constraint, which breaks replay idempotency — a replayed event gets a
new processing timestamp and the unique check would not catch it. If raw volume grows
large, S3/object storage is the more natural long-term archive.

```sql
CREATE TABLE raw_events (
  id               BIGSERIAL PRIMARY KEY,
  entity_id        TEXT,
  source           TEXT        NOT NULL,
  provider         TEXT,
  source_topic     TEXT        NOT NULL,
  source_partition INTEGER     NOT NULL,
  source_offset    BIGINT      NOT NULL,
  received_at      TIMESTAMPTZ NOT NULL,
  source_event_time TIMESTAMPTZ,
  payload          JSONB       NOT NULL,

  UNIQUE (source_topic, source_partition, source_offset)
);

CREATE INDEX ON raw_events (entity_id, received_at DESC);
```

`received_at` = processing time. `source_event_time` = provider event time
(`to_timestamp(timestamp_ms / 1000.0)`).

The unique constraint on `(source_topic, source_partition, source_offset)` makes
replayed events idempotent. Offsets are only unique per partition, so the partition
column is mandatory for correctness.

### Atomicity note

Two separate writes per event in v1. A crash between them leaves `position_history`
with a row and no matching `raw_events` row. Acceptable — raw is an archive, not a
correctness dependency. The Kafka offset is committed only after both writes succeed,
so a crash causes full replay and both writes are retried idempotently.

---

## Implementation plan

```text
normalization and schema contract: parse + normalize + log canonical object (no persistence)
validation and DLQ routing
TimescaleDB persistence: position_history write + raw_events write + idempotency + offset commit behavior
Redis live state + geo-cell index
downstream publishing: position.normalized publish + position-updates pub/sub publish
```

Schema contract documentation (`DATA_MODEL.md`, `ARCHITECTURE.md`, PUMLs) is updated
during normalization because the canonical shape is settled. Actual persistence comes later.

### Consumer write order

```text
1. INSERT position_history            (canonical columns; ON CONFLICT DO NOTHING)
2. INSERT raw_events                  (full payload; ON CONFLICT DO NOTHING)
3. HGET entity:live:{entity_id}       (read previous live_geo_cell for ZREM)
4. ZADD / ZREM geo-cell sorted sets   (monotonic guard by timestamp_ms)
5. HSET entity:live:{entity_id}       (monotonic guard on last_seen_ms)
6. PUBLISH position.normalized        (canonical fields only)
7. PUBLISH position-updates           (Redis pub/sub; same canonical payload)
8. commitOffsets                      (crash boundary; all above replay safely)
```

`position.normalized` carries the canonical record only — no raw payload. The raw
string from the Kafka message is written to `raw_events` in step 2.

---

## Architectural gap to document: `entity_metadata`

Fields like `IMO`, vessel `NAME`, aircraft registration, operator, and fleet are
entity-level identity data — they change rarely or never. Putting them in every
`position_history` row wastes space and conflates two different concerns:

```text
position_history   = things that change every position update
entity_metadata    = relatively stable identity information
raw_events         = exact provider records
```

A future `entity_metadata` table would look like:

```text
entity_id      TEXT PK
entity_type    TEXT
name           TEXT         (vessel NAME, aircraft registration)
callsign       TEXT
imo            TEXT         (vessel IMO number)
registration   TEXT         (aircraft registration)
operator       TEXT
subtype        TEXT
provider_meta  JSONB        (other provider-specific identity fields)
updated_at     TIMESTAMPTZ
```

**Do not build this in Phase 02.** Record the distinction here so it is not
forgotten, and so future sessions do not start stuffing `NAME` / `IMO` into every
position row or into `NormalizedPosition`.

---

## Persistence layer mappings

The canonical schema field names differ from DB column names in one place:

| Canonical field | DB column | Note |
| --- | --- | --- |
| `history_geo_cell` | `geo_cell` | map at write time in consumer |

---

## Files to change

### Now (schema contract, no persistence)

| File | Change |
| --- | --- |
| `services/position-consumer/src/normalize.ts` | Full canonical schema with all fields above; no `raw` field in the type |
| `services/position-consumer/src/consumer.ts` | Log `entity_id`; updated field names (`altitude_m` etc.) |
| `docs/DATA_MODEL.md` | `position.normalized` Kafka schema; Redis `entity:live` hash fields; `position_history` columns; new `raw_events` table definition |
| `docs/ARCHITECTURE.md` | Redis `entity:live` hash field list |
| `docs/use-cases/US-02-live-map-updates/update-push.puml` | `altitude` → `altitude_m` in HSET, PUBLISH, and push payload |

### Before persistence

| File | Change |
| --- | --- |
| `services/ingestion-poller/src/poller.ts` | Add `extended=1`; capture `category` at index 17 |

### With persistence

| File | Change |
| --- | --- |
| `infra/migrations/007_position_history_expand.sql` | Rename `altitude` → `altitude_m`; add new columns |
| `infra/migrations/008_raw_events_table.sql` | Create regular `raw_events` table with `source_partition` in unique key |
| `services/position-consumer/src/consumer.ts` | Full persistence implementation; follow write order above; commit offset last |

### After migration 007 runs

| File | Change |
| --- | --- |
| `scripts/manual-store-verification/timescaledb.sql` | Update INSERT to use `altitude_m` and include new columns needed for verification |
