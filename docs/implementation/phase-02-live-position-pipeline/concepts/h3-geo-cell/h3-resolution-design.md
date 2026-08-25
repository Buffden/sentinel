# H3 Resolution Design

---

## What H3 is

H3 is a hierarchical hexagonal grid that tiles the globe. Every point on Earth maps to exactly one hexagon at each resolution level. Resolutions run from 0 (122 large base cells) to 15 (very fine). Each step up multiplies the cell count by roughly 7: one res=5 cell contains approximately 7 res=6 cells, and approximately 49 res=7 cells.

The key property for Sentinel: nearby coordinates map to the same or adjacent cells. Adjacency is queryable with `gridDisk(cell, k)`, which returns all cells within k hops of a given cell. At k=1, that is the cell itself plus its 6 hex neighbors: 7 cells total.

---

## Two access patterns, two resolutions

Sentinel uses H3 in two independent ways, each with a separately configured resolution.

### HISTORY_H3_RESOLUTION = 5

**Purpose**: index `position_history` rows for regional time-window queries.

**Cell size at res=5**: approximately 252 km² average area.

**How it is used**: a historical query (e.g., "all aircraft in region R between T1 and T2") translates the bounding box into a set of res=5 H3 cells, then runs:

```sql
WHERE geo_cell IN (...)
  AND observed_at BETWEEN $from AND $to
```

TimescaleDB chunk exclusion narrows the time dimension. The `(geo_cell, observed_at DESC)` index narrows rows inside the selected time chunks. H3 does not create a second partition dimension — `position_history` is partitioned by `observed_at` only.

**Why coarser is fine here**: a res=5 cell covers a large area, so a bounding box translates to only a handful of cells. The time index does most of the work; `geo_cell IN (...)` is a secondary filter inside already-small time chunks.

### LIVE_H3_RESOLUTION = 7

**Purpose**: Redis sorted-set key for live proximity candidate reduction.

**Cell size at res=7**: approximately 5 km² average area.

**How it is used**: the Position Consumer writes `entity_id` into `geo-cell:{live_geo_cell}` with score=`last_seen_ms`. The Correlation Worker, when processing an incoming entity ping, queries the entity's current cell plus its k-ring to get all recently-seen entities in the area, then performs exact Haversine distance calculations on that reduced set.

**Why finer is better here**: a 5 km² cell keeps each sorted set small. For a proximity threshold of a few kilometres, a k=1 ring (7 cells, ~35 km² total) reliably captures all candidates without scanning a global set. A coarser resolution would put more entities per cell and per ring, increasing the exact-distance work.

---

## Hands-on experiment

From the London coordinate `lat=51.5, lon=-0.1`:

```
HISTORY res=5: 85194ad3fffffff
LIVE    res=7: 87194ad33ffffff
k-ring(1) size: 7 cells: [
  '87194ad33ffffff',
  '87194ad32ffffff',
  '87194ad14ffffff',
  '87194ad15ffffff',
  '87194ad06ffffff',
  '87194ad31ffffff',
  '87194ad30ffffff'
]
```

Notice the shared prefix `87194ad3` (res=7) inside `85194ad3` (res=5): H3 is hierarchical, so the child cells of a res=5 cell all share a common prefix with it.

Shifting `lat` to `51.54` (approximately 4.5 km north) produced a different res=7 cell (`87194ad36ffffff`). This is a boundary crossing: the entity moved from one sorted set to another.

---

## Changing resolutions later

`HISTORY_H3_RESOLUTION` is a write-time constant. If it is changed after data has been persisted, historical rows retain their old `geo_cell` values and will not match queries using the new cell set. Changing this value requires a backfill migration.

`LIVE_H3_RESOLUTION` affects only the current live sorted-set keys. Changing it effectively orphans the old `geo-cell:*` keys (which can be deleted) and starts building new ones. The live state self-heals after one full cycle of entity pings.
