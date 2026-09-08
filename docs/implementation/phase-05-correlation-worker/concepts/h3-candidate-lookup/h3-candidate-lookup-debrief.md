# H3 Proximity Candidate Lookup Debrief

---

## Setup

```bash
make up
cd services/correlation-worker
pnpm install
```

---

## Experiment 1: direct H3 exploration, before writing service code

A short script was run against this project's actual `h3-js` dependency (v4.5.0) and `LIVE_H3_RESOLUTION = 7`, using a base point (37.0, -121.0) inside the ingestion poller's California bbox.

```text
origin cell: 8729a9749ffffff
gridDisk(0) size: 1
gridDisk(1) size: 7
gridDisk(2) size: 19
average edge length at res 7 (m): 1406.475763
```

Boundary-crossing pair, found by taking the midpoint of an actual shared edge between the origin cell and its nearest neighbor (from `cellToBoundary`), then nudging ~10m to each side along the edge-normal direction:

```text
Point A (inside origin, near edge): 36.989033 -120.988104  cell: 8729a9749ffffff
Point B (just across boundary):    36.988954 -120.987902  cell: 8729a9748ffffff
distance A-B (m): 19.98
B is a k=1 neighbor of A? true
```

Outer-ring cell, found via `gridRing(origin, 2)`:

```text
Outer-ring cell (k=2 only): 8729a976bffffff
in gridDisk(1)? false
in gridDisk(2)? true
distance A-C (m): 5021.0
```

| Check | Expected | Observed |
| --- | --- | --- |
| `gridDisk(0/1/2)` sizes match normal hexagon topology | 1/7/19 | PASS |
| A real 2-cell-apart pair is physically close | yes | PASS — 19.98m; same-cell-only lookup would miss it |
| A cell exists in `gridDisk(2)` but not `gridDisk(1)` | yes | PASS — k=1 is insufficient for it |

This is the empirical basis for the implementation: `k` isn't computed from the average edge length (1406.48m, not a documented worst-case bound). It's a fixed constant (`CANDIDATE_SEARCH_K = 2`) chosen after seeing directly what each `k` does and doesn't cover.

---

## Experiment 2: `findProximityCandidates` against real Redis

Six integration tests against the live `sentinel-redis` container, using the exact cells and boundary-crossing pair from Experiment 1:

```text
Test Files  1 passed (1)
     Tests  6 passed (6)
```

| Test | Proves |
| --- | --- |
| finds a candidate in the same cell at k=0 | Baseline case works |
| excludes the querying entity from its own cell | Self-exclusion works |
| misses a boundary-crossing neighbor at k=0, finds it at k=1 | The 19.98m case is only caught once k reaches the right ring |
| misses an outer-ring candidate at k=1, finds it at k=2 | The `gridRing(2)`-only cell justifies the conservative default |
| excludes a stale same-cell member | `ZRANGEBYSCORE` freshness filtering works |
| dedupes a candidate reachable through more than one ring cell | No duplicate entity IDs across scanned cells |

Cleanup verified: after the suite ran, `KEYS geo-cell:8729a97*` showed only `geo-cell:8729a9752ffffff` — a real ADS-B entity (`a63745`) from live ingestion traffic already on this dev stack, unrelated to the test's own cells. Nothing leaked either direction.

---

## Engineering debrief

**Data flow:** given an entity's `live_geo_cell`, `findProximityCandidates` computes `gridDisk(liveGeoCell, k)`, runs `ZRANGEBYSCORE geo-cell:{cell} minLastSeenMs +inf` against each cell, unions the results, and drops the querying entity's own ID. It returns candidate entity IDs only — no distance, no pair identity, no persisted state.

**Trade-off:** `k=2` (19 cells) instead of `k=1` (7) means ~2.7x more Redis round-trips per lookup, traded for a correctness margin against H3's cell-size variance that an average-edge-length formula can't actually guarantee. Deliberately left open for benchmarking against real density.

**Failure behaviour:** a false negative (search radius too small) is unrecoverable downstream — nothing later ever sees a pair this step didn't surface. A stale candidate (entity gone dark, no key-level TTL) is excluded logically via the freshness filter. Self-exclusion prevents every entity from being its own candidate, a bug no type system catches.

## Manual inspection commands

```bash
# Re-run the H3 lab directly
cd services/correlation-worker
cat <<'EOF' > /tmp/h3-lab.mjs
import * as h3 from 'h3-js';
const origin = h3.latLngToCell(37.0, -121.0, 7);
console.log(h3.gridDisk(origin, 1).length, h3.gridDisk(origin, 2).length);
EOF
node /tmp/h3-lab.mjs

# Inspect a live geo-cell sorted set directly
docker exec sentinel-redis redis-cli ZRANGE "geo-cell:8729a9752ffffff" 0 -1 WITHSCORES

# Run the candidate-lookup integration suite
cd services/correlation-worker && node_modules/.bin/vitest run
```

## Knowledge-check questions

1. Why is a false negative in candidate lookup categorically worse than a false positive?
2. Why does the 19.98m boundary-crossing pair need `k=1`, not `k=0`, to be found?
3. Why was `k=2` chosen as a fixed constant instead of computed from H3's average edge length?
4. What removes a permanently-dark entity from its `geo-cell:*` set, and why does the freshness filter matter given that?

## Optional manual tweak

Add a test using `gridRing(ORIGIN_CELL, 3)` to confirm even `k=2` cannot find a candidate that far away — making the search radius's actual boundary concrete, not just asserted.

## Next

Exact great-circle (haversine) distance over these candidates, filtered to `PROXIMITY_THRESHOLD_METRES` — this is where the over-fetched `k=2` candidates that turn out too far away get discarded.

---

## Key observations

| Concept | Observed |
| --- | --- |
| `gridDisk(cell, k)` sizes for k=0/1/2 | 1 / 7 / 19 |
| H3's average edge length at res 7 | 1406.48m — an average, not a coverage bound |
| Real boundary-crossing pair | 19.98m apart, different cells, k=1 neighbor |
| Real outer-ring-only candidate | ~5021m away, needs k=2 |
| `findProximityCandidates` integration tests | 6/6 PASS against real Redis |
| Test cleanup against live dev traffic | PASS — no leakage either direction |
