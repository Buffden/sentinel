# H3 Proximity Candidate Lookup Debrief

---

## Setup

```bash
make up
cd services/correlation-worker
pnpm install
```

---

## Experiment 1: direct H3 exploration (before writing any service code)

Per this project's rule to interact directly with unfamiliar infrastructure/libraries before wrapping them in application code, a short script was run against this project's actual `h3-js` dependency (v4.5.0) and `LIVE_H3_RESOLUTION = 7`, using a base point (37.0, -121.0) inside the ingestion poller's California bbox.

```
origin cell: 8729a9749ffffff
gridDisk(0) size: 1
gridDisk(1) size: 7
gridDisk(2) size: 19
average edge length at res 7 (m): 1406.475763
```

Boundary-crossing pair, found by taking the midpoint of an actual shared edge between the origin cell and its nearest neighbor (from `cellToBoundary`), then nudging ~10m to each side of that midpoint along the edge-normal direction:

```
Point A (inside origin, near edge): 36.989033 -120.988104  cell: 8729a9749ffffff
Point B (just across boundary):    36.988954 -120.987902  cell: 8729a9748ffffff
distance A-B (m): 19.98
A and B in different H3 cells: true
B is a k=1 neighbor of A? true
```

Outer-ring cell, found via `gridRing(origin, 2)`:

```
Outer-ring cell (k=2 only): 8729a976bffffff
in gridDisk(1)? false
in gridDisk(2)? true
distance A-C (m): 5021.0
```

| Check | Expected | Observed |
| --- | --- | --- |
| `gridDisk(0/1/2)` sizes match normal hexagon topology (1/7/19) | yes | PASS |
| A real 2-cell-apart pair exists that is physically ~20m apart | yes | PASS — 19.98m, confirms same-cell-only lookup would miss it |
| A cell exists in `gridDisk(2)` but not `gridDisk(1)` | yes | PASS — proves k=1 is insufficient for that specific candidate |

This is the empirical basis for CP1's implementation: `k` is not computed from the average edge length (1406.48m), which the H3 documentation itself does not present as a worst-case bound. It's a fixed, conservative constant (`CANDIDATE_SEARCH_K = 2`) chosen after seeing directly what each `k` value does and does not cover.

---

## Experiment 2: `findProximityCandidates` against real Redis

Six integration tests, run against the live `sentinel-redis` container (not a mock), using the exact cell IDs and the verified boundary-crossing pair from Experiment 1:

```
 RUN  v4.1.11 /Users/harshwardhanpatil/Work/sentinel/services/correlation-worker

 Test Files  1 passed (1)
      Tests  6 passed (6)
```

| Test | Proves |
| --- | --- |
| finds a candidate in the same cell at k=0 | Baseline same-cell case works |
| excludes the querying entity from its own cell | Self-exclusion works |
| misses a boundary-crossing neighbor at k=0 but finds it at k=1 | The exact 19.98m same-boundary case from Experiment 1 is only caught once k reaches the ring it's actually in |
| misses an outer-ring candidate at k=1 but finds it at k=2 | The `gridRing(2)`-only cell from Experiment 1 requires k=2, justifying the conservative default |
| excludes a same-cell member whose `last_seen_ms` is older than the freshness bound | `ZRANGEBYSCORE` freshness filtering works, matching the signal-loss scan's staleness-exclusion pattern |
| dedupes a candidate reachable through more than one ring cell | Result set has no duplicate entity IDs across scanned cells |

Cleanup verified: after the suite ran, `KEYS geo-cell:8729a97*` showed only `geo-cell:8729a9752ffffff` — a real ADS-B entity (`a63745`) from live ingestion traffic already running on this dev stack, unrelated to the test's own cells (`...749`, `...748`, `...76b`), confirming the test suite's own seeded keys were fully cleaned up and nothing bled into real state.

---

## Checkpoint Completion

### Engineering debrief

**Data flow:** given an entity's `live_geo_cell` (already computed and stored by the Position Consumer on every accepted position), `findProximityCandidates` computes `gridDisk(liveGeoCell, k)` to get the origin cell plus k rings of neighbors, then runs `ZRANGEBYSCORE geo-cell:{cell} minLastSeenMs +inf` against each cell in that ring, unions the results into a deduplicated set, and removes the querying entity's own ID. It returns candidate entity IDs only — no distance, no pair identity, no persisted state.

**Main trade-off:** `k=2` (19 cells) instead of a computed `k=1` (7 cells) means roughly 2.7x more Redis round-trips per candidate lookup, in exchange for a defensible correctness margin against H3's cell-size variance that a formula based on the *average* edge length cannot actually guarantee. This is a deliberate correctness-over-throughput choice for V1, explicitly left open for future benchmarking against real entity density rather than assumed permanent.

**Failure behaviour:** the two failure modes this checkpoint protects against are both silent-by-default: a false negative (missing a genuinely close pair because the search radius was too small) is unrecoverable downstream, since CP2 never sees a pair CP1 didn't surface; a stale candidate (an entity that went dark but is still a sorted-set member because there's no key-level TTL) is excluded logically via the freshness score filter rather than via any cleanup process. Self-exclusion prevents a systemic false-positive (every entity being its own candidate) that would otherwise pass every test that didn't specifically check for it.

### Manual inspection commands

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
cd services/correlation-worker
node_modules/.bin/vitest run
```

### Knowledge-check questions

1. Why is a false negative in candidate lookup categorically worse than a false positive here?
2. Why does the debrief's boundary-crossing pair (19.98m apart) require `k=1`, not `k=0`, to be found?
3. Why was `k=2` chosen as a fixed constant instead of computing it from H3's published average edge length?
4. What removes a permanently-dark entity from its `geo-cell:*` sorted set, and why does the freshness filter matter given that answer?

### Optional manual tweak

In `candidates.integration.test.ts`, add a seventh test using `gridRing(ORIGIN_CELL, 3)` (three rings out) to confirm that even `k=2` cannot find a candidate that far away — making the current search radius's actual boundary concrete, not just asserted.

### Next checkpoint

CP2 — exact distance filtering. Take `findProximityCandidates`'s output and compute the real great-circle (haversine) distance from the querying entity's actual lat/lon (not the H3 cell center) to each candidate's actual lat/lon, then filter to those within `PROXIMITY_THRESHOLD_METRES`. This is where the over-fetched `k=2` candidates from CP1 (including any that turn out too far away) get discarded — CP1 intentionally never attempts this filtering itself.

---

## Key observations

| Concept | Observed |
| --- | --- |
| `gridDisk(cell, k)` sizes for k=0/1/2 | 1 / 7 / 19 — exact normal-hexagon topology |
| H3's published average edge length at res 7 | 1406.475763 m — an average, not a coverage bound |
| Real boundary-crossing pair found | 19.98m apart, different cells, k=1 neighbor |
| Real outer-ring-only candidate found | ~5021m away, in `gridDisk(2)` but not `gridDisk(1)` |
| `findProximityCandidates` — 6/6 integration tests against real Redis | PASS |
| Test cleanup verified against live dev traffic | PASS — no leftover test keys; unrelated real ADS-B entity in a neighboring cell untouched |
