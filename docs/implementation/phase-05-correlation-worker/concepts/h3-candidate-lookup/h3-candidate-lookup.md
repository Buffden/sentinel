# H3 Proximity Candidate Lookup — Design and Learning Reference

Plain language first, then the data. Use this to understand, inspect, and defend the implementation.

---

## Why candidate reduction

Comparing every live entity to every other on each ping is O(n²). The Position Consumer already tags each entity with a coarse H3 cell (`live_geo_cell`) and maintains a Redis sorted set per cell (`geo-cell:{cell_id}`). The Correlation Worker only needs to ask "who's in or near my cell" instead of "who's anywhere." This checkpoint is that narrowing step alone — not the proximity answer itself.

---

## Concepts

### Candidate recall vs proximity detection

This function answers "who might possibly be close?" A later, separate stage answers "who actually is within `PROXIMITY_THRESHOLD_METRES`?" The two are split because the risk is asymmetric: a false positive here costs one wasted distance check downstream — cheap. A false negative is unrecoverable — a pair never returned is never checked at all. Every design choice below follows from that: bias toward more candidates, never fewer.

### Same-cell lookup is insufficient

H3 tiles the earth into hexagons. Two entities can be almost on top of each other while sitting in different cells, if they're on opposite sides of a boundary. Verified directly against this project's `LIVE_H3_RESOLUTION = 7`: two points 19.98m apart resolved to different adjacent cells (see the lab below). A same-cell-only lookup would miss that pair entirely.

### `gridDisk(cell, k)`

Returns every cell within `k` grid steps of the origin, including the origin. For a normal hexagon: `k=0` → 1 cell, `k=1` → 7, `k=2` → 19 — confirmed directly with h3-js against a real cell in this project's test region.

### `k` is a chosen constant, not a formula

H3 publishes an *average* edge length per resolution (~1406m at resolution 7), but average is not a worst-case bound — H3 cells vary in size across the globe, and H3's own docs say so. Computing `k = ceil(threshold / averageEdgeLength)` and trusting it would present an estimate as a proof. `CANDIDATE_SEARCH_K` (2) was instead chosen by exercising `gridDisk`/`gridRing` against real boundaries and confirming what each `k` actually covers, erring toward the safer value pending future density benchmarking — the same "validate experimentally, don't just compute" reasoning ADR-006 applies to the resolution choice itself.

### Freshness lower bound

`geo-cell:*` membership is never cleaned up when an entity stops transmitting. Without a floor on `last_seen_ms`, a dark entity stays a permanent false-positive candidate. `ZRANGEBYSCORE` with a `minLastSeenMs` lower bound excludes it — the same pattern the signal-loss scan uses: staleness is enforced by filtering a timestamp, not by deleting data.

### Self-exclusion

An entity is a member of its own `geo-cell:{live_geo_cell}` set. Without filtering it out, every entity would be a candidate for itself.

---

## Failure modes

**Trusting average edge length as a bound.** A smaller-than-average cell near the search boundary could put a genuinely close entity one ring further out than a formula assumes — a silent false negative. This is what the fixed, conservative `k=2` hedges against.

**Skipping the freshness filter.** An entity dark for hours stays a live candidate forever, since Redis has no TTL on `geo-cell:*` membership.

**Skipping self-exclusion.** Every lookup would report the querying entity as its own candidate, silently corrupting every downstream pair calculation.

---

## Map to code

| Concept | Where |
| --- | --- |
| Candidate lookup | `findProximityCandidates` — `services/correlation-worker/src/candidates.ts` |
| k-ring computation | `gridDisk(liveGeoCell, k)` from `h3-js` |
| Candidate source | `geo-cell:{cell_id}` sorted sets, written by Position Consumer |
| Freshness filter | `ZRANGEBYSCORE geo-cell:{cell} minLastSeenMs +inf` |
| Search radius | `CANDIDATE_SEARCH_K = 2` — `services/correlation-worker/src/config.ts` |
| V1 proximity threshold | `PROXIMITY_THRESHOLD_METRES = 1000` — same file |

---

## H3 lab (already run — see debrief for full output)

Direct exploration with h3-js against a real cell in this project's California test region, without trusting any formula:

- `gridDisk(origin, 0/1/2)` sizes are exactly 1 / 7 / 19.
- A boundary-crossing pair 19.98m apart resolves to two different adjacent cells — proving same-cell lookup is insufficient, and `k=1` recovers it.
- A cell exists in `gridDisk(origin, 2)` but not `gridDisk(origin, 1)` — proving `k=1` is insufficient for a farther-but-relevant candidate, and `k=2` recovers it.

To reproduce, run from a service directory with `h3-js` installed:

```js
import * as h3 from 'h3-js';
const origin = h3.latLngToCell(37.0, -121.0, 7);
console.log(h3.gridDisk(origin, 1).length, h3.gridDisk(origin, 2).length);
console.log(h3.cellToBoundary(origin));
```

---

## Retention questions

1. Why is a false negative here categorically worse than a false positive?
2. Why does `gridDisk(cell, k)` include the origin cell, and what breaks if a caller unions it with `[cell]` again?
3. Why was `k=2` chosen as a fixed constant instead of `ceil(threshold / averageEdgeLength)`?
4. What removes a `geo-cell:*` member once its entity goes permanently dark, and why does the freshness filter matter given that?
5. What breaks if `findProximityCandidates` didn't exclude the querying entity's own ID?

---

## Completion checklist

- [ ] I can explain why candidate recall and proximity detection are separate stages
- [ ] I can give a concrete boundary example showing same-cell lookup misses real pairs
- [ ] I can state what `gridDisk(cell, k)` returns for k=0/1/2
- [ ] I can explain why `k` is a fixed constant, not a formula from average edge length
- [ ] I can explain the freshness filter and why `geo-cell:*` has no TTL
- [ ] I can explain why self-exclusion is necessary
- [ ] I ran the integration suite against real Redis and can interpret each test
