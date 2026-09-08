# H3 Proximity Candidate Lookup — Design and Learning Reference

Plain language first, then technical depth, then the data. Use this to understand, inspect, and defend the CP1 implementation.

---

## Why candidate reduction, not "compare everyone to everyone"

The Correlation Worker's job is to notice when two entities are physically close. The naive approach — on every position update, compute the distance from this entity to every other live entity — is O(n) per ping and O(n²) across the whole system. H3 exists to avoid that: the Position Consumer already tags every live entity with a coarse "which roughly 1.4km² cell are you in" label (`live_geo_cell`) and maintains a Redis sorted set per cell (`geo-cell:{cell_id}`). The Correlation Worker only has to ask "who's in my cell or nearby cells" instead of "who's anywhere."

This checkpoint proves that narrowing step alone — the candidate set, not the actual proximity answer.

---

## Concepts in plain language

### 1. Candidate recall vs proximity detection — two different jobs

CP1 answers "who might possibly be close?" CP2 (not yet built) answers "who actually is within `PROXIMITY_THRESHOLD_METRES`?" These are deliberately separate stages because they have opposite risk profiles: a false positive from CP1 (a candidate that turns out too far away) costs one wasted CP2 distance calculation — cheap. A false negative from CP1 (a genuinely close pair CP1 never surfaces) is unrecoverable — CP2 never sees that pair at all, so it's not merely a missed optimization, it's a missed detection. Every CP1 design decision below follows from that asymmetry: bias toward returning more candidates, never toward returning fewer.

### 2. Why same-cell lookup alone is wrong

H3 tiles the earth into a hexagonal grid. Two entities can sit almost on top of each other in the real world while belonging to different H3 cells, if they happen to be on opposite sides of a cell boundary. Verified directly against this project's actual `LIVE_H3_RESOLUTION = 7` (see the lab section below): two points 19.98 metres apart resolved to two *different* adjacent cells. A same-cell-only lookup would have missed that pair entirely — the exact unrecoverable failure mode CP1 exists to prevent.

### 3. `gridDisk(cell, k)` — origin plus k rings of neighbors

`gridDisk` returns every cell within `k` grid steps of the origin cell, including the origin itself. For a normal (non-pentagon) hexagon: `k=0` → 1 cell (just the origin), `k=1` → 7 cells (origin + 6 neighbors), `k=2` → 19 cells. Verified directly with h3-js against a real cell in this project's California test region (see lab output below) — exactly 1, 7, and 19.

### 4. Why `k` is a chosen conservative constant, not a formula

H3 publishes an *average* edge length per resolution (≈1406m at resolution 7) — but average is not a worst-case bound; H3 cells vary in size depending on where they are on the globe (H3's own documentation is explicit about this). Computing `k = ceil(threshold / averageEdgeLength)` and treating the result as a guarantee would be presenting an estimate as a proof. Instead, `CANDIDATE_SEARCH_K` in `config.ts` is a conservative constant (2) chosen by directly exercising `gridDisk`/`gridRing` against real cell boundaries and confirming empirically what each `k` value actually covers, then erring toward the larger, safer value pending future density benchmarking. This is the same reasoning ADR-006 already applies to the resolution choice itself: "both values should be validated experimentally against realistic data density and query patterns," not derived from a single formula and trusted.

### 5. The freshness lower bound

`geo-cell:{cell}` sorted sets are never actively cleaned up when an entity stops transmitting — an aircraft that went dark 20 minutes ago is still a member of whatever cell it was last in. Without a freshness filter, a stale entity would be a permanent false-positive candidate for anything near its last known position. `ZRANGEBYSCORE` with a `minLastSeenMs` lower bound excludes it the same way the Alert Evaluator's signal-loss scan excludes entities whose `last_seen_ms` is too old — a recurring pattern in this codebase: freshness is enforced by filtering on a timestamp score, not by deleting the underlying data proactively.

### 6. Self-exclusion

An entity is itself a member of its own `geo-cell:{live_geo_cell}` set — the Position Consumer doesn't special-case that. `findProximityCandidates` must filter the querying entity's own ID out of the result, or the Correlation Worker would consider every entity a proximity candidate for itself.

---

## Failure modes

### Treating average edge length as a coverage guarantee

If `k` were computed as `ceil(threshold / averageEdgeLength)` and trusted blindly, a smaller-than-average cell near the search boundary could put a genuinely close entity one ring further out than the formula assumed — a silent false negative with no error anywhere. This is exactly the failure mode the conservative fixed `k=2` (rather than a computed `k=1`) is deliberately hedging against pending real density data.

### Forgetting the freshness filter

Without `minLastSeenMs`, an entity that went dark hours ago still shows up as a live proximity candidate forever, because Redis has no TTL on `geo-cell:*` membership — cleanup is logical (via score filtering), not physical (via key expiry). This would make every proximity check progressively noisier as more entities go dark over the system's uptime.

### Forgetting self-exclusion

Without filtering the caller's own `entityId`, every candidate lookup would report the querying entity as a candidate for itself — not caught by any type system, silently corrupting every downstream distance/pair calculation in CP2 onward.

---

## Map mental model to code

| Concept | Where |
| --- | --- |
| Candidate lookup function | `findProximityCandidates` — `services/correlation-worker/src/candidates.ts` |
| k-ring computation | `gridDisk(liveGeoCell, k)` from `h3-js` |
| Redis candidate source | `geo-cell:{cell_id}` sorted sets, written by Position Consumer (`services/position-consumer/src/consumer.ts`) |
| Freshness filter | `ZRANGEBYSCORE geo-cell:{cell} minLastSeenMs +inf` |
| Conservative ring radius | `CANDIDATE_SEARCH_K = 2` — `services/correlation-worker/src/config.ts` |
| V1 proximity threshold | `PROXIMITY_THRESHOLD_METRES = 1000` — same file, explicitly a configurable V1 rule value, not an aviation-safety constant |

---

## H3 lab (already run — see debrief for full output)

Direct exploration with h3-js 4.5.0 against a real cell in the project's California test region proved, without trusting any formula:

- `gridDisk(origin, 0/1/2)` sizes are exactly 1 / 7 / 19, matching normal hexagon topology.
- A boundary-crossing pair 19.98m apart in the real world resolves to two different adjacent cells — proving same-cell lookup is insufficient, and that `k=1` recovers it.
- A specific cell exists that is in `gridDisk(origin, 2)` but not `gridDisk(origin, 1)` — proving `k=1` is insufficient for a legitimately farther-but-still-relevant candidate, and `k=2` recovers it.

To reproduce or extend this lab yourself, run a short Node script requiring `h3-js` from within a service directory that has it installed (e.g. `services/correlation-worker`):

```js
import * as h3 from 'h3-js';
const origin = h3.latLngToCell(37.0, -121.0, 7);
console.log(h3.gridDisk(origin, 1).length, h3.gridDisk(origin, 2).length);
console.log(h3.cellToBoundary(origin)); // inspect actual boundary vertices
```

---

## Retention questions

1. Why is a false negative in candidate lookup categorically worse than a false positive, given what CP2 does?
2. Why does `gridDisk(cell, k)` include the origin cell itself, and what would break if a caller forgot that and unioned `gridDisk(cell, k)` with `[cell]`?
3. Why was `k=2` chosen as a fixed constant instead of `ceil(PROXIMITY_THRESHOLD_METRES / averageEdgeLength)`? What specifically is wrong with trusting the average edge length as a bound?
4. What happens to a `geo-cell:*` sorted-set member for an entity that goes permanently dark? What removes it, and why is the freshness filter necessary if nothing else does?
5. What would happen to a proximity candidate result if `findProximityCandidates` did not exclude the querying entity's own ID?

---

## CP1 completion checklist

- [ ] I can explain why candidate recall and proximity detection are deliberately separate stages
- [ ] I can explain why same-cell lookup alone misses real proximity pairs, with a concrete boundary example
- [ ] I can state what `gridDisk(cell, k)` returns for k=0/1/2 and why
- [ ] I can explain why `k` is a conservative fixed constant rather than a formula from average edge length
- [ ] I can explain the freshness filter and why `geo-cell:*` has no TTL-based cleanup
- [ ] I can explain why self-exclusion is necessary
- [ ] I ran the integration test suite against real Redis and can interpret what each of the six tests proves
- [ ] I can state what CP1 delivered and what CP2 adds
