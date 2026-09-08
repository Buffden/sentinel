# Exact Distance Filtering Debrief

---

## Setup

```bash
make up
cd services/correlation-worker
```

---

## Experiment: `filterByDistance` against real Redis

Five integration tests against the live `sentinel-redis` container, using a fixed origin (37.0, -121.0) and latitude offsets chosen from the ~111,320m-per-degree approximation:

```text
Test Files  2 passed (2)
     Tests  11 passed (11)
```

(6 from the H3 candidate lookup suite, 5 new.)

| Test | Proves |
| --- | --- |
| keeps a candidate within the threshold and reports its distance | ~100m candidate survives a 1000m threshold with a correct computed distance |
| excludes a candidate beyond the threshold | ~5,566m candidate is dropped |
| skips a candidate with no live lat/lon rather than throwing | A hash that exists but lacks position data doesn't crash the filter |
| skips a candidate with no `entity:live` hash at all | A candidate ID with nothing in Redis doesn't crash the filter |
| sorts surviving candidates by ascending distance | ~33m candidate ranks before ~100m candidate in the result |

Cleanup verified: `KEYS entity:live:test-*` returned nothing after the suite ran.

---

## Engineering debrief

**Data flow:** given the querying entity's own lat/lon (already known from its `position.normalized` event — no Redis read needed for it) and a list of candidate IDs, `filterByDistance` fetches each candidate's `lat`/`lon` from `entity:live:{entity_id}` in parallel, computes `greatCircleDistance`, and keeps only those at or under the threshold, sorted nearest-first.

**Trade-off:** one Redis round trip per candidate (parallelized via `Promise.all`, not sequential). Pipelining these into a single round trip would reduce latency further but adds complexity; not worth it until candidate counts under real load show it matters.

**Failure behaviour:** a candidate with no position — missing hash or empty fields — is silently excluded, not an error. This is a normal, expected state (e.g. a live-state write racing with this lookup), not a system failure, and treating it as one would make the correlation path fragile against ordinary timing.

## Manual inspection commands

```bash
# Inspect a live entity's position fields directly
docker exec sentinel-redis redis-cli HMGET "entity:live:<entity_id>" lat lon

# Run the distance-filtering integration suite
cd services/correlation-worker && node_modules/.bin/vitest run
```

## Knowledge-check questions

1. Why isn't being in the same H3 cell proof that two entities are actually close?
2. Why is the querying entity's position passed in directly instead of also being looked up from `entity:live`?
3. What does `filterByDistance` do when a candidate's `entity:live` hash doesn't exist at all, and why is that the right behavior?

## Next

Canonical pair ordering (`pair_key = min(a,b):max(a,b)`) so an A-triggered-by-B encounter and a B-triggered-by-A encounter resolve to the same identity — needed before any episode state or Neo4j evidence can be keyed per pair.

---

## Key observations

| Concept | Observed |
| --- | --- |
| Candidate within threshold | Correctly kept with distance in expected range (90-110m for a ~100m offset) |
| Candidate beyond threshold | Correctly excluded |
| Missing/empty live position | Skipped, no throw |
| Result ordering | Nearest-first, verified with two candidates at different distances |
| Test cleanup | PASS — no leftover `entity:live:test-*` keys |
