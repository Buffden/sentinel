# Exact Distance Filtering — Design and Learning Reference

---

## What this stage does

Candidate lookup deliberately over-fetches (see the H3 candidate lookup note). This stage throws the extras away: for each candidate, fetch its live lat/lon and compute the real distance from the querying entity, keeping only those within `PROXIMITY_THRESHOLD_METRES`. This is where recall turns into precision.

---

## Concepts

### Why look up lat/lon again instead of trusting the cell

An H3 cell only says "roughly here" — resolution 7 cells span ~1.4km². Two candidates in the same or a neighboring cell can still be anywhere from a few metres to over a kilometre apart. The querying entity's own position arrives directly on its `position.normalized` event, but each candidate's current position has to be read from `entity:live:{entity_id}`, the same live-state hash the Position Consumer maintains for the map and the signal-loss scan.

### Reusing `greatCircleDistance`

The H3 candidate lookup's own exploration lab already validated `h3-js`'s `greatCircleDistance` against real coordinates. Reusing it here avoids a second, independently-written distance formula that would need its own correctness check.

### Skip, don't throw, on a missing position

A candidate can lack a usable position for two reasons: no `entity:live` hash at all (unlikely but not impossible under replay/timing edge cases), or a hash that exists but has empty `lat`/`lon` fields (same convention used elsewhere in this codebase for absent numeric fields). Either way, the correct response is to drop that candidate silently, not throw — a missing position isn't a proximity match, and it isn't a system error either. This mirrors the Alert Evaluator's own defensive handling of `entity:live` fields.

### Sorting by distance

Not required for correctness, but returning matches nearest-first makes the result deterministic and easier to reason about for anything downstream that only cares about the closest encounter.

---

## Failure modes

**Trusting cell membership as a proximity answer.** Two entities in the same H3 cell can still be 1km+ apart. Skipping this stage and treating every H3 candidate as a real match would produce alerts for pairs that were never actually close.

**Throwing on a missing position instead of skipping.** Would turn a normal, expected state (a candidate whose live-state write raced with this lookup, or was cleaned up) into a hard failure that could take down the correlation path over something that isn't actually wrong.

---

## Map to code

| Concept | Where |
| --- | --- |
| Distance filter | `filterByDistance` — `services/correlation-worker/src/distance.ts` |
| Live position source | `entity:live:{entity_id}` hash (`lat`, `lon`), written by Position Consumer |
| Distance calculation | `greatCircleDistance` from `h3-js` |
| Threshold | `PROXIMITY_THRESHOLD_METRES` — `services/correlation-worker/src/config.ts` |

---

## Retention questions

1. Why isn't H3 cell membership by itself proof of proximity?
2. Why is the querying entity's own position passed in directly, while each candidate's position is looked up from Redis?
3. Why does a missing or empty `entity:live` lat/lon get skipped instead of raising an error?
4. Why reuse `greatCircleDistance` from `h3-js` rather than writing a separate haversine implementation?

---

## Completion checklist

- [ ] I can explain why H3 cell membership alone isn't a proximity guarantee
- [ ] I can explain why candidate positions are re-fetched rather than trusted from the H3 lookup
- [ ] I can explain why a missing position is skipped, not an error
- [ ] I ran the integration suite against real Redis and can interpret each test
