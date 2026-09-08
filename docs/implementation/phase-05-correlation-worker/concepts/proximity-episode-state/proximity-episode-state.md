# Proximity Episode State — Design and Learning Reference

---

## What this solves

The Neo4j write needs an `episode_start_ms` for its idempotency key, but nothing upstream tells the Correlation Worker when an encounter "started" versus "is still going." This is that decision: given a confirming close-ping for a pair right now, is it part of an episode already in progress, or the beginning of a new one? `touchProximityEpisode` answers that and hands back the `episode_start_ms` to use either way.

---

## Concepts

### TTL as the gap detector, not a scan

Signal-loss detection needs an active scan because there's no natural "entity went silent" event. Proximity gap detection doesn't need one: as long as a pair keeps being found close together, each confirmation renews the key's TTL; the moment confirmations stop, Redis expires the key on its own, with no polling required. The TTL duration (`PROXIMITY_EPISODE_GAP_MS`) is the tolerance for a missed or delayed ping before the encounter is considered over.

### Why this needs a Lua script, not separate `EXISTS`/`HSET` calls

Correlation Worker instances aren't behind a single-leader lease the way the Alert Evaluator is — nothing stops two instances (or two consumer partitions) from processing confirmations for the same pair at nearly the same moment. If "check whether an episode exists" and "create or refresh it" were two separate round trips, both instances could see "no episode" and both create one, defeating the entire purpose of episode identity. The Lua script makes check-and-act one atomic Redis operation.

### The out-of-order case: renew the TTL, don't move `last_seen_ms` backward

A confirmation with an older source timestamp than what's already stored is treated as a duplicate/out-of-order arrival for `last_seen_ms` purposes — that field only ever moves forward. But the TTL still gets renewed, because the fact that *any* confirmation for this pair was just processed is itself real evidence the encounter is active, independent of that specific message's ordering.

### Why there's no separate replay-guard field

`deviation-state` needs a distinct `last_processed_ms` field because it guards a derived counter (`count`) that a stale replay could otherwise corrupt. Here, `last_seen_ms` is the only field being protected, so guarding it against moving backward is the whole guard — no second field is needed.

---

## Failure modes

**Separate `EXISTS` + `HSET` instead of one atomic script.** Two instances processing the same pair around the same instant could both observe no existing episode and both treat their confirmation as the start of a new one — silently producing two `episode_start_ms` values, two Neo4j edges, and potentially two alerts for one real encounter.

**Letting an out-of-order confirmation move `last_seen_ms` backward.** Would make "latest confirmed contact" an inaccurate, non-monotonic value — the same class of bug the live-state and deviation-state guards elsewhere in this codebase already exist to prevent.

---

## Map to code

| Concept | Where |
| --- | --- |
| Episode touch | `touchProximityEpisode` — `services/correlation-worker/src/episode.ts` |
| Gap TTL | `PROXIMITY_EPISODE_GAP_MS` — `services/correlation-worker/src/config.ts` |
| Redis key | `proximity-episode:{pair_key}` |

---

## Retention questions

1. Why does an episode's end get detected by TTL expiry instead of an explicit scan or close signal?
2. Why must "check for an existing episode" and "create or refresh it" happen as one atomic operation here?
3. Why does an out-of-order confirmation still renew the TTL even though it doesn't update `last_seen_ms`?
4. Why doesn't this need a separate guard field the way `deviation-state`'s `last_processed_ms` does?

---

## Completion checklist

- [ ] I can explain why TTL expiry replaces a scan for proximity gap detection
- [ ] I can explain the race two concurrent instances would hit without the Lua script
- [ ] I can explain why `last_seen_ms` is guarded but the TTL renewal isn't
- [ ] I ran the integration suite (including the real TTL-expiry test) against real Redis and can interpret each test
