# Canonical Pair Ordering Debrief

---

## Experiment: `canonicalPairKey` unit tests

Pure logic, no infrastructure needed:

```text
Test Files  3 passed (3)
     Tests  15 passed (15)
```

(11 from earlier checkpoints, 4 new.)

| Test | Proves |
| --- | --- |
| orders two entity IDs alphabetically | Basic ordering works |
| produces the same key regardless of argument order | A-finds-B and B-finds-A collapse to one key — the actual point of this stage |
| compares as strings, not numbers | `'9'` and `'10'` order lexicographically, matching how entity IDs are treated everywhere else |
| produces different keys for different pairs | No accidental collisions between distinct pairs |

---

## Engineering debrief

**Data flow:** given two entity IDs in either order, `canonicalPairKey` compares them as strings and returns `{min}:{max}`. No I/O, no state.

**Trade-off:** none of real weight — this is the simplest possible deterministic-identity function, deliberately not a hash, since human-readable pair keys make Redis/Neo4j inspection during debugging easier than an opaque digest would.

**Failure behaviour:** none to speak of at this stage — the function is pure and total over any two strings. Its correctness matters entirely for what's built on top of it next.

## Manual inspection commands

```bash
cd services/correlation-worker && node_modules/.bin/vitest run src/pair.test.ts
```

## Knowledge-check questions

1. Why does an A-finds-B lookup and a B-finds-A lookup need to produce the same key?
2. What would go wrong downstream (episode state, Neo4j evidence) if this function used the caller's argument order instead of comparing?

## Next

Neo4j `MERGE` for one proximity episode, keyed by `{pair_key}:{episode_start_ms}` — this is the first place the canonical pair key actually gets used.
