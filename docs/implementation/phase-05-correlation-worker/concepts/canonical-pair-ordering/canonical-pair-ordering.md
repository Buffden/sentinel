# Canonical Pair Ordering — Design and Learning Reference

---

## What this solves

Proximity is symmetric — if A is close to B, B is close to A. But candidate lookup runs once per incoming position, so the same encounter surfaces twice: once when A's ping finds B, once when B's ping finds A. Without a shared identity, that's two episodes, two graph edges, two alerts for one real-world encounter. `canonicalPairKey` collapses both directions to one key so everything downstream (episode state, Neo4j evidence, the alert itself) is keyed once per pair.

---

## Concepts

### Why plain string ordering, not a hash or a sorted tuple stored separately

The key only needs to be *deterministic given the same two IDs, regardless of order* — it doesn't need to be unguessable or short. Comparing the two ID strings and always putting the lexicographically smaller one first is enough, and it's the same technique the rest of this codebase already uses for deterministic identity (e.g. alert IDs built from concatenated fields, not hashes).

### Lexicographic, not numeric

Entity IDs (ICAO24 hex, MMSI, synthetic IDs) are opaque strings everywhere else in this codebase — never parsed as numbers. Ordering them the same way here means `'9'` sorts after `'10'`, which looks wrong at a glance but is exactly the same rule applied consistently, not an inconsistency to fix.

---

## Failure modes

**Building the key from application-supplied order instead of comparing.** If the caller's own argument order (e.g. "whoever triggered the lookup goes first") leaked into the key, A-finds-B and B-finds-A would produce two different keys — defeating the entire purpose of this stage.

---

## Map to code

| Concept | Where |
| --- | --- |
| Pair key | `canonicalPairKey` — `services/correlation-worker/src/pair.ts` |

---

## Retention questions

1. Why does an A-finds-B lookup and a B-finds-A lookup need to produce the same key?
2. Why lexicographic string comparison instead of numeric comparison?

---

## Completion checklist

- [ ] I can explain why proximity detection needs a canonical pair identity at all
- [ ] I can explain why the ordering rule is string comparison, not numeric
