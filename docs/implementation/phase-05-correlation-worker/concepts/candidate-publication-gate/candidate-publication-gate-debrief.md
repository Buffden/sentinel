# Candidate Publication Gate Debrief

---

## Experiment: `evaluateProximityEncounter` against real Redis and Neo4j together

Five integration tests:

```text
Test Files  6 passed (6)
     Tests  31 passed (31)
```

(26 from earlier checkpoints, 5 new.)

| Test | Proves |
| --- | --- |
| publishes a candidate for a brand-new unscheduled episode | Baseline: new episode, no known associate, `candidate_published` set to `'0'` |
| records graph evidence but never publishes for a known-associate pair | Evidence is written either way; publish is suppressed; the field is never set at all |
| does not re-publish a confirmed candidate on a later ping | Once `markCandidatePublished` runs, a later ping in the same episode reports `shouldPublishCandidate: false` |
| retries publishing on a later ping if the previous attempt never confirmed | Field left at `'0'` (simulating a crash/failed publish) causes a later ping to report `shouldPublishCandidate: true` again |
| recognizes an existing known-associate episode without re-querying | A later ping correctly reports `isKnownAssociate: true` from the missing field alone, no second graph read needed |

One real bug caught during this checkpoint, not assumed away: the first version of the known-associate test fixture used `MATCH (a:Entity {id:$a}), (b:Entity {id:$b})` to set up the relationship, but at that point in the test the `Entity` nodes didn't exist yet — `MATCH` against nonexistent nodes silently matches zero rows and the following `MERGE (a)-[:KNOWN_ASSOCIATE]->(b)` never executes, no error. Confirmed directly:

```text
MATCH (a:Entity {id: 'nonexistent-a'}), (b:Entity {id: 'nonexistent-b'})
MERGE (a)-[:KNOWN_ASSOCIATE]->(b)
RETURN a, b;
```
returned nothing — no rows, no error. Fixed by using `MERGE` for the entities in the test fixture too. A production code path never hits this, since `mergeProximityEvent` already `MERGE`s both entities before anything else runs, but it's a sharp edge worth knowing: `MATCH` on a pattern with a filter that matches nothing is not an error in Cypher, it's silently zero rows.

Cleanup verified: no leftover test nodes in Neo4j, no leftover test keys in Redis.

---

## Engineering debrief

**Data flow:** `evaluateProximityEncounter` runs `touchProximityEpisode` and `mergeProximityEvent` unconditionally, then branches only on `isNewEpisode`. For a new episode, it checks `KNOWN_ASSOCIATE` once and either stops there (known associate) or marks the episode pending and reports `shouldPublishCandidate: true`. For an existing episode, it reads back the `candidate_published` field and infers both "is this known" (field absent) and "does this need a retry" (field is `'0'`) from that one read.

**Trade-off:** the known-associate check costs one Neo4j round trip per new episode, not per ping — accepted because episode identity, not ping frequency, is what actually needs that answer.

**Failure behaviour:** the field-ordering (`markCandidatePending` before the Kafka publish, `markCandidatePublished` only after) is what makes a crash mid-publish recoverable as a retry rather than a silent loss, mirroring the same before/after gate pattern already used for the Alert Evaluator's episode gates.

## Manual inspection commands

```bash
# Inspect an episode's publish state directly
docker exec sentinel-redis redis-cli HGETALL "proximity-episode:<pair_key>"

# Confirm evidence exists regardless of known-associate status
docker exec sentinel-neo4j cypher-shell -u neo4j -p sentinel-dev \
  "MATCH (a:Entity)-[r:PROXIMITY_EVENT]-(b:Entity) RETURN a.id, b.id, r.idempotency_key"

# Run the full decision-layer integration suite
cd services/correlation-worker && node_modules/.bin/vitest run src/proximityDecision.integration.test.ts
```

## Knowledge-check questions

1. Why do episode timing and graph evidence get written even for a pair that turns out to be a known associate?
2. How does a later ping on an existing episode tell "known associate" apart from "unscheduled, not yet published" using only one field?
3. What Cypher behavior caused the first version of this checkpoint's own test fixture to silently fail, and why didn't it throw an error?

## Next

Wire `findProximityCandidates` → `filterByDistance` → `evaluateProximityEncounter` → (Kafka publish on `shouldPublishCandidate`) into the actual `position.normalized` consumer loop — the piece that turns these tested functions into a running service.

---

## Key observations

| Concept | Observed |
| --- | --- |
| New unscheduled episode | `candidate_published` set to `'0'`, `shouldPublishCandidate: true` |
| New known-associate episode | Evidence written, no `candidate_published` field, `shouldPublishCandidate: false` |
| Existing episode, already published | `shouldPublishCandidate: false` |
| Existing episode, publish never confirmed | `shouldPublishCandidate: true` (retry) |
| Existing known-associate episode | Recognized from the missing field alone |
| `MATCH` on a nonexistent node pattern | Silently zero rows, not an error — caught by a failing test, not assumed |
| Test cleanup (Redis + Neo4j) | PASS |
