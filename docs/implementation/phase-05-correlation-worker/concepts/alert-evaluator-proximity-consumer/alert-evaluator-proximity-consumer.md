# Alert Evaluator Proximity Consumer — Design and Learning Reference

---

## What this stage does

Turns a `proximity.candidates` message into a durable, deterministic `UNSCHEDULED_PROXIMITY` alert on the `alerts` topic — the same topic and downstream API path signal-loss already proved end-to-end. This is the Alert Evaluator's first Kafka *consumer*; until now it only produced, driven entirely by its own timer-based Redis scan.

---

## Concepts

### Why this consumer needs no leader election, unlike the scan

The signal-loss scan runs on a timer with no natural partitioning — every instance's clock ticks independently, so without a lease, every instance would scan and alert redundantly. A Kafka consumer is different: Kafka's own consumer-group protocol already assigns each partition to exactly one group member at a time. Two `alert-evaluator` instances both consuming `proximity.candidates` will each get disjoint partitions, not the same messages — there's nothing for a Redis lease to additionally coordinate. Leader election is specific to the scan's lack of built-in partitioning, not a blanket rule for everything this service does.

### No repeated `KNOWN_ASSOCIATE` check

By the time a message reaches this topic, the Correlation Worker has already confirmed exact distance, episode novelty, and the absence of a `KNOWN_ASSOCIATE` relationship. Re-checking Neo4j here would duplicate work for a fact that's already established, and would reintroduce a Neo4j dependency this service's own ADR (implicitly) avoids — the Alert Evaluator does not read Neo4j in the v1 contract.

### Why `entity_type` is looked up from Redis instead of carried on the message

`proximity.candidates`'s documented schema doesn't include entity type — the Correlation Worker doesn't need it for anything it does. But the `alerts` table's `entity_type` column is `NOT NULL`. Rather than adding a field to the candidate schema for one consumer's benefit, this reads `entity:live:{entity_id}`, the same source signal-loss already reads for its own last-known-state fields — one canonical place for "facts about an entity," not scattered across every producer's payload.

### Deterministic alert construction from already-canonical inputs

`entity_a_id`/`entity_b_id` arrive already ordered by the Correlation Worker's canonical pair key, so this consumer doesn't need to re-derive an ordering — it just assigns `entity_a_id` to the alert's `entity_id` and `entity_b_id` to `counterparty_entity_id`, consistently, every time. Combined with the deterministic `alert_id` (`{pair_key}:UNSCHEDULED_PROXIMITY:{episode_start_ms}`), a redelivered candidate for the same episode produces byte-for-byte the same alert identity — the API's `ON CONFLICT (alert_id) DO NOTHING` absorbs the duplicate exactly like it already does for signal-loss.

### Why `consumer.run()` doesn't block the leader loop

`run()` resolves once the internal fetch loop has started, not once message processing is "done" — it never finishes on its own. `main()` can `await` it and then continue into the pre-existing leader-election `while (true)` loop; both run concurrently from that point on, one driven by Kafka's fetch loop, the other by the lease timer.

---

## Failure modes

**Adding a Redis-lease gate to this consumer "for consistency" with the scan.** Would be redundant machinery solving a problem Kafka's partition assignment already solves, and would incorrectly couple proximity-alert throughput to leadership status for no reason.

**Re-querying `KNOWN_ASSOCIATE` here.** Wasted Neo4j round trip for information already resolved, and a step toward the kind of coupling ADR-003's consequences explicitly avoid ("Alert Evaluator consumes already-filtered `proximity.candidates`; it does not repeat the known-associate lookup").

**Letting `entity_type` be `null` when the entity has no live state.** Would violate the `alerts.entity_type NOT NULL` constraint at insert time on the API side. Defaulted to `''` instead, matching the exact pattern the signal-loss path already uses for the same column.

---

## Map to code

| Concept | Where |
| --- | --- |
| Candidate handler | `handleProximityCandidate` — `services/alert-evaluator/src/evaluator.ts` |
| Consumer wiring | `proximityConsumer` setup in `main()` — same file |
| Consumer group | `GROUP_ID: 'alert-evaluator'` — `services/alert-evaluator/src/config.ts` |

---

## Retention questions

1. Why doesn't this consumer need the same Redis-lease leader election the signal-loss scan uses?
2. Why doesn't `handleProximityCandidate` query Neo4j for `KNOWN_ASSOCIATE` again?
3. Why is `entity_type` read from `entity:live:*` instead of being added to the `proximity.candidates` schema?
4. Why does `entity_type` default to `''` rather than `null` when the entity has no live state?
5. Why can `main()` `await proximityConsumer.run(...)` and still reach the leader-election loop afterward?

---

## Completion checklist

- [ ] I can explain why Kafka consumer groups make leader election unnecessary here
- [ ] I can explain why this consumer trusts `proximity.candidates` without re-checking `KNOWN_ASSOCIATE`
- [ ] I can explain the `entity_type` default and which real constraint it protects
- [ ] I ran the integration suite against real Redis and the real `alerts` topic and can interpret each test
- [ ] I ran the real service end-to-end: a real `proximity.candidates` message produced a real `UNSCHEDULED_PROXIMITY` alert on the real `alerts` topic
