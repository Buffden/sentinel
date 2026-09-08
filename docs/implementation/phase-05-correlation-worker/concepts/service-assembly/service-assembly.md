# Service Assembly — Design and Learning Reference

---

## What this stage does

Every earlier piece (candidate lookup, distance filter, pair ordering, Neo4j evidence, episode state, publish gate) was a tested, standalone function. Nothing yet consumed a real `position.normalized` message or called a real Kafka producer as part of a live loop. This stage is that assembly: a Kafka consumer that turns an incoming position into calls against the pieces already built, and a producer that publishes `proximity.candidates` when the decision layer says to.

---

## Concepts

### Splitting "handle one message" from "run the consumer loop"

`handlePosition` contains all the actual logic and is directly testable against real Redis/Neo4j/Kafka. `run()` is the Kafka wiring: connect, subscribe, commit offsets. This mirrors Position Consumer and Alert Evaluator exactly — both keep their real logic in a plain async function and leave the surrounding consumer/producer plumbing untested, since that plumbing is kafkajs's own well-tested responsibility, not this service's.

### Candidate freshness is relative to event time, not wall-clock time

The freshness lower bound for candidate lookup (`CANDIDATE_FRESHNESS_MS`) is computed as `position.timestamp_ms - CANDIDATE_FRESHNESS_MS`, using the *incoming message's own source event time*, not `Date.now()`. Anchoring to wall-clock time would make freshness meaningless during any kind of backlog processing (a burst of catch-up messages would all compute freshness against "right now," incorrectly treating simultaneously-arriving old positions as mutually fresh, or excluding genuinely fresh candidates if the consumer is simply running a few seconds behind). Anchoring to the message's own timestamp keeps the freshness window meaningful regardless of processing lag.

### One Neo4j session for the process, not one per message

This consumer processes messages sequentially — kafkajs's `eachMessage` awaits the full handler before fetching the next message, with no concurrent partition consumption configured. There is no concurrency for separate sessions to isolate, so creating one session at startup and reusing it avoids per-message session overhead for no isolation benefit.

### Why `entity_a_id`/`entity_b_id` are computed again here, not parsed from `pair_key`

`evaluateProximityEncounter` already computed the canonical pair key internally, but the Kafka payload's `entity_a_id`/`entity_b_id` fields are computed by comparing the two entity IDs directly again, not by splitting `pair_key` on `:`. Entity IDs are opaque strings; nothing guarantees one could never contain a colon. Comparing the actual values is one line and has no such fragility.

---

## Failure modes

**Parsing an unparseable or malformed Kafka message as if it were valid.** Handled at the boundary: `parsePosition` returns `null` for anything that fails a basic shape check, and the consumer logs a warning and moves on (still committing the offset) rather than throwing and blocking the whole partition on one bad message.

**Freshness anchored to `Date.now()` instead of the message's own timestamp.** Would silently produce wrong candidate sets under any processing lag or backlog replay.

---

## Map to code

| Concept | Where |
| --- | --- |
| Testable message handler | `handlePosition` — `services/correlation-worker/src/worker.ts` |
| Consumer/producer wiring | `run()` — same file |
| Boundary validation | `parsePosition` — same file |
| Midpoint for detection location | `computeMidpoint` — `services/correlation-worker/src/midpoint.ts` |

---

## Manual end-to-end proof (already run — see debrief)

The real service was started against the real dev stack, a real `position.normalized` message was produced via `rpk`, and a real `proximity.candidates` message was consumed back out — with matching evidence in Redis (`proximity-episode:*`) and Neo4j (`PROXIMITY_EVENT`). This is the one piece no unit or integration test alone proves: that the assembled service, run as a process against the real brokers, actually does the whole thing.

---

## Retention questions

1. Why is `handlePosition` tested directly while `run()` is not?
2. Why does candidate freshness use the incoming message's own `timestamp_ms` instead of `Date.now()`?
3. Why is one Neo4j session reused for the life of the process here, when `proximityDecision.integration.test.ts` also does this — is that a coincidence?
4. What happens to a single malformed `position.normalized` message, and why doesn't it block the rest of the partition?

---

## Completion checklist

- [ ] I can explain the split between `handlePosition` and `run()` and why it mirrors other services in this codebase
- [ ] I can explain why freshness is anchored to event time, not wall-clock time
- [ ] I ran the real service against the real dev stack and watched a real candidate come out the other side
- [ ] I can state what the whole Correlation Worker pipeline does, end to end, in one paragraph
