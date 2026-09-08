# Phase 05 Concepts

Concept notes and debrief records, in the order you'd read them while working through this phase.

| Folder | Observable result |
| --- | --- |
| [h3-candidate-lookup/](h3-candidate-lookup/) | `findProximityCandidates` correctly finds a same-cell candidate, correctly finds a boundary-crossing candidate only once k reaches the ring it's actually in, and correctly excludes stale and self members, verified against real Redis |
| [exact-distance-filtering/](exact-distance-filtering/) | `filterByDistance` keeps a real close candidate, excludes a real far one, skips candidates with no live position instead of throwing, and sorts by distance, verified against real Redis |
| [canonical-pair-ordering/](canonical-pair-ordering/) | `canonicalPairKey` produces the same key regardless of which entity triggered the lookup |
| [neo4j-proximity-event/](neo4j-proximity-event/) | `mergeProximityEvent` creates one idempotent, direction-safe `PROXIMITY_EVENT` edge per episode, tightens `min_distance_metres` without ever increasing it, verified against real Neo4j |
| [proximity-episode-state/](proximity-episode-state/) | `touchProximityEpisode` starts a new episode, recognizes an existing one, guards against out-of-order confirmations, and starts fresh after a real TTL expiry, verified against real Redis |
| [candidate-publication-gate/](candidate-publication-gate/) | `evaluateProximityEncounter` publishes once per unscheduled episode, retries after an unconfirmed attempt, and never publishes for a known associate while still recording evidence, verified against real Redis and Neo4j together |
| [service-assembly/](service-assembly/) | `handlePosition` wires candidate lookup through the publish gate into a real Kafka consumer/producer; a real `position.normalized` message run through the real service produces a real `proximity.candidates` message with matching Redis and Neo4j evidence |
| [alert-evaluator-proximity-consumer/](alert-evaluator-proximity-consumer/) | `handleProximityCandidate` turns a real `proximity.candidates` message into a deterministic `UNSCHEDULED_PROXIMITY` alert on the real `alerts` topic, running concurrently with the leader-gated signal-loss scan with no additional coordination needed |
