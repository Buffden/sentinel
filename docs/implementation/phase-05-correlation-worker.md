# Phase 05 — Correlation Worker

## Goal

Detect entity proximity efficiently and build the relationship graph in Neo4j.

```text
position.normalized
        ↓
Correlation Worker
        ↓
H3 candidate lookup (geo-cell:* sorted sets)
        ↓
exact distance check
        ↓
Neo4j PROXIMITY_EVENT edge
        ↓
proximity.candidates (one event per new episode)
```

Start with two synthetic entities moving toward each other.

Do not start with scale.

---

## Learning Goals

- spatial indexing with H3 and why it reduces candidate space
- canonical pair ordering (`min(a,b):max(a,b)`) and why it is necessary
- proximity episode model — one episode per continuous encounter, not one per ping
- graph persistence and `MERGE` idempotency in Neo4j
- partial failure between Neo4j write and Kafka publish
- `candidate_published` flag as a Kafka publish recovery mechanism

---

## Suggested Checkpoints

1. H3 cell lookup: confirm two nearby entities appear in the same or adjacent cells
2. Exact distance check filters candidates below `PROXIMITY_THRESHOLD_METRES`
3. First detection: Neo4j MERGE writes a `PROXIMITY_EVENT` edge
4. `proximity-episode:{pair_key}` hash is created in Redis; one `proximity.candidates` event is published
5. Subsequent pings within the same episode: `last_seen_ms` updated, TTL refreshed, no new Kafka event
6. Known-associate pair: Neo4j edge written, episode hash created, no `proximity.candidates` published
7. Episode expires (TTL); pair comes close again — confirm a new episode starts with a new edge

---

## Required Failure Experiments

- same proximity pair triggered from both entity A's ping and entity B's ping — confirm canonical pair ordering produces one episode hash, not two
- Neo4j write succeeds but Kafka publish fails — confirm `candidate_published=0` persists and the next ping retries the publish
- Kafka event delivered twice — confirm no second episode is created
- known-associate pair — confirm no event appears in `proximity.candidates` regardless of proximity duration

---

## Exit Criteria

- two synthetic entities converging produces exactly one `proximity.candidates` event per encounter
- Neo4j Browser shows one `PROXIMITY_EVENT` edge per episode
- `redis-cli` shows `proximity-episode:{pair_key}` hash with correct fields and TTL
- known-associate pairs produce Neo4j edges but no `proximity.candidates` events
- developer can verify all of the above without reading the code
