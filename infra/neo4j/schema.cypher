// Neo4j 5.7+ Community Edition supports uniqueness constraints on both node and
// relationship properties. Both constraints below are database-enforced.
//
// The backing index for each constraint is created automatically.
// Do NOT add separate indexes on the same properties -- that would be redundant.

// Entity.id is the canonical logical identity for every tracked entity.
// Required so MERGE (e:Entity {id: $id}) is efficient and duplicate-safe.
CREATE CONSTRAINT entity_id_unique IF NOT EXISTS
  FOR (e:Entity)
  REQUIRE e.id IS UNIQUE;

// PROXIMITY_EVENT.idempotency_key encodes one proximity episode as {pair_key}:{episode_start_ms}.
// The DB constraint prevents duplicate edges at the storage level.
// The Correlation Worker still uses MERGE for replay-safe find-or-create behavior:
// MERGE finds an existing edge if present rather than always creating a new one.
// The constraint and MERGE solve related but different problems.
CREATE CONSTRAINT proximity_event_idempotency_key_unique IF NOT EXISTS
  FOR ()-[r:PROXIMITY_EVENT]-()
  REQUIRE r.idempotency_key IS UNIQUE;
