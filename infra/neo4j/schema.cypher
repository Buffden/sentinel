// Community Edition constraint: uniqueness constraints are supported on node
// properties only. Relationship property uniqueness constraints are NOT
// available in Community Edition.
//
// PROXIMITY_EVENT episode identity ({pair_key}:{episode_start_ms}) is therefore
// enforced by deterministic application MERGE in the Correlation Worker (Phase 05),
// not by a database schema constraint.
//
// The backing index for Entity.id is created automatically by the uniqueness
// constraint below. Do NOT add a separate index on Entity.id -- it would be redundant.

CREATE CONSTRAINT entity_id_unique IF NOT EXISTS
  FOR (e:Entity)
  REQUIRE e.id IS UNIQUE;
