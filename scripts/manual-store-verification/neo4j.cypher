// Neo4j manual store verification
// Password comes from NEO4J_AUTH in .env (format: neo4j/<password>)
// Run: docker exec -i sentinel-neo4j cypher-shell -u neo4j -p <password> < scripts/manual-store-verification/neo4j.cypher

// Verify Neo4j uniqueness constraints are in place
SHOW CONSTRAINTS;

// Create the two Entity nodes (MERGE = find-or-create)
MERGE (:Entity {id: 'test-aircraft', type: 'aircraft'});
MERGE (:Entity {id: 'test-vessel', type: 'vessel'});

// Create KNOWN_ASSOCIATE relationship
MATCH (a:Entity {id: 'test-aircraft'}), (b:Entity {id: 'test-vessel'})
CREATE (a)-[:KNOWN_ASSOCIATE {
  established_at: 1700000000000,
  relationship_type: 'same-fleet'
}]->(b);

// Query KNOWN_ASSOCIATE
MATCH (a:Entity)-[r:KNOWN_ASSOCIATE]->(b:Entity)
WHERE a.id = 'test-aircraft'
RETURN a.id AS entity_a, b.id AS entity_b,
  type(r) AS rel_type, r.established_at AS established_at;

// Create PROXIMITY_EVENT
MATCH (a:Entity {id: 'test-aircraft'}), (b:Entity {id: 'test-vessel'})
CREATE (a)-[:PROXIMITY_EVENT {
  idempotency_key: 'test-pair:1234567890',
  episode_start_ms: 1234567890,
  last_seen_ms: 1234567890,
  min_distance_metres: 85.5,
  lat: 51.5074,
  lon: -0.1278,
  distance_at_detection: 85.5
}]->(b);

// Query PROXIMITY_EVENT
MATCH (a:Entity)-[r:PROXIMITY_EVENT]->(b:Entity)
RETURN a.id AS entity_a, b.id AS entity_b,
  r.idempotency_key AS key,
  r.episode_start_ms AS start_ms,
  r.min_distance_metres AS min_dist_m,
  r.lat AS lat, r.lon AS lon;

// Attempt duplicate CREATE on same idempotency_key (will fail with constraint violation)
MATCH (a:Entity {id: 'test-aircraft'}), (b:Entity {id: 'test-vessel'})
CREATE (a)-[:PROXIMITY_EVENT {
  idempotency_key: 'test-pair:1234567890'
}]->(b);

// MERGE on same idempotency_key (replay-safe: finds existing edge, updates last_seen_ms)
MATCH (a:Entity {id: 'test-aircraft'}), (b:Entity {id: 'test-vessel'})
MERGE (a)-[r:PROXIMITY_EVENT {idempotency_key: 'test-pair:1234567890'}]->(b)
ON MATCH SET r.last_seen_ms = 1234568500;

// Failure test: invalid function reference
MATCH (n:Entity) WHERE n.nonexistent_function() RETURN n;

// Cleanup: remove both nodes and all attached relationships
MATCH (n:Entity) WHERE n.id STARTS WITH 'test-' DETACH DELETE n;
