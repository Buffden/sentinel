import type { Session } from 'neo4j-driver';

// Undirected pattern: KNOWN_ASSOCIATE existence doesn't depend on which
// direction it was created in or which order the two entities are passed --
// verified directly against Neo4j (see concepts/neo4j-proximity-event).
// Unlike PROXIMITY_EVENT, there's no uniqueness constraint or MERGE here,
// only a read, so canonical ordering isn't needed for correctness.
export async function isKnownAssociate(
	session: Session,
	entityIdA: string,
	entityIdB: string,
): Promise<boolean> {
	const result = await session.executeRead((tx) =>
		tx.run(
			'MATCH (:Entity {id: $a})-[:KNOWN_ASSOCIATE]-(:Entity {id: $b}) RETURN count(*) > 0 AS exists',
			{ a: entityIdA, b: entityIdB },
		),
	);
	return result.records[0]?.get('exists') === true;
}
