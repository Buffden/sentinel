import type { Session } from 'neo4j-driver';
import { canonicalPairKey } from './pair.js';

export interface ProximityEntity {
	id: string;
	type: string;
}

export interface ProximityDetection {
	episodeStartMs: number;
	lastSeenMs: number;
	distanceMetres: number;
	lat: number;
	lon: number;
}

// Idempotent, replay-safe write of one proximity episode edge. MERGE's
// pattern match is directional even though the idempotency_key uniqueness
// constraint is not: retrying with the two entities swapped but the same
// episode would otherwise attempt to create a second directed relationship
// and hit the uniqueness constraint as a hard error, not a no-op (verified
// directly against Neo4j). Node order is therefore always canonicalized
// here, regardless of which entity's ping triggered the call.
//
// A repeat call for the same episode (same pair, same episode_start_ms)
// refreshes last_seen_ms and tightens min_distance_metres if this
// observation is closer than any seen before; it never lets that value
// increase. Deciding whether a given ping still belongs to the same episode
// or starts a new one is Redis proximity-episode state's job, not this one.
export async function mergeProximityEvent(
	session: Session,
	entityA: ProximityEntity,
	entityB: ProximityEntity,
	detection: ProximityDetection,
): Promise<void> {
	const [first, second] = entityA.id <= entityB.id ? [entityA, entityB] : [entityB, entityA];
	const idempotencyKey = `${canonicalPairKey(entityA.id, entityB.id)}:${detection.episodeStartMs}`;

	await session.executeWrite((tx) =>
		tx.run(
			`
			MERGE (a:Entity {id: $minId})
			ON CREATE SET a.type = $minType
			MERGE (b:Entity {id: $maxId})
			ON CREATE SET b.type = $maxType
			MERGE (a)-[r:PROXIMITY_EVENT {idempotency_key: $idempotencyKey}]->(b)
			ON CREATE SET
				r.episode_start_ms = $episodeStartMs,
				r.last_seen_ms = $lastSeenMs,
				r.min_distance_metres = $distanceMetres,
				r.lat = $lat,
				r.lon = $lon,
				r.distance_at_detection = $distanceMetres
			ON MATCH SET
				r.last_seen_ms = $lastSeenMs,
				r.min_distance_metres = CASE
					WHEN $distanceMetres < r.min_distance_metres THEN $distanceMetres
					ELSE r.min_distance_metres
				END
			`,
			{
				minId: first.id,
				minType: first.type,
				maxId: second.id,
				maxType: second.type,
				idempotencyKey,
				episodeStartMs: detection.episodeStartMs,
				lastSeenMs: detection.lastSeenMs,
				distanceMetres: detection.distanceMetres,
				lat: detection.lat,
				lon: detection.lon,
			},
		),
	);
}
