import { greatCircleDistance } from 'h3-js';
import type { Redis } from 'ioredis';

export interface ProximityMatch {
	entityId: string;
	entityType: string;
	lat: number;
	lon: number;
	distanceMetres: number;
}

// Exact-distance filter over candidates the H3 lookup already over-fetched.
// Looks up each candidate's live lat/lon/entity_type (entity:live:*, written
// by the Position Consumer) and keeps only those within maxDistanceMetres of
// (originLat, originLon). A candidate with no live position or type -- hash
// missing, or a field empty -- is skipped rather than treated as a match or
// an error, the same defensive pattern the Alert Evaluator uses for
// entity:live reads. Position and type are returned, not just the ID,
// because the caller needs them to record graph evidence and compute a
// detection midpoint without a second Redis round trip.
export async function filterByDistance(
	redis: Redis,
	originLat: number,
	originLon: number,
	candidateIds: string[],
	maxDistanceMetres: number,
): Promise<ProximityMatch[]> {
	const matches = await Promise.all(
		candidateIds.map(async (entityId): Promise<ProximityMatch | null> => {
			const [latRaw, lonRaw, entityType] = await redis.hmget(
				`entity:live:${entityId}`,
				'lat',
				'lon',
				'entity_type',
			);
			if (!latRaw || !lonRaw || !entityType) return null;

			const lat = Number(latRaw);
			const lon = Number(lonRaw);
			if (!Number.isFinite(lat) || !Number.isFinite(lon)) return null;

			const distanceMetres = greatCircleDistance([originLat, originLon], [lat, lon], 'm');
			if (distanceMetres > maxDistanceMetres) return null;

			return { entityId, entityType, lat, lon, distanceMetres };
		}),
	);

	return matches
		.filter((m): m is ProximityMatch => m !== null)
		.sort((a, b) => a.distanceMetres - b.distanceMetres);
}
