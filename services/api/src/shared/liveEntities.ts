// Shared Redis entity:live:* scan, extracted from GET /entities/live (CP1's
// pre-Phase-09 implementation) so GET /entities (Phase 09 CP1) can reuse the
// exact same scan/parse mechanism with a different inclusion predicate,
// instead of a second hand-copied SCAN loop.
import { redis } from '../redis.js';
import { config } from '../config.js';

export interface LiveEntity {
	entity_id: string;
	lat: number;
	lon: number;
	altitude_m: number | null;
	speed_mps: number | null;
	course_deg: number | null;
	last_seen_ms: number;
	entity_type: string | null;
	entity_subtype: string | null;
	callsign: string | null;
	on_ground: boolean | null;
}

function parseFloat_(val: string | undefined): number | null {
	if (!val || val === '') return null;
	const n = parseFloat(val);
	return isFinite(n) ? n : null;
}

function nullIfEmpty(val: string | undefined): string | null {
	return val === '' || val === undefined ? null : val;
}

// Scans entity:live:* once, keeping only entities with a parseable position,
// fresh enough to pass the staleness cutoff, and accepted by `predicate`.
// Callers supply the predicate rather than a bbox/scope object directly --
// GET /entities/live filters by an ad-hoc viewport bbox, GET /entities
// filters by the operator's saved workspace scope, and the two shapes don't
// otherwise share a common type.
export async function scanLiveEntities(
	predicate: (entity: LiveEntity) => boolean,
): Promise<LiveEntity[]> {
	const nowMs = Date.now();
	const staleCutoffMs = nowMs - config.LIVE_ENTITY_STALE_AFTER_MS;

	const entities: LiveEntity[] = [];
	let cursor = '0';

	do {
		const [nextCursor, keys] = await redis.scan(
			cursor,
			'MATCH',
			'entity:live:*',
			'COUNT',
			config.REDIS_SCAN_COUNT,
		);
		cursor = nextCursor;

		for (const key of keys) {
			if (entities.length >= config.LIVE_ENTITIES_MAX) break;

			const hash = await redis.hgetall(key);
			if (!hash) continue;

			const lat = parseFloat_(hash['lat']);
			const lon = parseFloat_(hash['lon']);
			if (lat === null || lon === null) continue;

			const lastSeenMs = parseFloat_(hash['last_seen_ms']);
			if (lastSeenMs === null || lastSeenMs < staleCutoffMs) continue;

			const entity: LiveEntity = {
				entity_id: key.replace('entity:live:', ''),
				lat,
				lon,
				altitude_m: parseFloat_(hash['altitude_m']),
				speed_mps: parseFloat_(hash['speed_mps']),
				course_deg: parseFloat_(hash['course_deg']),
				last_seen_ms: lastSeenMs,
				entity_type: nullIfEmpty(hash['entity_type']),
				entity_subtype: nullIfEmpty(hash['entity_subtype']),
				callsign: nullIfEmpty(hash['callsign']),
				on_ground:
					hash['on_ground'] === 'true' ? true : hash['on_ground'] === 'false' ? false : null,
			};

			if (predicate(entity)) entities.push(entity);
		}
	} while (cursor !== '0' && entities.length < config.LIVE_ENTITIES_MAX);

	return entities;
}
