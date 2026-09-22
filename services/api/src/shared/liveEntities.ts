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

// Shared hash-to-entity parse, used both by the list scan (which then also
// applies a staleness cutoff) and by a direct by-id lookup (which
// deliberately does not -- see getLiveEntity below).
function hashToLiveEntity(entityId: string, hash: Record<string, string>): LiveEntity | null {
	const lat = parseFloat_(hash['lat']);
	const lon = parseFloat_(hash['lon']);
	if (lat === null || lon === null) return null;

	const lastSeenMs = parseFloat_(hash['last_seen_ms']);
	if (lastSeenMs === null) return null;

	return {
		entity_id: entityId,
		lat,
		lon,
		altitude_m: parseFloat_(hash['altitude_m']),
		speed_mps: parseFloat_(hash['speed_mps']),
		course_deg: parseFloat_(hash['course_deg']),
		last_seen_ms: lastSeenMs,
		entity_type: nullIfEmpty(hash['entity_type']),
		entity_subtype: nullIfEmpty(hash['entity_subtype']),
		callsign: nullIfEmpty(hash['callsign']),
		on_ground: hash['on_ground'] === 'true' ? true : hash['on_ground'] === 'false' ? false : null,
	};
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
	const staleCutoffMs = Date.now() - config.LIVE_ENTITY_STALE_AFTER_MS;

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

			const entity = hashToLiveEntity(key.replace('entity:live:', ''), hash);
			if (!entity || entity.last_seen_ms < staleCutoffMs) continue;

			if (predicate(entity)) entities.push(entity);
		}
	} while (cursor !== '0' && entities.length < config.LIVE_ENTITIES_MAX);

	return entities;
}

// Direct by-id lookup for GET /entities/:entity_id (Phase 09 CP2).
// Deliberately does NOT apply the staleness cutoff scanLiveEntities uses for
// list views: an entity whose last known state is stale is exactly what a
// SIGNAL_LOSS investigation is about, so its last known position/callsign
// must still be shown, not hidden as if it never existed.
export async function getLiveEntity(entityId: string): Promise<LiveEntity | null> {
	const hash = await redis.hgetall(`entity:live:${entityId}`);
	if (!hash || Object.keys(hash).length === 0) return null;
	return hashToLiveEntity(entityId, hash);
}
