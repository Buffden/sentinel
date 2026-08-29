import { Router } from 'express';
import { redis } from '../redis.js';

const router = Router();

// Entities unseen beyond 2x the signal-loss threshold are excluded — they are either
// permanently gone or will trigger a SIGNAL_LOSS alert on the next evaluator scan.
const SIGNAL_LOSS_THRESHOLD_MS = 300_000;
const STALE_CUTOFF_MULTIPLIER = 2;
const MAX_ENTITIES = 500;

function parseFloat_(val: string | undefined): number | null {
	if (!val || val === '') return null;
	const n = parseFloat(val);
	return isFinite(n) ? n : null;
}

function nullIfEmpty(val: string | undefined): string | null {
	return val === '' || val === undefined ? null : val;
}

router.get('/', async (req, res) => {
	const bboxParam = req.query['bbox'] as string | undefined;
	if (!bboxParam) {
		res.status(400).json({ error: 'bbox query parameter is required' });
		return;
	}

	const parts = bboxParam.split(',').map(Number);
	if (parts.length !== 4 || parts.some((n) => !isFinite(n))) {
		res.status(400).json({ error: 'bbox must be minLat,minLon,maxLat,maxLon' });
		return;
	}
	const [minLat, minLon, maxLat, maxLon] = parts as [number, number, number, number];

	const nowMs = Date.now();
	const staleCutoffMs = nowMs - SIGNAL_LOSS_THRESHOLD_MS * STALE_CUTOFF_MULTIPLIER;

	const entities: unknown[] = [];
	let cursor = '0';

	do {
		const [nextCursor, keys] = await redis.scan(cursor, 'MATCH', 'entity:live:*', 'COUNT', 100);
		cursor = nextCursor;

		for (const key of keys) {
			if (entities.length >= MAX_ENTITIES) break;

			const hash = await redis.hgetall(key);
			if (!hash) continue;

			const lat = parseFloat_(hash['lat']);
			const lon = parseFloat_(hash['lon']);
			if (lat === null || lon === null) continue;

			// Bbox filter
			if (lat < minLat || lat > maxLat || lon < minLon || lon > maxLon) continue;

			// Staleness filter
			const lastSeenMs = parseFloat_(hash['last_seen_ms']);
			if (lastSeenMs === null || lastSeenMs < staleCutoffMs) continue;

			entities.push({
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
			});
		}
	} while (cursor !== '0' && entities.length < MAX_ENTITIES);

	res.json(entities);
});

export { router as entitiesLiveRouter };
