// Centralized configuration for the correlation worker.
// This is the only file in this service that reads process.env.
// All other modules import from here.

function requirePositiveInt(name: string, raw: string | undefined, def: number): number {
	if (raw === undefined || raw === '') return def;
	const n = parseInt(raw, 10);
	if (!Number.isFinite(n) || n <= 0) {
		throw new Error(`Config: ${name}=${JSON.stringify(raw)} must be a positive integer`);
	}
	return n;
}

export const config = {
	REDIS_URL: process.env['REDIS_URL'] ?? 'redis://localhost:6379',

	// Must match Position Consumer's LIVE_H3_RESOLUTION -- geo-cell:{cell_id}
	// keys are only meaningful candidates if both services use the same resolution.
	LIVE_H3_RESOLUTION: requirePositiveInt(
		'LIVE_H3_RESOLUTION',
		process.env['LIVE_H3_RESOLUTION'],
		7,
	),

	// V1 experimental rule threshold, not an aviation-safety constant.
	PROXIMITY_THRESHOLD_METRES: requirePositiveInt(
		'PROXIMITY_THRESHOLD_METRES',
		process.env['PROXIMITY_THRESHOLD_METRES'],
		1000,
	),

	// gridDisk radius for candidate search. Chosen conservatively after direct
	// h3-js experimentation against real cell boundaries, not derived from H3's
	// *average* edge length -- an average is not a worst-case coverage bound.
	// See concepts/h3-candidate-lookup for the k=0/1/2 comparison.
	CANDIDATE_SEARCH_K: requirePositiveInt(
		'CANDIDATE_SEARCH_K',
		process.env['CANDIDATE_SEARCH_K'],
		2,
	),
} as const;
