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

	// Must match Position Consumer's LIVE_H3_RESOLUTION (services/position-consumer/src/config.ts).
	// geo-cell:{cell_id} keys are only meaningful candidate sources if both
	// services compute cell IDs at the same H3 resolution.
	LIVE_H3_RESOLUTION: requirePositiveInt(
		'LIVE_H3_RESOLUTION',
		process.env['LIVE_H3_RESOLUTION'],
		7,
	),

	// V1 experimental rule threshold, not an aviation-safety constant. See
	// docs/implementation/phase-05-correlation-worker/concepts/h3-candidate-lookup/.
	PROXIMITY_THRESHOLD_METRES: requirePositiveInt(
		'PROXIMITY_THRESHOLD_METRES',
		process.env['PROXIMITY_THRESHOLD_METRES'],
		1000,
	),

	// gridDisk radius for candidate search. Chosen conservatively (favoring
	// recall over Redis round-trip count) after direct experimentation with
	// h3-js gridDisk/gridRing against real cell boundaries -- not derived from
	// H3's published *average* edge length, which is not a worst-case bound.
	// See the CP1 concept note for the k=0/1/2 comparison and the specific
	// boundary-crossing case this constant exists to catch.
	CANDIDATE_SEARCH_K: requirePositiveInt(
		'CANDIDATE_SEARCH_K',
		process.env['CANDIDATE_SEARCH_K'],
		2,
	),
} as const;
