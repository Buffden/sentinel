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
	KAFKA_BROKERS: (process.env['KAFKA_BROKERS'] ?? 'localhost:9092').split(','),
	REDIS_URL: process.env['REDIS_URL'] ?? 'redis://localhost:6379',

	NEO4J_URI: process.env['NEO4J_URI'] ?? 'bolt://localhost:7687',
	NEO4J_USER: process.env['NEO4J_USER'] ?? 'neo4j',
	NEO4J_PASSWORD: process.env['NEO4J_PASSWORD'] ?? 'sentinel-dev',

	// Canonical topics and consumer group -- do not change without an ADR.
	SOURCE_TOPIC: 'position.normalized',
	CANDIDATES_TOPIC: 'proximity.candidates',
	GROUP_ID: 'correlation-worker',

	FROM_BEGINNING: (process.env['FROM_BEGINNING'] ?? 'false') === 'true',

	// Architectural constant matching Position Consumer's LIVE_H3_RESOLUTION.
	// Not env-configurable: geo-cell:{cell_id} keys are only meaningful
	// candidates if both services compute cells at the same resolution, so
	// this cannot be tuned independently per service.
	LIVE_H3_RESOLUTION: 7,

	// How stale a candidate's last known position may be before it's excluded
	// from the search entirely (ZRANGEBYSCORE lower bound). Matches
	// PROXIMITY_EPISODE_GAP_MS's timescale by default -- both express "how
	// long is this pair still plausibly relevant" -- but are independent
	// knobs and may need to diverge once tuned against real data.
	CANDIDATE_FRESHNESS_MS: requirePositiveInt(
		'CANDIDATE_FRESHNESS_MS',
		process.env['CANDIDATE_FRESHNESS_MS'],
		60_000,
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

	// How long a pair can go without a confirming close ping before the
	// encounter is considered over. Redis TTL expiry does the detection, not
	// a scan. 60s tolerates a few missed/delayed pings without truncating a
	// real encounter; tune once real ping cadence under load is known.
	PROXIMITY_EPISODE_GAP_MS: requirePositiveInt(
		'PROXIMITY_EPISODE_GAP_MS',
		process.env['PROXIMITY_EPISODE_GAP_MS'],
		60_000,
	),
} as const;
