// Centralized configuration for the position consumer.
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
	PG_URL: process.env['PG_URL'] ?? 'postgresql://sentinel:sentinel-dev@localhost:5433/sentinel',
	REDIS_URL: process.env['REDIS_URL'] ?? 'redis://localhost:6379',

	// Canonical topics, channels, and consumer group — do not change without an ADR.
	SOURCE_TOPIC: 'adsb.raw',
	DLQ_TOPIC: 'adsb.dlq',
	NORMALIZED_TOPIC: 'position.normalized',
	POSITION_UPDATES_CHANNEL: 'position-updates',
	GROUP_ID: 'position-consumer',

	FROM_BEGINNING: (process.env['FROM_BEGINNING'] ?? 'true') === 'true',

	// TTL applied to entity:live:{entity_id} on every accepted write.
	// 24 hours — must exceed SIGNAL_LOSS_THRESHOLD_MS so a recently lost entity's
	// last known position remains readable by the Alert Evaluator.
	LIVE_STATE_TTL_SECONDS: requirePositiveInt(
		'LIVE_STATE_TTL_SECONDS',
		process.env['LIVE_STATE_TTL_SECONDS'],
		86_400,
	),

	// PostgreSQL connection pool maximum connections.
	PG_POOL_MAX: requirePositiveInt('PG_POOL_MAX', process.env['PG_POOL_MAX'], 10),

	// H3 resolution for position_history.geo_cell. ~252 km² per cell at resolution 5.
	// Architectural constant — changing requires a data migration. Not env-configurable.
	HISTORY_H3_RESOLUTION: 5,

	// H3 resolution for Redis geo-cell:{cell_id} sorted sets. ~5 km² per cell at resolution 7.
	// Architectural constant — changing requires re-keying Redis and updating the Correlation Worker.
	// Not env-configurable.
	LIVE_H3_RESOLUTION: 7,
} as const;
