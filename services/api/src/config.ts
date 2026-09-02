// Centralized configuration for the API service.
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

function requireString(name: string, raw: string | undefined): string {
	if (!raw) throw new Error(`Config: ${name} is required`);
	return raw;
}

export const config = {
	PORT: requirePositiveInt('PORT', process.env['PORT'], 3000),

	// Auth
	JWT_SECRET: requireString('JWT_SECRET', process.env['JWT_SECRET']),
	GOOGLE_CLIENT_ID: (process.env['GOOGLE_CLIENT_ID'] ?? '').trim(),

	// Infrastructure
	KAFKA_BROKERS: (process.env['KAFKA_BROKERS'] ?? 'localhost:9092').split(','),
	REDIS_URL: process.env['REDIS_URL'] ?? 'redis://localhost:6379',
	PG_URL: process.env['PG_URL'] ?? 'postgres://sentinel:sentinel@localhost:5432/sentinel',

	// PostgreSQL connection pool maximum connections.
	PG_POOL_MAX: requirePositiveInt('PG_POOL_MAX', process.env['PG_POOL_MAX'], 10),

	// Canonical topics, channels, and consumer group — do not change without an ADR.
	ALERTS_TOPIC: 'alerts',
	ALERT_EVENTS_CHANNEL: 'alert-events',
	POSITION_UPDATES_CHANNEL: 'position-updates',
	API_GROUP_ID: 'api',

	// Staleness cutoff for GET /entities/live. Entities with last_seen_ms older than this
	// are excluded from the response.
	// Default 10 minutes, preserving the API's previous staleness cutoff.
	// This value is intentionally independent of the alert evaluator's signal-loss threshold.
	LIVE_ENTITY_STALE_AFTER_MS: requirePositiveInt(
		'LIVE_ENTITY_STALE_AFTER_MS',
		process.env['LIVE_ENTITY_STALE_AFTER_MS'],
		600_000,
	),

	// Redis SCAN cursor step hint for entity:live:* key iteration.
	// Higher values reduce round-trips at the cost of longer per-step latency.
	REDIS_SCAN_COUNT: requirePositiveInt('REDIS_SCAN_COUNT', process.env['REDIS_SCAN_COUNT'], 100),

	// Maximum entities returned by GET /entities/live.
	LIVE_ENTITIES_MAX: requirePositiveInt('LIVE_ENTITIES_MAX', process.env['LIVE_ENTITIES_MAX'], 500),

	// Maximum concurrent demo WebSocket connections.
	MAX_DEMO_CONNECTIONS: requirePositiveInt(
		'MAX_DEMO_CONNECTIONS',
		process.env['MAX_DEMO_CONNECTIONS'],
		10,
	),

	// Demo IP rate-limit window in milliseconds. One token per IP within this window.
	DEMO_RATE_LIMIT_WINDOW_MS: requirePositiveInt(
		'DEMO_RATE_LIMIT_WINDOW_MS',
		process.env['DEMO_RATE_LIMIT_WINDOW_MS'],
		3_600_000,
	),

	// Named constants — operational but not tuned via env in v1.
	COOKIE_NAME: 'sentinel_jwt',
	JWT_EXPIRES_IN: '8h',
	DEMO_JWT_EXPIRES_IN: '3m',
} as const;
