// Centralized configuration for the ingestion poller.
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

function requireNonNegativeInt(name: string, raw: string | undefined, def: number): number {
	if (raw === undefined || raw === '') return def;
	const n = parseInt(raw, 10);
	if (!Number.isFinite(n) || n < 0) {
		throw new Error(
			`Config: ${name}=${JSON.stringify(raw)} must be a non-negative integer (0 = no cap)`,
		);
	}
	return n;
}

function requireFiniteNumber(name: string, raw: string | undefined, def: number): number {
	if (raw === undefined || raw === '') return def;
	const n = Number(raw);
	if (!Number.isFinite(n)) {
		throw new Error(`Config: ${name}=${JSON.stringify(raw)} must be a finite number`);
	}
	return n;
}

export const config = {
	KAFKA_BROKERS: (process.env['KAFKA_BROKERS'] ?? 'localhost:9092').split(','),

	// Canonical Kafka topic — do not change without an ADR.
	TOPIC: 'adsb.raw',

	// How often to poll OpenSky. Anonymous rate limit is approximately one request per 10 s.
	POLL_INTERVAL_MS: requirePositiveInt('POLL_INTERVAL_MS', process.env['POLL_INTERVAL_MS'], 10_000),

	// HTTP fetch timeout per poll cycle. Must leave headroom inside POLL_INTERVAL_MS.
	FETCH_TIMEOUT_MS: requirePositiveInt('FETCH_TIMEOUT_MS', process.env['FETCH_TIMEOUT_MS'], 8_000),

	// Bounding box for the OpenSky states/all request. Decimal degrees.
	// Defaults to UK + Western Europe.
	OPENSKY_LAMIN: requireFiniteNumber('OPENSKY_LAMIN', process.env['OPENSKY_LAMIN'], 49.0),
	OPENSKY_LOMIN: requireFiniteNumber('OPENSKY_LOMIN', process.env['OPENSKY_LOMIN'], -8.0),
	OPENSKY_LAMAX: requireFiniteNumber('OPENSKY_LAMAX', process.env['OPENSKY_LAMAX'], 61.0),
	OPENSKY_LOMAX: requireFiniteNumber('OPENSKY_LOMAX', process.env['OPENSKY_LOMAX'], 10.0),

	// Maximum messages per producer.send() call.
	// 0 = no cap (default) — preserves current behavior during this refactor.
	// Set to a positive integer to enable chunking when polling larger geographic regions.
	POLLER_BATCH_MAX_MESSAGES: requireNonNegativeInt(
		'POLLER_BATCH_MAX_MESSAGES',
		process.env['POLLER_BATCH_MAX_MESSAGES'],
		0,
	),
} as const;
