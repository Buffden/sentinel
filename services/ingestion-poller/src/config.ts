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

// adsb.fi's documented public limit is one request per second; the poll
// interval may never be configured below it.
const ADSBFI_MIN_REQUEST_INTERVAL_MS = 1_000;

function requireAtLeast(name: string, raw: string | undefined, def: number, min: number): number {
	const n = requirePositiveInt(name, raw, def);
	if (n < min) {
		throw new Error(`Config: ${name}=${n} must be at least ${min}`);
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

	// OAuth2 client-credentials, from OpenSky's account "API Client" section.
	// Optional: undefined means unauthenticated requests, same as before —
	// the poller falls back to the anonymous rate limit rather than failing.
	OPENSKY_CLIENT_ID: process.env['OPENSKY_CLIENT_ID'] || undefined,
	OPENSKY_CLIENT_SECRET: process.env['OPENSKY_CLIENT_SECRET'] || undefined,

	// ---- adsb.fi regional primary (ADR-020, ADR-021) ----

	// adsb.fi has no box query, only a circle up to 250 NM. The circle must
	// contain the monitored box; aircraft outside the box are dropped after
	// the fetch. Defaults: the SF Bay box measured in the provider experiment.
	ADSBFI_CENTER_LAT: requireFiniteNumber(
		'ADSBFI_CENTER_LAT',
		process.env['ADSBFI_CENTER_LAT'],
		37.5,
	),
	ADSBFI_CENTER_LON: requireFiniteNumber(
		'ADSBFI_CENTER_LON',
		process.env['ADSBFI_CENTER_LON'],
		-122.15,
	),
	ADSBFI_RADIUS_NM: requireFiniteNumber('ADSBFI_RADIUS_NM', process.env['ADSBFI_RADIUS_NM'], 48),
	ADSBFI_BOX_LAMIN: requireFiniteNumber('ADSBFI_BOX_LAMIN', process.env['ADSBFI_BOX_LAMIN'], 36.9),
	ADSBFI_BOX_LOMIN: requireFiniteNumber(
		'ADSBFI_BOX_LOMIN',
		process.env['ADSBFI_BOX_LOMIN'],
		-122.8,
	),
	ADSBFI_BOX_LAMAX: requireFiniteNumber('ADSBFI_BOX_LAMAX', process.env['ADSBFI_BOX_LAMAX'], 38.1),
	ADSBFI_BOX_LOMAX: requireFiniteNumber(
		'ADSBFI_BOX_LOMAX',
		process.env['ADSBFI_BOX_LOMAX'],
		-121.5,
	),

	// Normal cadence. adsb.fi positions changed about every 2 s per aircraft in
	// the provider experiment, and its public limit is 1 request per second.
	ADSBFI_POLL_INTERVAL_MS: requireAtLeast(
		'ADSBFI_POLL_INTERVAL_MS',
		process.env['ADSBFI_POLL_INTERVAL_MS'],
		2_000,
		ADSBFI_MIN_REQUEST_INTERVAL_MS,
	),
	// After a failed cycle (429, other HTTP error, network error): bounded
	// exponential backoff with full jitter, never shorter than the poll interval.
	ADSBFI_BACKOFF_BASE_MS: requirePositiveInt(
		'ADSBFI_BACKOFF_BASE_MS',
		process.env['ADSBFI_BACKOFF_BASE_MS'],
		2_000,
	),
	ADSBFI_BACKOFF_MAX_MS: requirePositiveInt(
		'ADSBFI_BACKOFF_MAX_MS',
		process.env['ADSBFI_BACKOFF_MAX_MS'],
		60_000,
	),
	ADSBFI_FETCH_TIMEOUT_MS: requirePositiveInt(
		'ADSBFI_FETCH_TIMEOUT_MS',
		process.env['ADSBFI_FETCH_TIMEOUT_MS'],
		8_000,
	),
} as const;
