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

const COORDINATOR_LEASE_TTL_MS = requirePositiveInt(
	'COORDINATOR_LEASE_TTL_MS',
	process.env['COORDINATOR_LEASE_TTL_MS'],
	15_000,
);
const COORDINATOR_RENEWAL_INTERVAL_MS = requirePositiveInt(
	'COORDINATOR_RENEWAL_INTERVAL_MS',
	process.env['COORDINATOR_RENEWAL_INTERVAL_MS'],
	5_000,
);
const COORDINATOR_REDIS_COMMAND_TIMEOUT_MS = requirePositiveInt(
	'COORDINATOR_REDIS_COMMAND_TIMEOUT_MS',
	process.env['COORDINATOR_REDIS_COMMAND_TIMEOUT_MS'],
	2_000,
);

// Safety invariant: a coordinator that cannot reach Redis must give up the
// lease locally before the key can expire and a successor can acquire it.
//
// The TTL clock restarts when Redis runs a renewal, which can be as soon as
// the command is sent, but the coordinator only learns of the success when the
// reply arrives, up to one command timeout later. The next renewal is
// scheduled one interval after that reply, and may itself wait a full command
// timeout before failing. So the worst case from the TTL restarting to the
// coordinator noticing the loss is interval + 2 x timeout, and that must stay
// strictly below the TTL.
export function validateLeaseTiming(
	leaseTtlMs: number,
	renewalIntervalMs: number,
	redisCommandTimeoutMs: number,
): void {
	if (renewalIntervalMs + 2 * redisCommandTimeoutMs >= leaseTtlMs) {
		throw new Error(
			`Config: COORDINATOR_RENEWAL_INTERVAL_MS (${renewalIntervalMs}) + ` +
				`2 x COORDINATOR_REDIS_COMMAND_TIMEOUT_MS (${redisCommandTimeoutMs}) must be less than ` +
				`COORDINATOR_LEASE_TTL_MS (${leaseTtlMs})`,
		);
	}
}

validateLeaseTiming(
	COORDINATOR_LEASE_TTL_MS,
	COORDINATOR_RENEWAL_INTERVAL_MS,
	COORDINATOR_REDIS_COMMAND_TIMEOUT_MS,
);

export const config = {
	KAFKA_BROKERS: (process.env['KAFKA_BROKERS'] ?? 'localhost:9092').split(','),

	// Canonical Kafka topic — do not change without an ADR.
	TOPIC: 'adsb.raw',

	// How often to poll OpenSky. OpenSky limits access by a daily credit budget,
	// not a request rate (ADR-020): 400 credits a day anonymous, 4,000 logged in.
	// The 10 s (anonymous) and 5 s (logged in) figures OpenSky publishes are data
	// resolution, not an allowed request rate. At 1 credit per call, 25 s is
	// 3,456 calls a day, inside the logged-in budget.
	POLL_INTERVAL_MS: requirePositiveInt('POLL_INTERVAL_MS', process.env['POLL_INTERVAL_MS'], 25_000),

	// HTTP fetch timeout per poll cycle. Must leave headroom inside POLL_INTERVAL_MS.
	FETCH_TIMEOUT_MS: requirePositiveInt('FETCH_TIMEOUT_MS', process.env['FETCH_TIMEOUT_MS'], 8_000),

	// Bounding box for the OpenSky states/all request. Decimal degrees.
	// Defaults to the SF Bay box adsb.fi also monitors, so a failover between
	// the two keeps the same area. It is 1.56 square degrees: 1 credit per call.
	OPENSKY_LAMIN: requireFiniteNumber('OPENSKY_LAMIN', process.env['OPENSKY_LAMIN'], 36.9),
	OPENSKY_LOMIN: requireFiniteNumber('OPENSKY_LOMIN', process.env['OPENSKY_LOMIN'], -122.8),
	OPENSKY_LAMAX: requireFiniteNumber('OPENSKY_LAMAX', process.env['OPENSKY_LAMAX'], 38.1),
	OPENSKY_LOMAX: requireFiniteNumber('OPENSKY_LOMAX', process.env['OPENSKY_LOMAX'], -121.5),

	// Backoff after a 429 whose X-Rate-Limit-Retry-After-Seconds is missing or
	// unusable. A valid retry header always wins over this. Each retry waits a
	// random time between the base and an exponential ceiling (base, 2x, 4x...),
	// capped at the max. Fallback retries are never sooner than 60 s apart and
	// never more than 15 min apart, so an exhausted budget is not probed in a
	// fast loop. A valid retry header can legitimately pause for much longer.
	OPENSKY_BACKOFF_BASE_MS: requirePositiveInt(
		'OPENSKY_BACKOFF_BASE_MS',
		process.env['OPENSKY_BACKOFF_BASE_MS'],
		60_000,
	),
	OPENSKY_BACKOFF_MAX_MS: requirePositiveInt(
		'OPENSKY_BACKOFF_MAX_MS',
		process.env['OPENSKY_BACKOFF_MAX_MS'],
		900_000,
	),

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

	// ---- Ingestion coordinator (ADR-022) ----

	REDIS_URL: process.env['REDIS_URL'] ?? 'redis://localhost:6379',

	// Lease timings from ADR-022, the same convention as the Alert Evaluator.
	// The lease guards against two coordinators running at once. It is not
	// fencing: Kafka never checks it.
	COORDINATOR_LEASE_TTL_MS,
	COORDINATOR_RENEWAL_INTERVAL_MS,
	// How long a Redis command may take before it counts as an error. A
	// renewal that times out is treated as a lost lease (fail closed).
	COORDINATOR_REDIS_COMMAND_TIMEOUT_MS,
	// How often a follower retries acquiring the lease.
	COORDINATOR_FOLLOWER_RETRY_MS: requirePositiveInt(
		'COORDINATOR_FOLLOWER_RETRY_MS',
		process.env['COORDINATOR_FOLLOWER_RETRY_MS'],
		5_000,
	),

	// ---- Coverage timeline (ADR-022 sections 5 and 7) ----

	// An adsb.fi cycle fails once the response `now` has not advanced for this
	// long (the frozen-feed check). Measured on the coordinator's clock since
	// `now` last advanced.
	ADSBFI_FROZEN_FEED_MS: requirePositiveInt(
		'ADSBFI_FROZEN_FEED_MS',
		process.env['ADSBFI_FROZEN_FEED_MS'],
		10_000,
	),
	// A closed coverage segment is kept until its end is older than this:
	// live-state TTL (86,400 s) + the largest configured signal-loss threshold
	// (900 s, the evaluator's .env) + one scan interval (30 s). The coordinator
	// holds its own copy; consistency with those services is documented, not
	// enforced.
	COVERAGE_RETENTION_MS: requirePositiveInt(
		'COVERAGE_RETENTION_MS',
		process.env['COVERAGE_RETENTION_MS'],
		(86_400 + 900 + 30) * 1_000,
	),
} as const;
