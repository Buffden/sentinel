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

	// HTTP timeout shared by OpenSky token and state requests.
	FETCH_TIMEOUT_MS: requirePositiveInt('FETCH_TIMEOUT_MS', process.env['FETCH_TIMEOUT_MS'], 8_000),

	// Bounding box for the OpenSky states/all request. Defaults to the same
	// SF Bay region monitored by adsb.fi so failover keeps geographic scope.
	OPENSKY_LAMIN: requireFiniteNumber('OPENSKY_LAMIN', process.env['OPENSKY_LAMIN'], 36.9),
	OPENSKY_LOMIN: requireFiniteNumber('OPENSKY_LOMIN', process.env['OPENSKY_LOMIN'], -122.8),
	OPENSKY_LAMAX: requireFiniteNumber('OPENSKY_LAMAX', process.env['OPENSKY_LAMAX'], 38.1),
	OPENSKY_LOMAX: requireFiniteNumber('OPENSKY_LOMAX', process.env['OPENSKY_LOMAX'], -121.5),

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

	// Active cadence while adsb.fi is authoritative. Its public limit is one
	// request per second; the provider experiment observed position changes
	// about every two seconds per aircraft.
	ADSBFI_POLL_INTERVAL_MS: requireAtLeast(
		'ADSBFI_POLL_INTERVAL_MS',
		process.env['ADSBFI_POLL_INTERVAL_MS'],
		2_000,
		ADSBFI_MIN_REQUEST_INTERVAL_MS,
	),
	// Coordinator backoff after an adsb.fi request failure: bounded
	// exponential backoff with full jitter, never shorter than the active cadence.
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

	// ---- Provider health (ADR-022 section 2) ----

	// DEGRADED becomes UNAVAILABLE this long after entering DEGRADED without a
	// success, measured from entry. Repeated failures never move it.
	PROVIDER_DEGRADED_TIMEOUT_MS: requirePositiveInt(
		'PROVIDER_DEGRADED_TIMEOUT_MS',
		process.env['PROVIDER_DEGRADED_TIMEOUT_MS'],
		60_000,
	),
	// RECOVERING becomes HEALTHY after this long with every request succeeding.
	PROVIDER_RECOVERY_WINDOW_MS: requirePositiveInt(
		'PROVIDER_RECOVERY_WINDOW_MS',
		process.env['PROVIDER_RECOVERY_WINDOW_MS'],
		120_000,
	),
	// Check rates while OpenSky is on standby. HEALTHY costs 96 credits a day
	// on the 1-credit SF Bay box. The DEGRADED recheck is 30 s so one check
	// lands inside the 60 s before UNAVAILABLE.
	OPENSKY_HEALTHY_CHECK_INTERVAL_MS: requirePositiveInt(
		'OPENSKY_HEALTHY_CHECK_INTERVAL_MS',
		process.env['OPENSKY_HEALTHY_CHECK_INTERVAL_MS'],
		900_000,
	),
	OPENSKY_DEGRADED_CHECK_INTERVAL_MS: requirePositiveInt(
		'OPENSKY_DEGRADED_CHECK_INTERVAL_MS',
		process.env['OPENSKY_DEGRADED_CHECK_INTERVAL_MS'],
		30_000,
	),
	OPENSKY_RECOVERING_CHECK_INTERVAL_MS: requirePositiveInt(
		'OPENSKY_RECOVERING_CHECK_INTERVAL_MS',
		process.env['OPENSKY_RECOVERING_CHECK_INTERVAL_MS'],
		25_000,
	),
	// While UNAVAILABLE and not paused: from this, doubling, up to the max.
	OPENSKY_UNAVAILABLE_BACKOFF_BASE_MS: requirePositiveInt(
		'OPENSKY_UNAVAILABLE_BACKOFF_BASE_MS',
		process.env['OPENSKY_UNAVAILABLE_BACKOFF_BASE_MS'],
		60_000,
	),
	OPENSKY_UNAVAILABLE_BACKOFF_MAX_MS: requirePositiveInt(
		'OPENSKY_UNAVAILABLE_BACKOFF_MAX_MS',
		process.env['OPENSKY_UNAVAILABLE_BACKOFF_MAX_MS'],
		900_000,
	),

	// ---- Failover (ADR-022 section 4) ----

	// OpenSky's active cycle while it is authoritative: 3,456 credits a day on
	// the 1-credit SF Bay box, inside the 4,000 authenticated budget. Each
	// active request is also OpenSky's health evidence.
	OPENSKY_ACTIVE_INTERVAL_MS: requirePositiveInt(
		'OPENSKY_ACTIVE_INTERVAL_MS',
		process.env['OPENSKY_ACTIVE_INTERVAL_MS'],
		25_000,
	),
	// adsb.fi's request rate when it is not authoritative (standby, or a
	// candidate while authority is none), with the same bounded backoff while failing.
	ADSBFI_STANDBY_INTERVAL_MS: requireAtLeast(
		'ADSBFI_STANDBY_INTERVAL_MS',
		process.env['ADSBFI_STANDBY_INTERVAL_MS'],
		10_000,
		ADSBFI_MIN_REQUEST_INTERVAL_MS,
	),
	// Retry of a whole selection round after a delivery failure while
	// authority is none (a publish that failed, or a commit refused for its
	// time): from this, doubling, up to the max.
	SELECTION_RETRY_BASE_MS: requirePositiveInt(
		'SELECTION_RETRY_BASE_MS',
		process.env['SELECTION_RETRY_BASE_MS'],
		60_000,
	),
	SELECTION_RETRY_MAX_MS: requirePositiveInt(
		'SELECTION_RETRY_MAX_MS',
		process.env['SELECTION_RETRY_MAX_MS'],
		900_000,
	),
} as const;
