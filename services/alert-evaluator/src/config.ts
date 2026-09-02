// Centralized configuration for the alert evaluator.
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

const LEADER_LEASE_TTL_MS = requirePositiveInt(
	'LEADER_LEASE_TTL_MS',
	process.env['LEADER_LEASE_TTL_MS'],
	15_000,
);
const LEADER_RENEWAL_INTERVAL_MS = requirePositiveInt(
	'LEADER_RENEWAL_INTERVAL_MS',
	process.env['LEADER_RENEWAL_INTERVAL_MS'],
	5_000,
);

// Safety invariant: the renewal timer must fire before the lease expires.
// If LEADER_RENEWAL_INTERVAL_MS >= LEADER_LEASE_TTL_MS, a renewal attempt fires
// at or after the key has already expired, so the leader immediately loses the
// lease it just acquired.
if (LEADER_RENEWAL_INTERVAL_MS >= LEADER_LEASE_TTL_MS) {
	throw new Error(
		`Config: LEADER_RENEWAL_INTERVAL_MS (${LEADER_RENEWAL_INTERVAL_MS}) must be less than ` +
			`LEADER_LEASE_TTL_MS (${LEADER_LEASE_TTL_MS})`,
	);
}

export const config = {
	KAFKA_BROKERS: (process.env['KAFKA_BROKERS'] ?? 'localhost:9092').split(','),
	REDIS_URL: process.env['REDIS_URL'] ?? 'redis://localhost:6379',

	// Canonical Kafka topic — do not change without an ADR.
	ALERTS_TOPIC: 'alerts',

	// Canonical Redis key for the leader lease — do not change without an ADR.
	LEADER_KEY: 'alert-evaluator:leader',

	// How often the leader scans all entity:live:* keys.
	SCAN_INTERVAL_MS: requirePositiveInt('SCAN_INTERVAL_MS', process.env['SCAN_INTERVAL_MS'], 30_000),

	// Silence duration before an entity is declared lost.
	// Applied uniformly to all entity types in v1.
	SIGNAL_LOSS_THRESHOLD_MS: requirePositiveInt(
		'SIGNAL_LOSS_THRESHOLD_MS',
		process.env['SIGNAL_LOSS_THRESHOLD_MS'],
		300_000,
	),

	// How long before a follower retries acquiring the leader lease.
	FOLLOWER_RETRY_INTERVAL_MS: requirePositiveInt(
		'FOLLOWER_RETRY_INTERVAL_MS',
		process.env['FOLLOWER_RETRY_INTERVAL_MS'],
		5_000,
	),

	// Redis SCAN cursor step hint. Higher values reduce round-trips at the cost
	// of longer per-step latency.
	REDIS_SCAN_COUNT: requirePositiveInt('REDIS_SCAN_COUNT', process.env['REDIS_SCAN_COUNT'], 100),

	// Leader lease TTL and renewal interval.
	// Invariant enforced above: LEADER_RENEWAL_INTERVAL_MS < LEADER_LEASE_TTL_MS.
	LEADER_LEASE_TTL_MS,
	LEADER_RENEWAL_INTERVAL_MS,
} as const;
