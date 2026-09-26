// Process bootstrap for live aircraft ingestion.
// Coordinator owns orchestration; this file owns concrete clients, config,
// logging, lifecycle signals and dependency wiring.

import { randomUUID } from 'node:crypto';
import { Redis } from 'ioredis';
import { Kafka, Partitioners } from 'kafkajs';
import { fetchAdsbfiResponse, type Log } from './adsbfiPoller.js';
import { config } from './config.js';
import { Coordinator } from './coordinator.js';
import { CoordinatorLease } from './coordinatorLease.js';
import { CoverageTimeline } from './coverageTimeline.js';
import { fetchOpenskyCycle, openskyAuthenticated } from './poller.js';
import { ProviderHealthStore } from './providerHealthStore.js';

const instanceId = randomUUID();

const log: Log = (level, message, extra) => {
	process.stdout.write(
		JSON.stringify({
			timestamp: new Date().toISOString(),
			level,
			service: 'ingestion-coordinator',
			instance_id: instanceId,
			message,
			...extra,
		}) + '\n',
	);
};

const adsbfiLog: Log = (level, message, extra) =>
	log(level, message, { provider: 'adsbfi', ...extra });

const redis = new Redis(config.REDIS_URL, {
	// A hung renewal must fail before the lease can expire and be acquired by
	// another coordinator.
	commandTimeout: config.COORDINATOR_REDIS_COMMAND_TIMEOUT_MS,
});

const producer = new Kafka({
	clientId: 'ingestion-coordinator',
	brokers: config.KAFKA_BROKERS,
	logLevel: 0,
}).producer({
	createPartitioner: Partitioners.LegacyPartitioner,
});

const coordinator = new Coordinator({
	lease: new CoordinatorLease(redis, config.COORDINATOR_LEASE_TTL_MS),
	timeline: new CoverageTimeline(redis, config.COVERAGE_RETENTION_MS),
	health: new ProviderHealthStore(redis),
	fetchCycle: () => fetchAdsbfiResponse(adsbfiLog),
	fetchOpensky: () => fetchOpenskyCycle(),
	openskyAuthenticated: openskyAuthenticated(),
	healthTiming: {
		degradedTimeoutMs: config.PROVIDER_DEGRADED_TIMEOUT_MS,
		recoveryWindowMs: config.PROVIDER_RECOVERY_WINDOW_MS,
	},
	openskyCadence: {
		healthyMs: config.OPENSKY_HEALTHY_CHECK_INTERVAL_MS,
		degradedMs: config.OPENSKY_DEGRADED_CHECK_INTERVAL_MS,
		recoveringMs: config.OPENSKY_RECOVERING_CHECK_INTERVAL_MS,
		backoffBaseMs: config.OPENSKY_UNAVAILABLE_BACKOFF_BASE_MS,
		backoffMaxMs: config.OPENSKY_UNAVAILABLE_BACKOFF_MAX_MS,
	},
	publish: async (messages) => {
		const results = await producer.send({ topic: config.TOPIC, messages });
		return results[0]?.baseOffset ?? 'unknown';
	},
	log,
	renewalIntervalMs: config.COORDINATOR_RENEWAL_INTERVAL_MS,
	followerRetryMs: config.COORDINATOR_FOLLOWER_RETRY_MS,
	pollIntervalMs: config.ADSBFI_POLL_INTERVAL_MS,
	backoffBaseMs: config.ADSBFI_BACKOFF_BASE_MS,
	backoffMaxMs: config.ADSBFI_BACKOFF_MAX_MS,
	frozenFeedMs: config.ADSBFI_FROZEN_FEED_MS,
	adsbfiStandbyIntervalMs: config.ADSBFI_STANDBY_INTERVAL_MS,
	openskyActiveIntervalMs: config.OPENSKY_ACTIVE_INTERVAL_MS,
	selectionRetryBaseMs: config.SELECTION_RETRY_BASE_MS,
	selectionRetryMaxMs: config.SELECTION_RETRY_MAX_MS,
});

let shuttingDown = false;

async function shutdown(signal: string): Promise<void> {
	if (shuttingDown) return;
	shuttingDown = true;
	log('info', 'shutdown initiated', { signal });
	await coordinator.shutdown();
	await producer.disconnect();
	await redis.quit();
	log('info', 'shutdown complete');
	process.exit(0);
}

process.on('SIGINT', () => {
	shutdown('SIGINT').catch(() => process.exit(1));
});
process.on('SIGTERM', () => {
	shutdown('SIGTERM').catch(() => process.exit(1));
});

producer
	.connect()
	.then(() => {
		log('info', 'coordinator starting', {
			providers: ['adsbfi', 'opensky'],
			opensky_authenticated: openskyAuthenticated(),
			degraded_timeout_ms: config.PROVIDER_DEGRADED_TIMEOUT_MS,
			recovery_window_ms: config.PROVIDER_RECOVERY_WINDOW_MS,
			lease_ttl_ms: config.COORDINATOR_LEASE_TTL_MS,
			renewal_interval_ms: config.COORDINATOR_RENEWAL_INTERVAL_MS,
			follower_retry_ms: config.COORDINATOR_FOLLOWER_RETRY_MS,
			redis_command_timeout_ms: config.COORDINATOR_REDIS_COMMAND_TIMEOUT_MS,
			poll_interval_ms: config.ADSBFI_POLL_INTERVAL_MS,
			adsbfi_standby_interval_ms: config.ADSBFI_STANDBY_INTERVAL_MS,
			opensky_active_interval_ms: config.OPENSKY_ACTIVE_INTERVAL_MS,
			frozen_feed_ms: config.ADSBFI_FROZEN_FEED_MS,
			coverage_retention_ms: config.COVERAGE_RETENTION_MS,
		});
		coordinator.start();
	})
	.catch((err: unknown) => {
		log('error', 'coordinator failed to start', {
			error: err instanceof Error ? err.message : String(err),
		});
		process.exit(1);
	});
