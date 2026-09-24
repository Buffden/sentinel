// Ingestion coordinator (ADR-022): the single process that will own live
// provider authority.
//
// This slice adds only leadership. The coordinator polls adsb.fi and publishes
// to adsb.raw while it holds {live-provider}:lease, and waits as a follower
// otherwise. Provider health, the coverage timeline, OpenSky and failover are
// not here yet.
//
// Fail closed: a renewal that returns 0, errors or times out means ownership
// can no longer be confirmed. The coordinator stops polling and publishing at
// once, never deletes the key (it may already be a successor's), and goes
// back to follower mode.
//
// The lease is not fencing. Leadership is re-checked between fetching a cycle
// and publishing it, which narrows the window, but a Kafka send that has
// already started cannot be recalled (ADR-022 section 8).

import { randomUUID } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { Redis } from 'ioredis';
import { Kafka, Partitioners } from 'kafkajs';
import { config } from './config.js';
import {
	fetchAdsbfiCycle,
	nextDelayMs,
	pollCycleSummary,
	type Log,
	type SplitResult,
} from './adsbfiPoller.js';
import { CoordinatorLease } from './coordinatorLease.js';

const TOPIC = 'adsb.raw';

type Lease = Pick<
	CoordinatorLease,
	'token' | 'tryAcquire' | 'renew' | 'release' | 'forget' | 'currentHolder'
>;

export interface CoordinatorDeps {
	lease: Lease;
	fetchCycle: () => Promise<SplitResult | null>;
	// Publishes one cycle's messages and returns the first offset.
	publish: (messages: SplitResult['messages']) => Promise<string>;
	log: Log;
	renewalIntervalMs: number;
	followerRetryMs: number;
	pollIntervalMs: number;
	backoffBaseMs: number;
	backoffMaxMs: number;
}

function errorMessage(err: unknown): string {
	return err instanceof Error ? err.message : String(err);
}

export class Coordinator {
	private stopping = false;
	private renewalActive = false;
	private acquireTimer: ReturnType<typeof setTimeout> | null = null;
	private renewTimer: ReturnType<typeof setTimeout> | null = null;
	private pollTimer: ReturnType<typeof setTimeout> | null = null;
	private pendingAcquire: Promise<void> | null = null;
	private inFlightCycle: Promise<void> | null = null;
	private consecutiveFailures = 0;
	private waitingLogged = false;

	constructor(private readonly deps: CoordinatorDeps) {}

	get isLeader(): boolean {
		return this.deps.lease.token !== null;
	}

	start(): void {
		this.scheduleAcquire(0);
	}

	// ---- Follower: acquire -------------------------------------------------

	private scheduleAcquire(delayMs: number): void {
		if (this.stopping) return;
		this.acquireTimer = setTimeout(() => {
			this.acquireTimer = null;
			this.pendingAcquire = this.attemptAcquire().finally(() => {
				this.pendingAcquire = null;
			});
		}, delayMs);
	}

	private async attemptAcquire(): Promise<void> {
		if (this.stopping) return;
		const { lease, log } = this.deps;
		let acquired = false;
		try {
			acquired = await lease.tryAcquire();
		} catch (err) {
			log('warn', 'lease acquisition failed', { error: errorMessage(err) });
		}
		if (!acquired) {
			if (!this.waitingLogged) {
				// Logged once per follower period, not on every retry.
				const holder = await lease.currentHolder().catch(() => null);
				log('info', 'lease held by another coordinator: waiting as follower', {
					holder_token: holder,
					retry_ms: this.deps.followerRetryMs,
				});
				this.waitingLogged = true;
			}
			this.scheduleAcquire(this.deps.followerRetryMs);
			return;
		}
		this.waitingLogged = false;
		log('info', 'lease acquired: now leader', { lease_token: lease.token });
		// A shutdown that raced the acquisition releases the lease itself.
		if (this.stopping) return;

		// Stamp the heartbeat at once through the same compare-and-renew path,
		// rather than waiting a full renewal interval.
		this.renewalActive = true;
		if (!(await this.renewOnce())) return;
		this.scheduleRenewal();
		this.consecutiveFailures = 0;
		this.schedulePoll(0);
	}

	// ---- Leader: renew -----------------------------------------------------

	private scheduleRenewal(): void {
		if (!this.renewalActive) return;
		this.renewTimer = setTimeout(() => {
			this.renewTimer = null;
			void this.renewOnce().then((ok) => {
				if (ok) this.scheduleRenewal();
			});
		}, this.deps.renewalIntervalMs);
	}

	private stopRenewal(): void {
		this.renewalActive = false;
		if (this.renewTimer !== null) clearTimeout(this.renewTimer);
		this.renewTimer = null;
	}

	private async renewOnce(): Promise<boolean> {
		const token = this.deps.lease.token;
		if (token === null) return false;
		try {
			if (await this.deps.lease.renew()) return true;
			this.loseLeadership(token, 'renewal rejected: token no longer matches');
		} catch (err) {
			this.loseLeadership(token, 'renewal error: ownership cannot be confirmed', err);
		}
		return false;
	}

	private loseLeadership(token: string, reason: string, err?: unknown): void {
		// Already handled for this token (for example a renewal and a shutdown racing).
		if (this.deps.lease.token !== token) return;
		// Forget locally only. The key may already be a successor's, so it is
		// never deleted here.
		this.deps.lease.forget();
		this.stopRenewal();
		if (this.pollTimer !== null) clearTimeout(this.pollTimer);
		this.pollTimer = null;
		this.deps.log('warn', 'lease lost: stopped polling and publishing', {
			lease_token: token,
			reason,
			...(err === undefined ? {} : { error: errorMessage(err) }),
		});
		this.scheduleAcquire(this.deps.followerRetryMs);
	}

	// ---- Leader: poll ------------------------------------------------------

	private schedulePoll(delayMs: number): void {
		const token = this.deps.lease.token;
		if (this.stopping || token === null) return;
		this.pollTimer = setTimeout(() => {
			this.pollTimer = null;
			const cycle: Promise<void> = this.runCycle(token).finally(() => {
				// A cycle from a lost lease can finish after this process has
				// reacquired and started a new one. Clearing the field then would
				// let shutdown release the lease while the new cycle still publishes.
				if (this.inFlightCycle === cycle) this.inFlightCycle = null;
			});
			this.inFlightCycle = cycle;
		}, delayMs);
	}

	private async runCycle(token: string): Promise<void> {
		const { lease, log } = this.deps;
		if (this.stopping || lease.token !== token) return;
		let ok = false;
		try {
			const split = await this.deps.fetchCycle();
			if (split !== null) {
				// The fetch can take seconds. If the lease was lost meanwhile, a
				// successor may already be publishing, so this cycle is dropped.
				if (lease.token !== token) {
					log('warn', 'lease lost during cycle: fetched positions discarded, not published', {
						lease_token: token,
						discarded: split.messages.length,
					});
					return;
				}
				const firstOffset =
					split.messages.length > 0 ? await this.deps.publish(split.messages) : 'none';
				log('info', 'poll cycle complete', {
					...pollCycleSummary(split, firstOffset),
					lease_token: token,
				});
				ok = true;
			}
		} catch (err) {
			// Kafka publish errors land here; count them as a failed cycle.
			log('error', 'poll cycle error', { error: errorMessage(err) });
		}
		if (ok && this.consecutiveFailures > 0) {
			log('info', 'adsb.fi recovered', { after_failures: this.consecutiveFailures });
		}
		this.consecutiveFailures = ok ? 0 : this.consecutiveFailures + 1;

		if (this.stopping || lease.token !== token) return;
		const delay = nextDelayMs(
			this.consecutiveFailures,
			this.deps.pollIntervalMs,
			this.deps.backoffBaseMs,
			this.deps.backoffMaxMs,
		);
		if (this.consecutiveFailures > 0) {
			log('warn', 'backing off before next request', {
				consecutive_failures: this.consecutiveFailures,
				delay_ms: delay,
			});
		}
		this.schedulePoll(delay);
	}

	// ---- Clean shutdown ----------------------------------------------------

	// Order matters: the lease is held until the last in-flight publish has
	// finished, so a successor cannot start publishing alongside it.
	async shutdown(): Promise<void> {
		// 1. Stop starting new poll cycles (and new acquisition attempts).
		this.stopping = true;
		if (this.pollTimer !== null) clearTimeout(this.pollTimer);
		if (this.acquireTimer !== null) clearTimeout(this.acquireTimer);
		this.pollTimer = null;
		this.acquireTimer = null;
		if (this.pendingAcquire !== null) await this.pendingAcquire;

		// 2. Let the in-flight cycle finish, including its publish. Renewal
		// keeps running meanwhile, so the lease is still held.
		if (this.inFlightCycle !== null) await this.inFlightCycle;

		// ADR-022 shutdown step 3 (close coverage as coordinator_shutdown)
		// belongs here once the coverage timeline exists.

		// 3. Stop renewal, so a late renewal cannot log a false lease loss.
		this.stopRenewal();

		// 4. Compare-and-delete: only this token's lease is ever removed.
		const token = this.deps.lease.token;
		if (token === null) return;
		try {
			const released = await this.deps.lease.release();
			this.deps.log('info', released ? 'lease released' : 'lease already gone at release', {
				lease_token: token,
			});
		} catch (err) {
			this.deps.log('warn', 'lease release failed: it will expire after its TTL', {
				lease_token: token,
				error: errorMessage(err),
			});
		}
	}
}

// ---- Process wiring --------------------------------------------------------

function main(): void {
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

	// A renewal that cannot reach Redis must fail rather than wait: the
	// command timeout turns a hung renewal into a lost lease well before the
	// key can expire (see the invariant in config.ts).
	const redis = new Redis(config.REDIS_URL, {
		commandTimeout: config.COORDINATOR_REDIS_COMMAND_TIMEOUT_MS,
	});

	const producer = new Kafka({
		clientId: 'ingestion-coordinator',
		brokers: config.KAFKA_BROKERS,
		logLevel: 0,
	}).producer({
		// Same partitioner as both pollers, so an ICAO24 key maps to the same
		// partition whichever process published it.
		createPartitioner: Partitioners.LegacyPartitioner,
	});

	const coordinator = new Coordinator({
		lease: new CoordinatorLease(redis, config.COORDINATOR_LEASE_TTL_MS),
		fetchCycle: () => fetchAdsbfiCycle(adsbfiLog),
		publish: async (messages) => {
			const results = await producer.send({ topic: TOPIC, messages });
			return results[0]?.baseOffset ?? 'unknown';
		},
		log,
		renewalIntervalMs: config.COORDINATOR_RENEWAL_INTERVAL_MS,
		followerRetryMs: config.COORDINATOR_FOLLOWER_RETRY_MS,
		pollIntervalMs: config.ADSBFI_POLL_INTERVAL_MS,
		backoffBaseMs: config.ADSBFI_BACKOFF_BASE_MS,
		backoffMaxMs: config.ADSBFI_BACKOFF_MAX_MS,
	});

	let shuttingDown = false;
	const shutdown = async (signal: string): Promise<void> => {
		if (shuttingDown) return;
		shuttingDown = true;
		log('info', 'shutdown initiated', { signal });
		await coordinator.shutdown();
		// 5. Disconnect only after the lease is released.
		await producer.disconnect();
		await redis.quit();
		log('info', 'shutdown complete');
		process.exit(0);
	};
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
				providers: ['adsbfi'],
				lease_ttl_ms: config.COORDINATOR_LEASE_TTL_MS,
				renewal_interval_ms: config.COORDINATOR_RENEWAL_INTERVAL_MS,
				follower_retry_ms: config.COORDINATOR_FOLLOWER_RETRY_MS,
				redis_command_timeout_ms: config.COORDINATOR_REDIS_COMMAND_TIMEOUT_MS,
				poll_interval_ms: config.ADSBFI_POLL_INTERVAL_MS,
			});
			coordinator.start();
		})
		.catch((err: unknown) => {
			log('error', 'coordinator failed to start', { error: errorMessage(err) });
			process.exit(1);
		});
}

// Only run when executed directly (`npm run coordinate`), not when imported by tests.
if (process.argv[1] === fileURLToPath(import.meta.url)) {
	main();
}
