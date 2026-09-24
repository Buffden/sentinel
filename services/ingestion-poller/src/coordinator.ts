// Ingestion coordinator (ADR-022): the single process that will own live
// provider authority.
//
// The coordinator polls adsb.fi and publishes to adsb.raw while it holds
// {live-provider}:lease, and waits as a follower otherwise. After each
// successful, fresh cycle it credits adsb.fi coverage in the Redis timeline
// (coverageTimeline.ts). Provider health, OpenSky and failover are not here
// yet, so authority, once committed, stays adsb.fi.
//
// Kafka before Redis: a cycle fetches, validates, publishes every message,
// and only then credits coverage. There is no transaction across the two, so
// a crash or Redis failure after a publish leaves delivered positions
// uncredited. That can delay a signal loss, never cause a false one.
//
// Fail closed: a renewal that returns 0, errors or times out means ownership
// can no longer be confirmed. The coordinator stops polling and publishing at
// once, never deletes the key (it may already be a successor's), and goes
// back to follower mode.
//
// Timeline writes fail closed the same way: a rejected token, an error or a
// timeout drops the lease. Otherwise a failure close that did not land could
// leave coverage open, and the next success would count the failure.
//
// The lease is not fencing. Leadership is re-checked between fetching a cycle
// and publishing it, which narrows the window, but a Kafka send that has
// already started cannot be recalled (ADR-022 section 8). The timeline scripts
// check the token, so a stale coordinator can still publish but cannot write
// coverage.

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
import { AdsbfiFreshness, type FreshnessVerdict } from './adsbfiFreshness.js';
import { CoordinatorLease } from './coordinatorLease.js';
import { CoverageTimeline, type CoverageCloseReason } from './coverageTimeline.js';

const TOPIC = 'adsb.raw';

type Lease = Pick<
	CoordinatorLease,
	'token' | 'tryAcquire' | 'renew' | 'release' | 'forget' | 'currentHolder'
>;

type Timeline = Pick<CoverageTimeline, 'credit' | 'close'>;

export interface CoordinatorDeps {
	lease: Lease;
	timeline: Timeline;
	fetchCycle: () => Promise<SplitResult | null>;
	// Publishes one cycle's messages and returns the first offset.
	publish: (messages: SplitResult['messages']) => Promise<string>;
	log: Log;
	renewalIntervalMs: number;
	followerRetryMs: number;
	pollIntervalMs: number;
	backoffBaseMs: number;
	backoffMaxMs: number;
	frozenFeedMs: number;
}

// How an active cycle ended. `lease_lost` means this acquisition can no
// longer write: the next lease holder closes any open coverage.
type CycleOutcome = 'success' | 'failure' | 'lease_lost';

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
	// Reset on every acquisition: a new lease holder must see adsb.fi's `now`
	// advance before it credits coverage.
	private freshness: AdsbfiFreshness;

	constructor(private readonly deps: CoordinatorDeps) {
		this.freshness = new AdsbfiFreshness(deps.frozenFeedMs);
	}

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
		this.freshness = new AdsbfiFreshness(this.deps.frozenFeedMs);

		// Any segment still open was left by a coordinator that stopped without
		// closing it (a crash, a lost lease, or this process before it lost the
		// lease). It must close before polling: otherwise the first success
		// would extend it across the downtime.
		const token = lease.token;
		if (token === null || !(await this.closeCoverage(token, 'coordinator_down'))) return;
		if (this.stopping) return;
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
		const outcome = await this.activeCycle(token);
		if (outcome === 'lease_lost') return;
		// Every failed active cycle asks Redis to close coverage. The close is
		// idempotent, so Redis, not local state, decides whether a segment was
		// open.
		if (outcome === 'failure' && !(await this.closeCoverage(token, 'failure'))) return;

		const ok = outcome === 'success';
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

	// Fetch, validate, publish, then credit (ADR-022 sections 3 and 6).
	private async activeCycle(token: string): Promise<CycleOutcome> {
		const { lease, log } = this.deps;
		const split = await this.deps.fetchCycle();
		// The fetch can take seconds. If the lease was lost meanwhile, a
		// successor may already be publishing, so this cycle is dropped.
		if (lease.token !== token) {
			if (split !== null) {
				log('warn', 'lease lost during cycle: fetched positions discarded, not published', {
					lease_token: token,
					discarded: split.messages.length,
				});
			}
			return 'lease_lost';
		}
		// Request or response failure, already logged by the adapter.
		if (split === null) return 'failure';

		const verdict: FreshnessVerdict = this.freshness.observe(split.responseNowMs, Date.now());
		if (verdict === 'frozen') {
			// A frozen feed fails validation, so nothing from it is published.
			log('warn', 'adsb.fi feed frozen: now has not advanced', {
				response_now_ms: split.responseNowMs,
				stale_for_ms: this.freshness.staleForMs(Date.now()),
				frozen_feed_ms: this.deps.frozenFeedMs,
			});
			return 'failure';
		}

		let firstOffset = 'none';
		if (split.messages.length > 0) {
			try {
				firstOffset = await this.deps.publish(split.messages);
			} catch (err) {
				// A failed publish fails the active cycle, but says nothing about
				// adsb.fi's health.
				log('error', 'poll cycle error', { error: errorMessage(err) });
				return 'failure';
			}
		}
		// The publish stage has completed (immediately after validation for a
		// cycle with nothing to publish): this is the coverage timestamp.
		const activeSuccessMs = Date.now();
		const credited = verdict === 'fresh';
		log('info', 'poll cycle complete', {
			...pollCycleSummary(split, firstOffset),
			lease_token: token,
			freshness: verdict,
			coverage_credited: credited,
		});
		if (!credited) return 'success';
		if (lease.token !== token) return 'lease_lost';
		return (await this.creditCoverage(token, activeSuccessMs)) ? 'success' : 'lease_lost';
	}

	// ---- Leader: coverage timeline -----------------------------------------

	// Returns false when the write was refused or its result is unknown. The
	// lease is then dropped, so nothing more is published under this
	// acquisition.
	private async creditCoverage(token: string, activeSuccessMs: number): Promise<boolean> {
		const { log } = this.deps;
		try {
			const result = await this.deps.timeline.credit(token, activeSuccessMs);
			if (result.status === 'lease_mismatch') {
				this.loseLeadership(token, 'timeline write rejected: token no longer matches');
				return false;
			}
			if (result.status === 'bootstrapped') {
				log('info', 'adsb.fi authority committed and coverage opened', {
					active_success_ms: activeSuccessMs,
					timeline_version: result.timelineVersion,
				});
			} else if (result.status === 'opened') {
				log('info', 'coverage opened', {
					active_success_ms: activeSuccessMs,
					timeline_version: result.timelineVersion,
				});
			} else if (result.status === 'stale_clock') {
				log('warn', 'coverage not credited: time is not after the last success', {
					active_success_ms: activeSuccessMs,
				});
			}
			return true;
		} catch (err) {
			this.loseLeadership(token, 'timeline write error: result unknown', err);
			return false;
		}
	}

	private async closeCoverage(token: string, reason: CoverageCloseReason): Promise<boolean> {
		const { log } = this.deps;
		try {
			const result = await this.deps.timeline.close(token, reason, Date.now());
			if (result.status === 'lease_mismatch') {
				this.loseLeadership(token, 'timeline write rejected: token no longer matches');
				return false;
			}
			if (result.status === 'closed') {
				log('info', 'coverage closed', {
					reason,
					segment: result.member,
					timeline_version: result.timelineVersion,
					pruned: result.pruned,
				});
			}
			return true;
		} catch (err) {
			this.loseLeadership(token, 'timeline write error: result unknown', err);
			return false;
		}
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

		// 3. Close coverage while the lease is still held and renewed. A
		// failure here drops the lease, and the next holder closes the segment
		// as coordinator_down instead.
		const held = this.deps.lease.token;
		if (held !== null) await this.closeCoverage(held, 'coordinator_shutdown');

		// 4. Stop renewal, so a late renewal cannot log a false lease loss.
		this.stopRenewal();

		// 5. Compare-and-delete: only this token's lease is ever removed.
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
		// Same client as the lease, so timeline writes share its command
		// timeout and run in order on one connection.
		timeline: new CoverageTimeline(redis, config.COVERAGE_RETENTION_MS),
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
		frozenFeedMs: config.ADSBFI_FROZEN_FEED_MS,
	});

	let shuttingDown = false;
	const shutdown = async (signal: string): Promise<void> => {
		if (shuttingDown) return;
		shuttingDown = true;
		log('info', 'shutdown initiated', { signal });
		await coordinator.shutdown();
		// 6. Disconnect only after the lease is released.
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
				frozen_feed_ms: config.ADSBFI_FROZEN_FEED_MS,
				coverage_retention_ms: config.COVERAGE_RETENTION_MS,
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
