// Ingestion coordinator (ADR-022): the single process that will own live
// provider authority.
//
// The coordinator polls adsb.fi and publishes to adsb.raw while it holds
// {live-provider}:lease, and waits as a follower otherwise. After each
// successful, fresh cycle it credits adsb.fi coverage in the Redis timeline
// (coverageTimeline.ts). It also keeps adsb.fi and OpenSky health
// (providerHealth.ts) and runs OpenSky standby checks, which never publish.
// Nothing acts on health yet: failover is not here, so authority, once
// committed, stays adsb.fi even while its health says UNAVAILABLE.
//
// Health before Kafka: a request's outcome is recorded as soon as the
// request and its validation finish, before publishing. A publish failure
// afterwards fails the cycle and closes coverage, but adds no health failure.
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
	fetchAdsbfiResponse,
	nextDelayMs,
	pollCycleSummary,
	type AdsbfiFetchFailure,
	type Log,
	type SplitResult,
} from './adsbfiPoller.js';
import { AdsbfiFreshness, type FreshnessVerdict } from './adsbfiFreshness.js';
import { CoordinatorLease } from './coordinatorLease.js';
import { CoverageTimeline, type CoverageCloseReason } from './coverageTimeline.js';
import { checkOpenskyHealth, type OpenskyCheckResult } from './poller.js';
import {
	applyEvidence,
	expireDegraded,
	nextOpenskyCheckDelayMs,
	restoreHealth,
	type HealthEvidence,
	type OpenskyCadence,
	type Provider,
	type ProviderHealth,
	type TransitionTiming,
} from './providerHealth.js';
import { PROVIDERS, ProviderHealthStore } from './providerHealthStore.js';

const TOPIC = 'adsb.raw';

type Lease = Pick<
	CoordinatorLease,
	'token' | 'tryAcquire' | 'renew' | 'release' | 'forget' | 'currentHolder'
>;

type Timeline = Pick<CoverageTimeline, 'credit' | 'close'>;

type HealthStore = Pick<ProviderHealthStore, 'readForAcquisition' | 'write'>;

export interface CoordinatorDeps {
	lease: Lease;
	timeline: Timeline;
	health: HealthStore;
	// One adsb.fi request: the split response, or the failure's last_error class.
	fetchCycle: () => Promise<SplitResult | AdsbfiFetchFailure>;
	// One OpenSky standby check. It never publishes, and it always runs while
	// this coordinator holds the lease: CP3e will depend on OpenSky's health.
	checkOpensky: () => Promise<OpenskyCheckResult>;
	healthTiming: TransitionTiming;
	openskyCadence: OpenskyCadence;
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

type Timer = ReturnType<typeof setTimeout>;

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

	// ---- Provider health, per acquisition ----
	// Incremented on every acquisition. Anything started under an older term
	// (a late cycle, check or timer from a lost lease) sees the change and
	// writes nothing, so it can never touch the new term's health.
	private term = 0;
	// Rebuilt from Redis at every acquisition; null is unknown health.
	private health: Record<Provider, ProviderHealth | null> = { adsbfi: null, opensky: null };
	// A true first deployment, decided at acquisition: no initialized authority
	// and no health record. Only then may adsb.fi's first valid response mean
	// HEALTHY straight away.
	private firstDeployment = false;
	// Health transitions for one provider run one at a time, so a request
	// outcome and a deadline callback can never interleave.
	private healthQueue: Record<Provider, Promise<unknown>> = {
		adsbfi: Promise.resolve(),
		opensky: Promise.resolve(),
	};
	private deadlineTimers: Record<Provider, Timer | null> = { adsbfi: null, opensky: null };
	private openskyTimer: Timer | null = null;
	private inFlightCheck: Promise<void> | null = null;
	// OpenSky checks failed in a row while UNAVAILABLE and not paused.
	private openskyUnavailableStep = 0;

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
		const term = ++this.term;
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

		// Health is restored before any request, so no outcome can be applied
		// to the previous holder's in-memory view or to unrestored state.
		const openskyDelayMs = await this.restoreProviderHealth(token, term);
		if (openskyDelayMs === null || this.stopping) return;
		this.schedulePoll(0);
		this.scheduleOpenskyCheck(token, term, openskyDelayMs);
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
		this.clearHealthTimers();
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
		const term = this.term;
		if (this.stopping || token === null) return;
		this.pollTimer = setTimeout(() => {
			this.pollTimer = null;
			const cycle: Promise<void> = this.runCycle(token, term).finally(() => {
				// A cycle from a lost lease can finish after this process has
				// reacquired and started a new one. Clearing the field then would
				// let shutdown release the lease while the new cycle still publishes.
				if (this.inFlightCycle === cycle) this.inFlightCycle = null;
			});
			this.inFlightCycle = cycle;
		}, delayMs);
	}

	private async runCycle(token: string, term: number): Promise<void> {
		const { lease, log } = this.deps;
		if (this.stopping || lease.token !== token) return;
		const outcome = await this.activeCycle(token, term);
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
	private async activeCycle(token: string, term: number): Promise<CycleOutcome> {
		const { lease, log } = this.deps;
		const result = await this.deps.fetchCycle();
		// The fetch can take seconds. If the lease was lost meanwhile, a
		// successor may already be publishing, so this cycle is dropped.
		if (lease.token !== token) {
			if (!('error' in result)) {
				log('warn', 'lease lost during cycle: fetched positions discarded, not published', {
					lease_token: token,
					discarded: result.messages.length,
				});
			}
			return 'lease_lost';
		}
		// Request or response failure, already logged by the adapter: provider
		// health evidence, and a failed active cycle.
		if ('error' in result) {
			const ev: HealthEvidence = { kind: 'failure', atMs: Date.now(), error: result.error };
			return (await this.recordHealth('adsbfi', token, term, ev)) ? 'failure' : 'lease_lost';
		}
		const split = result;

		// The freshness verdict serves both health and coverage: frozen is a
		// health failure and a coverage failure; seeded and unconfirmed are
		// valid responses (health success) that coverage does not credit.
		const verdict: FreshnessVerdict = this.freshness.observe(split.responseNowMs, Date.now());
		if (verdict === 'frozen') {
			// A frozen feed fails validation, so nothing from it is published.
			log('warn', 'adsb.fi feed frozen: now has not advanced', {
				response_now_ms: split.responseNowMs,
				stale_for_ms: this.freshness.staleForMs(Date.now()),
				frozen_feed_ms: this.deps.frozenFeedMs,
			});
			const ev: HealthEvidence = { kind: 'failure', atMs: Date.now(), error: 'frozen_feed' };
			return (await this.recordHealth('adsbfi', token, term, ev)) ? 'failure' : 'lease_lost';
		}

		// Recorded before publishing, so a Kafka failure below cannot reach it.
		if (!(await this.recordHealth('adsbfi', token, term, { kind: 'success', atMs: Date.now() }))) {
			return 'lease_lost';
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
			} else if (result.status === 'closed_empty') {
				log('info', 'coverage closed with no length: no segment written', {
					reason,
					timeline_version: result.timelineVersion,
				});
			}
			return true;
		} catch (err) {
			this.loseLeadership(token, 'timeline write error: result unknown', err);
			return false;
		}
	}

	// ---- Leader: provider health -------------------------------------------

	// Runs one health transition for a provider after any already queued.
	private serializeHealth<T>(provider: Provider, fn: () => Promise<T>): Promise<T> {
		const run = this.healthQueue[provider].then(fn);
		this.healthQueue[provider] = run.catch(() => undefined);
		return run;
	}

	// Applies one request outcome and persists it. Returns false when this
	// acquisition can no longer write (the lease is then dropped), true
	// otherwise, including when the outcome was dropped as stale.
	private recordHealth(
		provider: Provider,
		token: string,
		term: number,
		ev: HealthEvidence,
	): Promise<boolean> {
		return this.serializeHealth(provider, async () => {
			if (this.term !== term || this.deps.lease.token !== token) return false;
			const next = applyEvidence(this.health[provider], ev, this.deps.healthTiming, {
				firstDeployment: provider === 'adsbfi' && this.firstDeployment,
			});
			return this.persistHealth(provider, token, term, next);
		});
	}

	// Fail closed like the timeline: a refused, failed or timed-out write
	// means this acquisition is no longer trusted.
	private async persistHealth(
		provider: Provider,
		token: string,
		term: number,
		next: ProviderHealth,
	): Promise<boolean> {
		const previous = this.health[provider];
		try {
			const result = await this.deps.health.write(token, provider, next);
			if (result === 'lease_mismatch') {
				this.loseLeadership(token, 'health write rejected: token no longer matches');
				return false;
			}
		} catch (err) {
			this.loseLeadership(token, 'health write error: result unknown', err);
			return false;
		}
		if (this.term !== term || this.deps.lease.token !== token) return false;
		this.health[provider] = next;
		if (previous?.state !== next.state) {
			this.deps.log('info', 'provider health changed', {
				provider,
				from: previous?.state ?? 'unknown',
				to: next.state,
				state_since_ms: next.stateSinceMs,
				consecutive_failures: next.consecutiveFailures,
				last_error: next.lastError,
			});
		}
		this.armDeadline(provider, token, term);
		return true;
	}

	// One deadline per DEGRADED term, armed from state_since_ms so a late or
	// early arm always targets the same logical moment. Leaving DEGRADED
	// cancels it; the callback also re-checks the term, so a timer that fires
	// after a newer success or a newer DEGRADED term changes nothing.
	private armDeadline(provider: Provider, token: string, term: number): void {
		const current = this.deadlineTimers[provider];
		if (current !== null) clearTimeout(current);
		this.deadlineTimers[provider] = null;
		const h = this.health[provider];
		if (h === null || h.state !== 'DEGRADED') return;
		const termSinceMs = h.stateSinceMs;
		const delayMs = Math.max(
			0,
			termSinceMs + this.deps.healthTiming.degradedTimeoutMs - Date.now(),
		);
		this.deadlineTimers[provider] = setTimeout(() => {
			this.deadlineTimers[provider] = null;
			void this.serializeHealth(provider, async () => {
				if (this.term !== term || this.deps.lease.token !== token) return false;
				const next = expireDegraded(
					this.health[provider],
					termSinceMs,
					Date.now(),
					this.deps.healthTiming,
				);
				if (next === null) return true;
				return this.persistHealth(provider, token, term, next);
			});
		}, delayMs);
	}

	private clearHealthTimers(): void {
		for (const provider of PROVIDERS) {
			const timer = this.deadlineTimers[provider];
			if (timer !== null) clearTimeout(timer);
			this.deadlineTimers[provider] = null;
		}
		if (this.openskyTimer !== null) clearTimeout(this.openskyTimer);
		this.openskyTimer = null;
	}

	// ADR-022 section 7, at acquisition and before any request. Returns the
	// delay before the first OpenSky check, or null when the lease was lost.
	private async restoreProviderHealth(token: string, term: number): Promise<number | null> {
		const { log } = this.deps;
		let snapshot;
		try {
			snapshot = await this.deps.health.readForAcquisition();
		} catch (err) {
			this.loseLeadership(token, 'health read error: state unknown', err);
			return null;
		}
		const nowMs = Date.now();
		this.health = { adsbfi: null, opensky: null };
		this.openskyUnavailableStep = 0;
		this.firstDeployment =
			!snapshot.authorityInitialized &&
			!snapshot.stored.adsbfi.present &&
			!snapshot.stored.opensky.present;

		let openskyPauseExpired = false;
		for (const provider of PROVIDERS) {
			const stored = snapshot.stored[provider];
			if (stored.problem !== null) {
				log('warn', 'stored provider health unreadable: treated as unknown', {
					provider,
					problem: stored.problem,
				});
			}
			const { health, pauseExpired } = restoreHealth(stored.health, nowMs);
			if (provider === 'opensky') openskyPauseExpired = pauseExpired;
			if (health === null) continue;
			// Seeded with what was stored, so the transition log reads from it.
			this.health[provider] = stored.health;
			const changed = JSON.stringify(health) !== JSON.stringify(stored.health);
			const ok = changed
				? await this.serializeHealth(provider, () =>
						this.persistHealth(provider, token, term, health),
					)
				: true;
			if (!ok) return null;
			this.health[provider] = health;
			this.armDeadline(provider, token, term);
		}

		log('info', 'provider health restored', {
			adsbfi: this.health.adsbfi?.state ?? 'unknown',
			opensky: this.health.opensky?.state ?? 'unknown',
			first_deployment: this.firstDeployment,
		});
		const authority = snapshot.authorityProvider;
		if (authority !== null && this.health[authority as Provider]?.state === 'UNAVAILABLE') {
			// ADR-022 says authority becomes none here. That switch is failover
			// (CP3e); until then authority is left exactly as it is.
			log(
				'warn',
				'authoritative provider restored UNAVAILABLE: authority unchanged until failover exists',
				{
					provider: authority,
				},
			);
		}

		const opensky = this.health.opensky;
		if (openskyPauseExpired) return 0;
		if (opensky?.state === 'UNAVAILABLE' && opensky.pausedUntilMs === null) {
			// A restored outage restarts its backoff at the initial value.
			this.openskyUnavailableStep = 1;
		}
		return nextOpenskyCheckDelayMs(
			opensky,
			nowMs,
			this.openskyUnavailableStep,
			this.deps.openskyCadence,
		);
	}

	// ---- Leader: OpenSky standby checks -------------------------------------

	private scheduleOpenskyCheck(token: string, term: number, delayMs: number): void {
		if (this.stopping || this.deps.lease.token !== token || this.term !== term) return;
		this.openskyTimer = setTimeout(() => {
			this.openskyTimer = null;
			const check: Promise<void> = this.runOpenskyCheck(token, term).finally(() => {
				if (this.inFlightCheck === check) this.inFlightCheck = null;
			});
			this.inFlightCheck = check;
		}, delayMs);
	}

	// Fetch and validate only: a check never publishes, never touches coverage
	// and never changes authority. Its only effect is OpenSky's health.
	private async runOpenskyCheck(token: string, term: number): Promise<void> {
		if (this.stopping || this.deps.lease.token !== token || this.term !== term) return;
		let result: OpenskyCheckResult;
		try {
			result = await this.deps.checkOpensky();
		} catch (err) {
			result = { kind: 'failed', error: `error: ${errorMessage(err)}` };
		}
		if (this.deps.lease.token !== token || this.term !== term) return;

		const atMs = Date.now();
		let ev: HealthEvidence;
		if (result.kind === 'ok') {
			ev = { kind: 'success', atMs, creditsRemaining: result.creditsRemaining, probe: true };
		} else if (result.kind === 'rate_limited' && result.retryAfterSeconds !== null) {
			ev = {
				kind: 'paused',
				atMs,
				error: 'rate_limited',
				pausedUntilMs: atMs + result.retryAfterSeconds * 1000,
				probe: true,
			};
		} else {
			const error = result.kind === 'failed' ? result.error : 'rate_limited';
			ev = { kind: 'failure', atMs, error, probe: true };
		}

		const previous = this.health.opensky;
		if (!(await this.recordHealth('opensky', token, term, ev))) return;
		const next = this.health.opensky;
		if (ev.kind === 'success' || next === null || next.state !== 'UNAVAILABLE') {
			this.openskyUnavailableStep = 0;
		} else if (next.pausedUntilMs === null) {
			this.openskyUnavailableStep =
				previous?.state === 'UNAVAILABLE' ? this.openskyUnavailableStep + 1 : 1;
		}
		const delayMs = nextOpenskyCheckDelayMs(
			next,
			Date.now(),
			this.openskyUnavailableStep,
			this.deps.openskyCadence,
		);
		this.deps.log('info', 'opensky health check', {
			provider: 'opensky',
			outcome: result.kind,
			...(result.kind === 'failed' ? { error: result.error } : {}),
			state: next?.state ?? 'unknown',
			credits_remaining: next?.creditsRemaining ?? null,
			paused_until_ms: next?.pausedUntilMs ?? null,
			next_check_in_ms: delayMs,
		});
		if (this.stopping) return;
		this.scheduleOpenskyCheck(token, term, delayMs);
	}

	// ---- Clean shutdown ----------------------------------------------------

	// Order matters: the lease is held until the last in-flight publish has
	// finished, so a successor cannot start publishing alongside it.
	async shutdown(): Promise<void> {
		// 1. Stop starting new poll cycles (and new acquisition attempts).
		this.stopping = true;
		if (this.pollTimer !== null) clearTimeout(this.pollTimer);
		if (this.acquireTimer !== null) clearTimeout(this.acquireTimer);
		if (this.openskyTimer !== null) clearTimeout(this.openskyTimer);
		this.pollTimer = null;
		this.acquireTimer = null;
		this.openskyTimer = null;
		if (this.pendingAcquire !== null) await this.pendingAcquire;

		// 2. Let the in-flight cycle finish, including its publish, and any
		// in-flight OpenSky check. Renewal keeps running meanwhile, so the
		// lease is still held. Health stays in Redis for the next holder.
		if (this.inFlightCycle !== null) await this.inFlightCycle;
		if (this.inFlightCheck !== null) await this.inFlightCheck;
		this.clearHealthTimers();

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
		health: new ProviderHealthStore(redis),
		fetchCycle: () => fetchAdsbfiResponse(adsbfiLog),
		checkOpensky: () => checkOpenskyHealth(),
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
				degraded_timeout_ms: config.PROVIDER_DEGRADED_TIMEOUT_MS,
				recovery_window_ms: config.PROVIDER_RECOVERY_WINDOW_MS,
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
