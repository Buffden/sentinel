// Ingestion coordinator (ADR-022): the single process that owns live provider
// authority.
//
// While it holds {live-provider}:lease the coordinator runs one request loop
// per provider, adsb.fi and OpenSky, and waits as a follower otherwise. What
// a request is for depends on authority at the moment it runs:
//
// - the authoritative provider's request is an active cycle: it publishes to
//   adsb.raw and credits that provider's coverage;
// - the other provider's request is a standby check: health only, never a
//   publish, never a commit;
// - while authority is none, a request is a candidate: health evidence, and,
//   if the response qualifies, a delivery that publishes and then commits
//   authority. Candidate delivery shares the single publish lane.
//
// Health drives failover: when the authoritative provider reaches
// UNAVAILABLE, authority is relinquished to none and a selection round tries
// eligible providers until one commits. Voluntary OpenSky -> adsb.fi failback
// is separate: adsb.fi must be HEALTHY and OpenSky must have held authority
// for the ADR's minimum window before a serialized handover is attempted.
//
// Three things stay separate. Provider health describes the upstream
// provider. Coverage describes successful authoritative delivery. Authority
// decides who may publish.
//
// Health before Kafka: a request's outcome is recorded as soon as the
// request and its validation finish, before publishing. A publish failure
// afterwards fails the cycle and closes coverage, but adds no health failure.
//
// Kafka before Redis: a cycle publishes every message and only then credits
// coverage or commits authority. There is no transaction across the two, so
// a crash or Redis failure after a publish leaves delivered positions
// uncredited. That can delay a signal loss, never cause a false one.
//
// One publisher at a time: every publish goes through one queue, and each
// re-checks authority when its turn comes. A send that has already started
// cannot be recalled, so a candidate waits for it to settle before
// publishing.
//
// Fail closed: a renewal, health write or timeline write that is refused,
// errors or times out means ownership can no longer be confirmed. The
// coordinator stops at once, never deletes the key (it may already be a
// successor's), and goes back to follower mode.
//
// The lease is not fencing. Leadership is re-checked between fetching and
// publishing, which narrows the window, but a Kafka send that has already
// started cannot be recalled (ADR-022 section 8). The timeline scripts check
// the token, so a stale coordinator can still publish but cannot write
// authority or coverage.

import type { AdsbfiFetchFailure, Log, SplitResult } from './adsbfiPoller.js';
import { AdsbfiFreshness, type FreshnessVerdict } from './adsbfiFreshness.js';
import { CoordinatorLease } from './coordinatorLease.js';
import { CoverageTimeline, type CoverageCloseReason } from './coverageTimeline.js';
import type { OpenskyFetchResult } from './poller.js';
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
import {
	isStale,
	nextProviderDelayMs,
	nextSelectionRetryMs,
	planSelectionRound,
} from './providerSelection.js';

type Lease = Pick<
	CoordinatorLease,
	'token' | 'tryAcquire' | 'renew' | 'release' | 'forget' | 'currentHolder'
>;

type Timeline = Pick<CoverageTimeline, 'credit' | 'close' | 'commit' | 'handover' | 'relinquish'>;

type HealthStore = Pick<ProviderHealthStore, 'readForAcquisition' | 'write'>;

type Messages = SplitResult['messages'];

export interface CoordinatorDeps {
	lease: Lease;
	timeline: Timeline;
	health: HealthStore;
	// One adsb.fi request: the split response, or the failure's last_error class.
	fetchCycle: () => Promise<SplitResult | AdsbfiFetchFailure>;
	// One OpenSky request: mapped messages, or a failure or rate limit. It
	// never publishes; the coordinator decides what the request is for.
	fetchOpensky: () => Promise<OpenskyFetchResult>;
	// Anonymous OpenSky access has a small daily budget: logged as a warning
	// whenever OpenSky becomes authoritative without credentials.
	openskyAuthenticated: boolean;
	healthTiming: TransitionTiming;
	openskyCadence: OpenskyCadence;
	// Publishes one cycle's messages and returns the first offset.
	publish: (messages: Messages) => Promise<string>;
	log: Log;
	renewalIntervalMs: number;
	followerRetryMs: number;
	// adsb.fi while authoritative, with bounded backoff after failures.
	pollIntervalMs: number;
	backoffBaseMs: number;
	backoffMaxMs: number;
	frozenFeedMs: number;
	// adsb.fi while not authoritative, with the same backoff.
	adsbfiStandbyIntervalMs: number;
	// OpenSky while authoritative.
	openskyActiveIntervalMs: number;
	// Per-provider candidate delivery backoff while authority is none.
	selectionRetryBaseMs: number;
	selectionRetryMaxMs: number;
	// Voluntary failback is never attempted before OpenSky has held authority
	// this long. The production value is the ADR's fixed five minutes.
	failbackMinOpenskyAuthorityMs: number;
}

// Who holds authority, as this coordinator knows it. `none` also covers a
// timeline that never had an authority: nothing is written for it, and the
// first commit creates epoch 1.
type Mode = Provider | 'none';

type Role = 'active' | 'standby' | 'candidate' | 'handover';

// Selection-round requests are distinguished from normal cadence because a
// stale provider gets only conservative recovery credit on that immediate try.
type Trigger = 'cadence' | 'round';

interface RequestOutcome {
	role: Role;
	// The provider answered badly (health failure evidence).
	providerFailed: boolean;
	// An active cycle failed (including a publish failure).
	cycleFailed: boolean;
	deliveryFailed: boolean;
	committed: boolean;
	leaseLost: boolean;
}

interface RestoredState {
	authorityInitialized: boolean;
	authorityProvider: string | null;
	authoritySinceMs: number | null;
	openskyDelayMs: number;
}

type Timer = ReturnType<typeof setTimeout>;

function errorMessage(err: unknown): string {
	return err instanceof Error ? err.message : String(err);
}

export class Coordinator {
	private stopping = false;
	private renewalActive = false;
	private acquireTimer: Timer | null = null;
	private renewTimer: Timer | null = null;
	private pendingAcquire: Promise<void> | null = null;
	private waitingLogged = false;
	// One per acquisition, never reset by an authority change: standby checks
	// keep it current, so adsb.fi can deliver on its first request after
	// authority becomes none.
	private freshness: AdsbfiFreshness;

	// ---- Per acquisition ----
	// Incremented on every acquisition. Anything started under an older term
	// (a late request or timer from a lost lease) sees the change and writes
	// nothing.
	private term = 0;
	private mode: Mode = 'none';
	private authoritySinceMs: number | null = null;
	private handoverInProgress = false;
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

	// Within one acquisition, requests to one provider run one at a time.
	// Queues start empty at every acquisition: work left from a lost lease is
	// term-guarded and writes nothing, so a new holder never waits for it.
	private requestQueue: Record<Provider, Promise<unknown>> = {
		adsbfi: Promise.resolve(),
		opensky: Promise.resolve(),
	};
	private requestTimers: Record<Provider, Timer | null> = { adsbfi: null, opensky: null };
	// Every request still running, of any acquisition, for shutdown to await.
	private inFlight = new Set<Promise<unknown>>();
	// Every publish, of any provider, role or acquisition, runs one at a
	// time: a send that has started cannot be recalled.
	private publishQueue: Promise<unknown> = Promise.resolve();

	// adsb.fi requests failed in a row, for bounded request backoff.
	private adsbfiFailures = 0;
	// OpenSky requests failed in a row while UNAVAILABLE and not paused.
	private openskyUnavailableStep = 0;

	// ---- Selection while authority is none ----
	// Providers that used their one immediate attempt in this entry into none.
	private oneShotUsed = new Set<Provider>();
	// Delivery failures are not provider-health failures. Back them off on
	// that provider's request schedule instead of maintaining another timer.
	private deliveryFailures: Record<Provider, number> = { adsbfi: 0, opensky: 0 };
	private round: Promise<void> | null = null;

	constructor(private readonly deps: CoordinatorDeps) {
		this.freshness = new AdsbfiFreshness(deps.frozenFeedMs);
	}

	get isLeader(): boolean {
		return this.deps.lease.token !== null;
	}

	// Who holds authority, as this coordinator knows it.
	get authority(): Mode {
		return this.mode;
	}

	start(): void {
		this.scheduleAcquire(0);
	}

	private current(token: string, term: number): boolean {
		return !this.stopping && this.deps.lease.token === token && this.term === term;
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
		this.mode = 'none';
		this.authoritySinceMs = null;
		this.handoverInProgress = false;
		this.requestQueue = { adsbfi: Promise.resolve(), opensky: Promise.resolve() };
		log('info', 'lease acquired: now leader', { lease_token: lease.token });
		// A shutdown that raced the acquisition releases the lease itself.
		if (this.stopping) return;

		// Stamp the heartbeat at once through the same compare-and-renew path,
		// rather than waiting a full renewal interval.
		this.renewalActive = true;
		if (!(await this.renewOnce())) return;
		this.scheduleRenewal();
		this.adsbfiFailures = 0;
		this.freshness = new AdsbfiFreshness(this.deps.frozenFeedMs);

		// Any segment still open was left by a coordinator that stopped without
		// closing it (a crash, a lost lease, or this process before it lost the
		// lease). It must close before polling: otherwise the first success
		// would extend it across the downtime.
		const token = lease.token;
		if (token === null || !(await this.closeCoverage(token, 'coordinator_down'))) return;
		if (this.stopping) return;

		// Health first, then authority, both before any request.
		const restored = await this.restoreProviderHealth(token, term);
		if (restored === null || !this.current(token, term)) return;
		if (!(await this.restoreAuthority(token, term, restored))) return;
		if (!this.current(token, term)) return;

		// The authority's loop starts at once. While none, the selection round
		// entered above makes the first requests. (Read through the getter:
		// restoreAuthority may have changed it.)
		const mode = this.authority;
		if (mode === 'adsbfi') {
			this.scheduleRequest('adsbfi', 0, token, term);
			this.scheduleRequest('opensky', restored.openskyDelayMs, token, term);
		} else if (mode === 'opensky') {
			this.scheduleRequest('opensky', 0, token, term);
			this.scheduleRequest('adsbfi', 0, token, term);
		}
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
		this.clearTimers();
		this.deps.log('warn', 'lease lost: stopped polling and publishing', {
			lease_token: token,
			reason,
			...(err === undefined ? {} : { error: errorMessage(err) }),
		});
		this.scheduleAcquire(this.deps.followerRetryMs);
	}

	private clearTimers(): void {
		for (const provider of PROVIDERS) {
			const request = this.requestTimers[provider];
			if (request !== null) clearTimeout(request);
			this.requestTimers[provider] = null;
			const deadline = this.deadlineTimers[provider];
			if (deadline !== null) clearTimeout(deadline);
			this.deadlineTimers[provider] = null;
		}
	}

	// ---- Leader: request loops -----------------------------------------------

	private scheduleRequest(provider: Provider, delayMs: number, token: string, term: number): void {
		if (!this.current(token, term)) return;
		const existing = this.requestTimers[provider];
		if (existing !== null) clearTimeout(existing);
		this.requestTimers[provider] = setTimeout(() => {
			this.requestTimers[provider] = null;
			void this.request(provider, token, term, 'cadence');
		}, delayMs);
	}

	// One request to a provider, after any already running for it. Its role is
	// decided when it starts, from authority at that moment.
	private request(
		provider: Provider,
		token: string,
		term: number,
		trigger: Trigger,
	): Promise<RequestOutcome | null> {
		const run = this.requestQueue[provider].then(async () => {
			if (!this.current(token, term)) return null;
			const outcome = await this.providerRequest(provider, token, term, trigger);
			if (outcome !== null && !outcome.leaseLost && this.current(token, term)) {
				this.scheduleNext(provider, outcome, token, term);
			}
			return outcome;
		});
		this.requestQueue[provider] = run.catch(() => undefined);
		this.inFlight.add(run);
		void run.finally(() => this.inFlight.delete(run)).catch(() => undefined);
		return run;
	}

	private roleOf(provider: Provider): Role {
		if (this.mode === provider) return 'active';
		return this.mode === 'none' ? 'candidate' : 'standby';
	}

	// The next request's delay, from the provider's role now (authority may
	// have changed during the request) and how the request went.
	private scheduleNext(
		provider: Provider,
		outcome: RequestOutcome,
		token: string,
		term: number,
	): void {
		const role = this.roleOf(provider);
		let delayMs: number;

		// A voluntary handover has stopped OpenSky after its current request.
		// The handover's finally block decides whether it resumes as active or
		// returns as standby.
		if (provider === 'opensky' && this.handoverInProgress && this.mode === 'opensky') return;

		if (outcome.committed) this.deliveryFailures[provider] = 0;
		if (role === 'candidate' && outcome.deliveryFailed) {
			const step = ++this.deliveryFailures[provider];
			delayMs = nextSelectionRetryMs(
				step,
				this.deps.selectionRetryBaseMs,
				this.deps.selectionRetryMaxMs,
			);
			this.deps.log('warn', 'candidate delivery backing off', {
				provider,
				retry_in_ms: delayMs,
			});
			this.scheduleRequest(provider, delayMs, token, term);
			return;
		}

		if (provider === 'adsbfi') {
			const failed = outcome.role === 'active' ? outcome.cycleFailed : outcome.providerFailed;
			this.adsbfiFailures = failed ? this.adsbfiFailures + 1 : 0;
			const interval =
				role === 'active' ? this.deps.pollIntervalMs : this.deps.adsbfiStandbyIntervalMs;
			delayMs = nextProviderDelayMs(
				this.adsbfiFailures,
				interval,
				this.deps.backoffBaseMs,
				this.deps.backoffMaxMs,
			);
			if (this.adsbfiFailures > 0) {
				this.deps.log('warn', 'backing off before next request', {
					provider,
					consecutive_failures: this.adsbfiFailures,
					delay_ms: delayMs,
				});
			}
		} else if (role === 'active') {
			delayMs = this.deps.openskyActiveIntervalMs;
		} else {
			delayMs = this.openskyStandbyDelayMs();
		}
		this.scheduleRequest(provider, delayMs, token, term);
	}

	private openskyStandbyDelayMs(): number {
		return nextOpenskyCheckDelayMs(
			this.health.opensky,
			Date.now(),
			this.openskyUnavailableStep,
			this.deps.openskyCadence,
		);
	}

	// Fetch, validate, record health, then act on the role (ADR-022 sections
	// 3 and 6). The role is read inside the candidate queue when authority is
	// none, so a commit by the other provider turns this into a standby check.
	private async providerRequest(
		provider: Provider,
		token: string,
		term: number,
		trigger: Trigger,
	): Promise<RequestOutcome> {
		const role = this.roleOf(provider);
		// ADR-022 section 4: a success after stale health proves only a start.
		// Staleness is judged only for a round's immediate attempt: a request
		// made at the provider's own rate is on schedule, never stale, even if
		// scheduling jitter puts it a moment past one interval.
		const stale =
			role === 'candidate' && trigger === 'round' && this.isProviderStale(provider, Date.now());
		return provider === 'adsbfi'
			? this.adsbfiRequest(role, stale, token, term)
			: this.openskyRequest(role, stale, token, term);
	}

	private outcomeFor(role: Role): (o: Partial<RequestOutcome>) => RequestOutcome {
		return (o) => ({
			role,
			providerFailed: false,
			cycleFailed: false,
			deliveryFailed: false,
			committed: false,
			leaseLost: false,
			...o,
		});
	}

	private async adsbfiRequest(
		role: Role,
		stale: boolean,
		token: string,
		term: number,
	): Promise<RequestOutcome> {
		const { lease, log } = this.deps;
		const outcome = this.outcomeFor(role);
		const result = await this.deps.fetchCycle();
		// The fetch can take seconds. If the lease was lost meanwhile, a
		// successor may already be publishing, so this request is dropped.
		if (lease.token !== token || this.term !== term) {
			if (!('error' in result)) {
				log('warn', 'lease lost during cycle: fetched positions discarded, not published', {
					lease_token: token,
					discarded: result.messages.length,
				});
			}
			return outcome({ leaseLost: true });
		}
		// Request or response failure, already logged by the adapter: provider
		// health evidence, and a failed active cycle.
		if ('error' in result) {
			const ev: HealthEvidence = { kind: 'failure', atMs: Date.now(), error: result.error };
			if (!(await this.recordHealth('adsbfi', token, term, ev))) {
				return outcome({ leaseLost: true });
			}
			return this.failRequest(role, 'adsbfi', token, outcome, true);
		}
		const split = result;

		// Freshness serves health and coverage, not authority. Frozen is a
		// health failure; seeded and unconfirmed are valid health successes
		// that may establish authority after a successful publish, but they do
		// not credit coverage until a later fresh cycle.
		const verdict: FreshnessVerdict = this.freshness.observe(split.responseNowMs, Date.now());
		if (verdict === 'frozen') {
			// A frozen feed fails validation, so nothing from it is published.
			log('warn', 'adsb.fi feed frozen: now has not advanced', {
				response_now_ms: split.responseNowMs,
				stale_for_ms: this.freshness.staleForMs(Date.now()),
				frozen_feed_ms: this.deps.frozenFeedMs,
			});
			const ev: HealthEvidence = { kind: 'failure', atMs: Date.now(), error: 'frozen_feed' };
			if (!(await this.recordHealth('adsbfi', token, term, ev))) {
				return outcome({ leaseLost: true });
			}
			return this.failRequest(role, 'adsbfi', token, outcome, true);
		}

		// Recorded before publishing, so a Kafka failure below cannot reach it.
		const success: HealthEvidence = { kind: 'success', atMs: Date.now() };
		if (!(await this.recordHealth('adsbfi', token, term, success, stale))) {
			return outcome({ leaseLost: true });
		}

		if (role === 'standby') {
			log('info', 'adsb.fi standby check', {
				freshness: verdict,
				aircraft: split.messages.length,
				state: this.health.adsbfi?.state ?? 'unknown',
			});
			if (this.failbackEligible(Date.now()) && !this.handoverInProgress) {
				await this.attemptFailback(token, term);
			}
			return outcome({});
		}
		if (role === 'candidate') {
			return this.deliverCandidate(
				'adsbfi',
				split.messages,
				token,
				term,
				verdict === 'fresh',
				outcome,
			);
		}
		if (role === 'handover') {
			return this.deliverHandover(
				split.messages,
				token,
				term,
				verdict === 'fresh',
				outcome,
			);
		}

		// Active cycle.
		const published = await this.publishAs('adsbfi', split.messages);
		if (published.status === 'authority_changed') {
			log('info', 'adsb.fi cycle dropped: authority changed before publishing');
			return outcome({});
		}
		if (published.status === 'failed') {
			// A failed publish fails the active cycle, but says nothing about
			// adsb.fi's health.
			log('error', 'poll cycle error', { error: published.error });
			return this.failRequest(role, 'adsbfi', token, outcome, false);
		}
		// The publish stage has completed (immediately after validation for a
		// cycle with nothing to publish): this is the coverage timestamp.
		const activeSuccessMs = Date.now();
		const credited = verdict === 'fresh';
		log('info', 'poll cycle complete', {
			aircraft_in_response: split.total,
			published: split.messages.length,
			skipped_non_icao: split.skippedNonIcao,
			skipped_no_position: split.skippedNoPosition,
			skipped_outside_box: split.skippedOutsideBox,
			first_offset: published.firstOffset,
			lease_token: token,
			freshness: verdict,
			coverage_credited: credited,
		});
		if (!credited) return outcome({});
		if (lease.token !== token) return outcome({ leaseLost: true });
		return (await this.creditCoverage(token, 'adsbfi', activeSuccessMs))
			? outcome({})
			: outcome({ leaseLost: true });
	}

	private async openskyRequest(
		role: Role,
		stale: boolean,
		token: string,
		term: number,
	): Promise<RequestOutcome> {
		const { lease, log } = this.deps;
		const outcome = this.outcomeFor(role);
		let result: OpenskyFetchResult;
		try {
			result = await this.deps.fetchOpensky();
		} catch (err) {
			result = { kind: 'failed', error: `error: ${errorMessage(err)}` };
		}
		if (lease.token !== token || this.term !== term) return outcome({ leaseLost: true });

		// Every OpenSky request is health evidence, whatever its role: there is
		// never a second request for the same purpose.
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
		if (!(await this.recordHealth('opensky', token, term, ev, stale))) {
			return outcome({ leaseLost: true });
		}
		const next = this.health.opensky;
		if (ev.kind === 'success' || next === null || next.state !== 'UNAVAILABLE') {
			this.openskyUnavailableStep = 0;
		} else if (next.pausedUntilMs === null) {
			this.openskyUnavailableStep =
				previous?.state === 'UNAVAILABLE' ? this.openskyUnavailableStep + 1 : 1;
		}
		log('info', 'opensky request', {
			provider: 'opensky',
			role,
			outcome: result.kind,
			...(result.kind === 'failed' ? { error: result.error } : {}),
			...(result.kind === 'ok' ? { aircraft: result.messages.length } : {}),
			state: next?.state ?? 'unknown',
			credits_remaining: next?.creditsRemaining ?? null,
			paused_until_ms: next?.pausedUntilMs ?? null,
		});

		if (result.kind !== 'ok') return this.failRequest(role, 'opensky', token, outcome, true);
		if (role === 'standby') return outcome({});
		if (role === 'candidate') {
			return this.deliverCandidate(
				'opensky',
				result.messages,
				token,
				term,
				true,
				outcome,
			);
		}

		// Active cycle.
		const published = await this.publishAs('opensky', result.messages);
		if (published.status === 'authority_changed') return outcome({});
		if (published.status === 'failed') {
			log('error', 'opensky cycle error', { error: published.error });
			return this.failRequest(role, 'opensky', token, outcome, false);
		}
		const activeSuccessMs = Date.now();
		log('info', 'opensky cycle complete', {
			provider: 'opensky',
			published: result.messages.length,
			first_offset: published.firstOffset,
			lease_token: token,
		});
		if (lease.token !== token) return outcome({ leaseLost: true });
		return (await this.creditCoverage(token, 'opensky', activeSuccessMs))
			? outcome({})
			: outcome({ leaseLost: true });
	}

	// A failed request. For an active cycle it also closes coverage (the first
	// failed active cycle, ADR-022 section 5); standby and candidate requests
	// have no coverage of their own.
	private async failRequest(
		role: Role,
		provider: Provider,
		token: string,
		outcome: (o: Partial<RequestOutcome>) => RequestOutcome,
		providerFailed: boolean,
	): Promise<RequestOutcome> {
		if (role !== 'active') return outcome({ providerFailed });
		// Only while this provider still holds authority: once relinquished,
		// RELINQUISH has already closed anything that was open.
		if (this.mode === provider && !(await this.closeCoverage(token, 'failure'))) {
			return outcome({ leaseLost: true });
		}
		return outcome({ providerFailed, cycleFailed: true });
	}

	// ---- Leader: publishing ---------------------------------------------------

	// Publishes as provider's active cycle, after any publish already running,
	// and only if provider still holds authority when its turn comes.
	private publishAs(
		provider: Provider,
		messages: Messages,
	): Promise<
		| { status: 'published'; firstOffset: string }
		| { status: 'failed'; error: string }
		| { status: 'authority_changed' }
	> {
		return this.serializedPublish(async () => {
			if (this.mode !== provider) return { status: 'authority_changed' as const };
			return this.sendAll(messages);
		});
	}

	private serializedPublish<T>(fn: () => Promise<T>): Promise<T> {
		const run = this.publishQueue.then(fn);
		this.publishQueue = run.catch(() => undefined);
		return run;
	}

	private async sendAll(
		messages: Messages,
	): Promise<{ status: 'published'; firstOffset: string } | { status: 'failed'; error: string }> {
		if (messages.length === 0) return { status: 'published', firstOffset: 'none' };
		try {
			return { status: 'published', firstOffset: await this.deps.publish(messages) };
		} catch (err) {
			return { status: 'failed', error: errorMessage(err) };
		}
	}

	// ---- Leader: selection and commit -------------------------------------

	// A candidate's delivery while authority is none: publish every message,
	// then commit authority. If this response qualifies as coverage, CREDIT
	// opens coverage separately at the same publish-completion timestamp.
	// Authority changes only if COMMIT succeeds.
	private async deliverCandidate(
		provider: Provider,
		messages: Messages,
		token: string,
		term: number,
		creditEligible: boolean,
		outcome: (o: Partial<RequestOutcome>) => RequestOutcome,
	): Promise<RequestOutcome> {
		const { log } = this.deps;
		// Candidate publish + COMMIT stay in the same serialization lane. If two
		// candidates finish fetching together, the second re-checks mode only
		// after the first has either committed authority or failed.
		return this.serializedPublish(async () => {
			if (this.mode !== 'none' || !this.current(token, term)) return outcome({});

			const published = await this.sendAll(messages);
			if (published.status === 'failed') {
				log('error', 'candidate publish failed: authority stays none', {
					provider,
					error: published.error,
				});
				return outcome({ deliveryFailed: true });
			}

			const commitMs = Date.now();
			if (this.mode !== 'none' || !this.current(token, term)) return outcome({});

			let result;
			try {
				result = await this.deps.timeline.commit(token, provider, commitMs);
			} catch (err) {
				this.loseLeadership(token, 'authority commit error: result unknown', err);
				return outcome({ leaseLost: true });
			}
			if (result.status === 'lease_mismatch') {
				this.loseLeadership(token, 'authority commit rejected: token no longer matches');
				return outcome({ leaseLost: true });
			}
			if (result.status === 'not_none') {
				// Redis holds an authority this coordinator believed was none.
				this.loseLeadership(token, `authority commit refused: authority is ${result.authority}`);
				return outcome({ leaseLost: true });
			}
			if (result.status === 'stale_clock') {
				log('warn', 'authority commit refused: time is not after the last success', {
					provider,
					commit_ms: commitMs,
				});
				return outcome({ deliveryFailed: true });
			}

			this.mode = provider;
			this.authoritySinceMs = commitMs;
			this.deliveryFailures[provider] = 0;
			log('info', 'authority committed', {
				provider,
				epoch: result.epoch,
				timeline_version: result.timelineVersion,
				commit_ms: commitMs,
				published: messages.length,
			});
			if (provider === 'opensky' && !this.deps.openskyAuthenticated) {
				log(
					'warn',
					'opensky authoritative without credentials: the anonymous budget will run out',
					{ active_interval_ms: this.deps.openskyActiveIntervalMs },
				);
			}
			if (creditEligible && !(await this.creditCoverage(token, provider, commitMs))) {
				return outcome({ committed: true, leaseLost: true });
			}

			// The other provider continues as a standby check at its own rate.
			const other: Provider = provider === 'adsbfi' ? 'opensky' : 'adsbfi';
			if (this.requestTimers[other] === null) {
				const delayMs =
					other === 'adsbfi'
						? this.deps.adsbfiStandbyIntervalMs
						: this.openskyStandbyDelayMs();
				this.scheduleRequest(other, delayMs, token, term);
			}
			return outcome({ committed: true });
		});
	}

	private failbackEligible(nowMs: number): boolean {
		return (
			this.mode === 'opensky' &&
			this.health.adsbfi?.state === 'HEALTHY' &&
			this.authoritySinceMs !== null &&
			nowMs - this.authoritySinceMs >= this.deps.failbackMinOpenskyAuthorityMs
		);
	}

	// Voluntary failback reuses the existing provider request loops and publish
	// lane. No extra timer is needed: each adsb.fi standby check re-evaluates
	// the five-minute condition.
	private async attemptFailback(token: string, term: number): Promise<void> {
		if (this.handoverInProgress || !this.current(token, term) || !this.failbackEligible(Date.now())) {
			return;
		}
		this.handoverInProgress = true;
		const timer = this.requestTimers.opensky;
		if (timer !== null) clearTimeout(timer);
		this.requestTimers.opensky = null;
		this.deps.log('info', 'failback attempt started', {
			from: 'opensky',
			to: 'adsbfi',
			authority_since_ms: this.authoritySinceMs,
		});

		try {
			// Let a cycle that already started finish. scheduleNext will not
			// arm another OpenSky request while handoverInProgress is true.
			await this.requestQueue.opensky;
			await this.publishQueue;
			if (!this.current(token, term) || !this.failbackEligible(Date.now())) return;

			if (!(await this.closeCoverage(token, 'handover_attempt'))) return;
			if (!this.current(token, term) || this.mode !== 'opensky') return;

			const result = await this.adsbfiRequest('handover', false, token, term);
			if (result.leaseLost) return;
			if (this.mode === 'opensky') {
				this.deps.log('warn', 'failback attempt failed: OpenSky remains authoritative');
			}
		} finally {
			this.handoverInProgress = false;
			if (!this.current(token, term)) return;
			if (this.mode === 'opensky' && this.requestTimers.opensky === null) {
				// A failed handover deliberately left OpenSky coverage closed;
				// the next successful active cycle reopens it truthfully.
				this.scheduleRequest('opensky', 0, token, term);
			} else if (this.mode === 'adsbfi' && this.requestTimers.opensky === null) {
				this.scheduleRequest('opensky', this.openskyStandbyDelayMs(), token, term);
			}
		}
	}

	private deliverHandover(
		messages: Messages,
		token: string,
		term: number,
		creditEligible: boolean,
		outcome: (o: Partial<RequestOutcome>) => RequestOutcome,
	): Promise<RequestOutcome> {
		const { log } = this.deps;
		return this.serializedPublish(async () => {
			if (
				!this.handoverInProgress ||
				this.mode !== 'opensky' ||
				!this.current(token, term)
			) {
				return outcome({});
			}

			const published = await this.sendAll(messages);
			if (published.status === 'failed') {
				log('error', 'failback publish failed: OpenSky remains authoritative', {
					error: published.error,
				});
				return outcome({ deliveryFailed: true });
			}

			const commitMs = Date.now();
			let result;
			try {
				result = await this.deps.timeline.handover(
					token,
					'opensky',
					'adsbfi',
					commitMs,
				);
			} catch (err) {
				this.loseLeadership(token, 'authority handover error: result unknown', err);
				return outcome({ leaseLost: true });
			}
			if (result.status === 'lease_mismatch') {
				this.loseLeadership(token, 'authority handover rejected: token no longer matches');
				return outcome({ leaseLost: true });
			}
			if (result.status === 'unexpected_provider' || result.status === 'coverage_open') {
				this.loseLeadership(
					token,
					result.status === 'unexpected_provider'
						? `authority handover refused: authority is ${result.authority ?? 'not initialized'}`
						: 'authority handover refused: OpenSky coverage is still open',
				);
				return outcome({ leaseLost: true });
			}
			if (result.status === 'stale_clock') {
				log('warn', 'authority handover refused: time is not after the last success');
				return outcome({ deliveryFailed: true });
			}

			this.mode = 'adsbfi';
			this.authoritySinceMs = commitMs;
			log('info', 'failback committed', {
				from: 'opensky',
				to: 'adsbfi',
				epoch: result.epoch,
				timeline_version: result.timelineVersion,
				commit_ms: commitMs,
				published: messages.length,
			});
			if (creditEligible && !(await this.creditCoverage(token, 'adsbfi', commitMs))) {
				return outcome({ committed: true, leaseLost: true });
			}
			return outcome({ committed: true });
		});
	}

	// The authoritative provider has become UNAVAILABLE: stop its active
	// cycles at once and relinquish authority to none, then select.
	private async relinquish(provider: Provider, token: string, term: number): Promise<void> {
		if (this.mode !== provider || !this.current(token, term)) return;
		const { log } = this.deps;
		// No new active cycle of this provider starts from here, and a cycle
		// that has not reached its publish drops out when its turn comes.
		this.mode = 'none';
		this.authoritySinceMs = null;
		const timer = this.requestTimers[provider];
		if (timer !== null) clearTimeout(timer);
		this.requestTimers[provider] = null;
		log('warn', 'authoritative provider UNAVAILABLE: relinquishing authority', { provider });

		let result;
		try {
			result = await this.deps.timeline.relinquish(token, provider, Date.now());
		} catch (err) {
			this.loseLeadership(token, 'authority relinquish error: result unknown', err);
			return;
		}
		if (result.status !== 'relinquished') {
			this.loseLeadership(
				token,
				result.status === 'lease_mismatch'
					? 'authority relinquish rejected: token no longer matches'
					: `authority relinquish refused: ${result.status}`,
			);
			return;
		}
		log('info', 'authority relinquished: now none', {
			from: provider,
			timeline_version: result.timelineVersion,
			closed_segment: result.member,
		});
		this.enterNone(token, term, 'relinquished');
	}

	// A genuine entry into none: one-shots and per-provider delivery backoff
	// start over.
	private enterNone(token: string, term: number, reason: string): void {
		this.oneShotUsed = new Set();
		this.deliveryFailures = { adsbfi: 0, opensky: 0 };
		this.startSelectionRound(token, term, reason);
	}

	private startSelectionRound(token: string, term: number, reason: string): void {
		if (this.round !== null || !this.current(token, term)) return;
		this.round = this.runSelectionRound(token, term, reason).finally(() => {
			this.round = null;
		});
	}

	// Tries eligible providers one after another, stopping at the first
	// commit. Each attempt is that provider's next request, made now.
	private async runSelectionRound(token: string, term: number, reason: string): Promise<void> {
		const nowMs = Date.now();
		const stale = {
			adsbfi: this.isProviderStale('adsbfi', nowMs),
			opensky: this.isProviderStale('opensky', nowMs),
		};
		const plan = planSelectionRound(this.health, stale, nowMs, this.oneShotUsed);
		this.deps.log('info', 'selection round', {
			reason,
			plan: plan.map((p) => `${p.provider}:tier${p.tier}${p.oneShot ? ':one_shot' : ''}`),
		});
		for (const attempt of plan) {
			if (this.mode !== 'none' || !this.current(token, term)) break;
			if (attempt.oneShot) this.oneShotUsed.add(attempt.provider);
			const timer = this.requestTimers[attempt.provider];
			if (timer !== null) clearTimeout(timer);
			this.requestTimers[attempt.provider] = null;
			await this.request(attempt.provider, token, term, 'round');
		}
		if (!this.current(token, term)) return;
		if (this.mode === 'none') {
			this.deps.log('warn', 'selection round ended without an authority', { reason });
		}
		// Every provider keeps a request scheduled at its own rate.
		for (const provider of PROVIDERS) {
			if (this.requestTimers[provider] === null) {
				const delayMs =
					provider === 'adsbfi' ? this.deps.adsbfiStandbyIntervalMs : this.openskyStandbyDelayMs();
				this.scheduleRequest(provider, delayMs, token, term);
			}
		}
	}

	private isProviderStale(provider: Provider, nowMs: number): boolean {
		const h = this.health[provider];
		const intervalMs =
			provider === 'adsbfi'
				? Math.max(
						this.deps.adsbfiStandbyIntervalMs,
						Math.min(
							this.deps.backoffMaxMs,
							this.deps.backoffBaseMs * 2 ** Math.max(0, this.adsbfiFailures - 1),
						),
					)
				: nextOpenskyCheckDelayMs(
						h,
						nowMs,
						Math.max(1, this.openskyUnavailableStep),
						this.deps.openskyCadence,
					);
		return isStale(h, nowMs, intervalMs);
	}

	// ADR-022 section 7, after health: a stored authority whose provider is
	// restored UNAVAILABLE (including paused) is relinquished before any
	// request; a stored none, or a timeline that never had an authority,
	// enters selection; a stored authority that is DEGRADED keeps authority.
	private async restoreAuthority(
		token: string,
		term: number,
		restored: RestoredState,
	): Promise<boolean> {
		const { log } = this.deps;
		const stored = restored.authorityProvider;
		if (!restored.authorityInitialized || stored === 'none') {
			log('info', 'authority restored: none', {
				never_committed: !restored.authorityInitialized,
			});
			this.mode = 'none';
			this.authoritySinceMs = null;
			this.enterNone(
				token,
				term,
				restored.authorityInitialized ? 'restored_none' : 'first_deployment',
			);
			return true;
		}
		if (stored !== 'adsbfi' && stored !== 'opensky') {
			this.loseLeadership(token, `stored authority is not a known provider: ${stored}`);
			return false;
		}
		this.mode = stored;
		this.authoritySinceMs = restored.authoritySinceMs;
		log('info', 'authority restored', {
			provider: stored,
			health: this.health[stored]?.state ?? 'unknown',
		});
		if (this.health[stored]?.state === 'UNAVAILABLE') await this.relinquish(stored, token, term);
		return this.current(token, term);
	}

	// ---- Leader: coverage timeline -----------------------------------------

	// Returns false when the write was refused or its result is unknown. The
	// lease is then dropped, so nothing more is published under this
	// acquisition. A credit for a provider that no longer holds authority is
	// benign: its cycle finished after authority moved on.
	private async creditCoverage(
		token: string,
		provider: Provider,
		activeSuccessMs: number,
	): Promise<boolean> {
		const { log } = this.deps;
		try {
			const result = await this.deps.timeline.credit(token, provider, activeSuccessMs);
			if (result.status === 'lease_mismatch') {
				this.loseLeadership(token, 'timeline write rejected: token no longer matches');
				return false;
			}
			if (result.status === 'authority_changed') {
				// Benign only for a cycle that finished after this coordinator had
				// already moved authority on. If it still believes this provider
				// holds authority, memory and Redis disagree: publishing on would
				// be unsafe, so it fails closed and the next acquisition resyncs.
				if (this.mode === provider) {
					this.loseLeadership(
						token,
						`authority is ${result.authority ?? 'not initialized'} in Redis, not ${provider}`,
					);
					return false;
				}
				log('info', 'coverage not credited: authority changed', {
					provider,
					authority: result.authority,
				});
			} else if (result.status === 'opened') {
				log('info', 'coverage opened', {
					provider,
					active_success_ms: activeSuccessMs,
					timeline_version: result.timelineVersion,
				});
			} else if (result.status === 'stale_clock') {
				log('warn', 'coverage not credited: time is not after the last success', {
					provider,
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
	// otherwise.
	private recordHealth(
		provider: Provider,
		token: string,
		term: number,
		ev: HealthEvidence,
		stale = false,
	): Promise<boolean> {
		return this.serializeHealth(provider, async () => {
			if (this.term !== term || this.deps.lease.token !== token) return false;
			const next = applyEvidence(this.health[provider], ev, this.deps.healthTiming, {
				firstDeployment: provider === 'adsbfi' && this.firstDeployment,
				stale,
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
		// The authoritative provider has become UNAVAILABLE. Not awaited: the
		// relinquish and its selection round must not run inside this
		// provider's health queue.
		if (this.mode === provider && next.state === 'UNAVAILABLE') {
			void this.relinquish(provider, token, term);
		}
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

	// ADR-022 section 7, at acquisition and before any request. Returns what
	// authority restoration needs, or null when the lease was lost.
	private async restoreProviderHealth(token: string, term: number): Promise<RestoredState | null> {
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

		const opensky = this.health.opensky;
		let openskyDelayMs: number;
		if (openskyPauseExpired) {
			openskyDelayMs = 0;
		} else {
			if (opensky?.state === 'UNAVAILABLE' && opensky.pausedUntilMs === null) {
				// A restored outage restarts its backoff at the initial value.
				this.openskyUnavailableStep = 1;
			}
			openskyDelayMs = this.openskyStandbyDelayMs();
		}
		return {
			authorityInitialized: snapshot.authorityInitialized,
			authorityProvider: snapshot.authorityProvider,
			authoritySinceMs: snapshot.authoritySinceMs,
			openskyDelayMs,
		};
	}

	// ---- Clean shutdown ----------------------------------------------------

	// Order matters: the lease is held until the last in-flight publish has
	// finished, so a successor cannot start publishing alongside it.
	async shutdown(): Promise<void> {
		// 1. Stop starting new requests, rounds and acquisition attempts.
		this.stopping = true;
		if (this.acquireTimer !== null) clearTimeout(this.acquireTimer);
		this.acquireTimer = null;
		for (const provider of PROVIDERS) {
			const timer = this.requestTimers[provider];
			if (timer !== null) clearTimeout(timer);
			this.requestTimers[provider] = null;
		}
		if (this.pendingAcquire !== null) await this.pendingAcquire;

		// 2. Let in-flight requests and any selection round finish, including
		// their publishes. Renewal keeps running meanwhile, so the lease is
		// still held. Health stays in Redis for the next holder.
		if (this.round !== null) await this.round;
		await Promise.allSettled([...this.inFlight]);
		await this.publishQueue;
		this.clearTimers();

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
