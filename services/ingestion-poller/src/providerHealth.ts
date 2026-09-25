// Provider health state machine (ADR-022 section 2).
//
// Health is an opinion about a provider formed only from requests to it: a
// request that succeeded, failed, or was refused with a retry time. It is
// never judged from individual aircraft, and it never depends on Kafka: the
// coordinator records a request's outcome right after the request and its
// validation, before publishing, so a later publish failure cannot touch it.
//
// Pure: no Redis, no timers. The coordinator owns the clock, the deadline
// timer and persistence (providerHealthStore.ts), and passes times in.
//
// In CP3d nothing acts on health. It may say adsb.fi is UNAVAILABLE while
// adsb.fi stays authoritative; switching belongs to CP3e.

export type Provider = 'adsbfi' | 'opensky';
export type HealthState = 'HEALTHY' | 'DEGRADED' | 'UNAVAILABLE' | 'RECOVERING';

const STATES: readonly HealthState[] = ['HEALTHY', 'DEGRADED', 'UNAVAILABLE', 'RECOVERING'];

// null in a field means "never happened" and is stored as an empty string.
// Unknown health (no record at all) is a null ProviderHealth, never a state.
export interface ProviderHealth {
	state: HealthState;
	stateSinceMs: number;
	lastSuccessMs: number | null;
	lastFailureMs: number | null;
	consecutiveFailures: number;
	// The most recently observed error, kept after recovery: it is history,
	// not the current state.
	lastError: string | null;
	// Only while RECOVERING: when the current unbroken run of successes began.
	successStreakSinceMs: number | null;
	// OpenSky only.
	pausedUntilMs: number | null;
	creditsRemaining: number | null;
	lastProbeMs: number | null;
}

export type HealthEvidence =
	| { kind: 'success'; atMs: number; creditsRemaining?: number | null; probe?: boolean }
	| { kind: 'failure'; atMs: number; error: string; probe?: boolean }
	// An OpenSky 429 with a usable retry time: straight to UNAVAILABLE, paused.
	| { kind: 'paused'; atMs: number; error: string; pausedUntilMs: number; probe?: boolean };

export interface TransitionTiming {
	degradedTimeoutMs: number;
	recoveryWindowMs: number;
}

function enter(
	h: ProviderHealth,
	state: HealthState,
	atMs: number,
	streakSinceMs: number | null = null,
): ProviderHealth {
	return { ...h, state, stateSinceMs: atMs, successStreakSinceMs: streakSinceMs };
}

// DEGRADED whose 60 s ran out before this evidence was processed becomes
// UNAVAILABLE at the logical deadline first, whether or not the timer has
// fired yet. Evidence is then applied to that state.
function settleDeadline(h: ProviderHealth, atMs: number, timing: TransitionTiming): ProviderHealth {
	if (h.state !== 'DEGRADED') return h;
	const deadlineMs = h.stateSinceMs + timing.degradedTimeoutMs;
	return atMs >= deadlineMs ? enter(h, 'UNAVAILABLE', deadlineMs) : h;
}

function emptyHealth(atMs: number): ProviderHealth {
	return {
		state: 'UNAVAILABLE',
		stateSinceMs: atMs,
		lastSuccessMs: null,
		lastFailureMs: null,
		consecutiveFailures: 0,
		lastError: null,
		successStreakSinceMs: null,
		pausedUntilMs: null,
		creditsRemaining: null,
		lastProbeMs: null,
	};
}

// One request outcome. `firstDeployment` is decided at lease acquisition (no
// initialized authority and no health record for any provider). Only then
// does a first success mean HEALTHY; otherwise unknown health must prove
// itself through RECOVERING. `stale` means the provider had not been asked
// for longer than its check interval before this request (ADR-022 section
// 4): whatever its stored state, a success then starts RECOVERING afresh,
// and a failure is applied to the stored state as usual.
export function applyEvidence(
	current: ProviderHealth | null,
	ev: HealthEvidence,
	timing: TransitionTiming,
	opts: { firstDeployment?: boolean; stale?: boolean } = {},
): ProviderHealth {
	const at = ev.atMs;
	const probe = ev.probe ? { lastProbeMs: at } : {};

	if (ev.kind === 'success') {
		const recorded = (h: ProviderHealth): ProviderHealth => ({
			...h,
			...probe,
			lastSuccessMs: at,
			consecutiveFailures: 0,
			pausedUntilMs: null,
			creditsRemaining:
				ev.creditsRemaining === undefined || ev.creditsRemaining === null
					? h.creditsRemaining
					: ev.creditsRemaining,
		});
		if (current === null) {
			const base = emptyHealth(at);
			return recorded(
				opts.firstDeployment ? enter(base, 'HEALTHY', at) : enter(base, 'RECOVERING', at, at),
			);
		}
		const h = settleDeadline(current, at, timing);
		if (opts.stale) return recorded(enter(h, 'RECOVERING', at, at));
		switch (h.state) {
			case 'HEALTHY':
				return recorded(h);
			case 'DEGRADED':
				return recorded(enter(h, 'HEALTHY', at));
			case 'UNAVAILABLE':
				return recorded(enter(h, 'RECOVERING', at, at));
			case 'RECOVERING': {
				const streak = h.successStreakSinceMs ?? at;
				return recorded(at - streak >= timing.recoveryWindowMs ? enter(h, 'HEALTHY', at) : h);
			}
		}
	}

	const recorded = (h: ProviderHealth): ProviderHealth => ({
		...h,
		...probe,
		lastFailureMs: at,
		consecutiveFailures: h.consecutiveFailures + 1,
		lastError: ev.error,
		// A new retry time replaces any pause; an ordinary failure clears an
		// obsolete one (no request is ever made while a pause is active).
		pausedUntilMs: ev.kind === 'paused' ? ev.pausedUntilMs : null,
	});
	if (current === null) return recorded(emptyHealth(at));
	const h = settleDeadline(current, at, timing);
	if (ev.kind === 'paused') {
		return recorded(h.state === 'UNAVAILABLE' ? h : enter(h, 'UNAVAILABLE', at));
	}
	switch (h.state) {
		case 'HEALTHY':
			return recorded(enter(h, 'DEGRADED', at));
		case 'DEGRADED':
		case 'UNAVAILABLE':
			// The DEGRADED clock runs from entry; repeated failures never move it.
			return recorded(h);
		case 'RECOVERING':
			return recorded(enter(h, 'UNAVAILABLE', at));
	}
}

// The deadline timer's transition. It carries the state_since_ms of the
// DEGRADED term that armed it, and does nothing unless that same term is
// still current: a success, or a later DEGRADED term, must never be
// overwritten by an older timer. Returns null when nothing changes.
export function expireDegraded(
	current: ProviderHealth | null,
	termStateSinceMs: number,
	nowMs: number,
	timing: TransitionTiming,
): ProviderHealth | null {
	if (current === null || current.state !== 'DEGRADED') return null;
	if (current.stateSinceMs !== termStateSinceMs) return null;
	const deadlineMs = current.stateSinceMs + timing.degradedTimeoutMs;
	if (nowMs < deadlineMs) return null;
	return enter(current, 'UNAVAILABLE', deadlineMs);
}

// Restoring stored health at lease acquisition (ADR-022 section 7).
// Downtime never counts toward recovery: a streak is lost and DEGRADED gets a
// fresh 60 s from acquisition. A pause still in the future is kept exactly;
// one already over is cleared, and the caller may check at once.
export function restoreHealth(
	stored: ProviderHealth | null,
	acquisitionMs: number,
): { health: ProviderHealth | null; pauseExpired: boolean } {
	if (stored === null) return { health: null, pauseExpired: false };
	const pauseExpired = stored.pausedUntilMs !== null && stored.pausedUntilMs <= acquisitionMs;
	const h = pauseExpired ? { ...stored, pausedUntilMs: null } : stored;
	switch (h.state) {
		case 'HEALTHY':
		case 'DEGRADED':
			return { health: enter(h, 'DEGRADED', acquisitionMs), pauseExpired };
		case 'RECOVERING':
			return { health: enter(h, 'UNAVAILABLE', acquisitionMs), pauseExpired };
		case 'UNAVAILABLE':
			return { health: h, pauseExpired };
	}
}

// ---- Redis hash fields (ADR-022 section 7) ----------------------------------

const str = (v: number | string | null): string => (v === null ? '' : String(v));

export function toHashFields(provider: Provider, h: ProviderHealth): string[] {
	const fields = [
		'state',
		h.state,
		'state_since_ms',
		str(h.stateSinceMs),
		'last_success_ms',
		str(h.lastSuccessMs),
		'last_failure_ms',
		str(h.lastFailureMs),
		'consecutive_failures',
		str(h.consecutiveFailures),
		'last_error',
		str(h.lastError),
		'success_streak_since_ms',
		str(h.successStreakSinceMs),
	];
	if (provider === 'opensky') {
		fields.push(
			'paused_until_ms',
			str(h.pausedUntilMs),
			'credits_remaining',
			str(h.creditsRemaining),
			'last_probe_ms',
			str(h.lastProbeMs),
		);
	}
	return fields;
}

const WHOLE = /^\d+$/;

function optionalMs(v: string | undefined): number | null | undefined {
	if (v === undefined || v === '') return null;
	return WHOLE.test(v) ? Number(v) : undefined;
}

// A record with no state is unknown health. One that cannot be read is also
// treated as unknown, the conservative reading (its next success must prove
// itself through RECOVERING), and the problem is returned for logging.
export function parseHealthHash(record: Record<string, string>): {
	health: ProviderHealth | null;
	problem: string | null;
} {
	if (!record['state']) return { health: null, problem: null };
	const state = record['state'] as HealthState;
	if (!STATES.includes(state)) return { health: null, problem: `unknown state ${record['state']}` };
	const stateSinceMs = optionalMs(record['state_since_ms']);
	const values = {
		lastSuccessMs: optionalMs(record['last_success_ms']),
		lastFailureMs: optionalMs(record['last_failure_ms']),
		successStreakSinceMs: optionalMs(record['success_streak_since_ms']),
		pausedUntilMs: optionalMs(record['paused_until_ms']),
		creditsRemaining: optionalMs(record['credits_remaining']),
		lastProbeMs: optionalMs(record['last_probe_ms']),
		consecutiveFailures: optionalMs(record['consecutive_failures']),
	};
	if (typeof stateSinceMs !== 'number') {
		return { health: null, problem: 'state_since_ms is not a timestamp' };
	}
	for (const [name, value] of Object.entries(values)) {
		if (value === undefined) return { health: null, problem: `${name} is not a whole number` };
	}
	return {
		health: {
			state,
			stateSinceMs,
			lastSuccessMs: values.lastSuccessMs as number | null,
			lastFailureMs: values.lastFailureMs as number | null,
			consecutiveFailures: (values.consecutiveFailures as number | null) ?? 0,
			lastError: record['last_error'] ? record['last_error'] : null,
			successStreakSinceMs: values.successStreakSinceMs as number | null,
			pausedUntilMs: values.pausedUntilMs as number | null,
			creditsRemaining: values.creditsRemaining as number | null,
			lastProbeMs: values.lastProbeMs as number | null,
		},
		problem: null,
	};
}

// ---- last_error vocabulary ---------------------------------------------------

// Classes from the shapes observed against the live APIs: `timeout` (fetch's
// TimeoutError), `network:<code>` (TypeError "fetch failed" with a cause
// code such as ENOTFOUND). Status, validation, rate-limit, auth and frozen-feed
// classes are built by the adapters themselves: `http_<status>`,
// `validation: <message>`, `rate_limited`, `auth: <message>`, `frozen_feed`.
export function classifyRequestError(err: unknown): string {
	const e = err as { name?: string; message?: string; cause?: { code?: unknown } };
	if (e?.name === 'TimeoutError' || e?.name === 'AbortError') return 'timeout';
	if (e?.name === 'TypeError' && e.message === 'fetch failed') {
		return typeof e.cause?.code === 'string' ? `network:${e.cause.code}` : 'network';
	}
	return `error: ${e?.message ?? String(err)}`;
}

// ---- OpenSky standby check rates (ADR-022 section 2) -------------------------

export interface OpenskyCadence {
	healthyMs: number;
	degradedMs: number;
	recoveringMs: number;
	backoffBaseMs: number;
	backoffMaxMs: number;
}

// setTimeout's largest delay. A retry time beyond it waits the maximum and
// the next plan waits again.
const MAX_TIMER_DELAY_MS = 2_147_483_647;

// Delay before the next OpenSky check, from its health after the last one.
// `unavailableStep` is how many checks in a row have failed while UNAVAILABLE
// and not paused, counted from 1; the coordinator resets it on success, on a
// fresh entry into UNAVAILABLE, and at every lease acquisition.
export function nextOpenskyCheckDelayMs(
	h: ProviderHealth | null,
	nowMs: number,
	unavailableStep: number,
	cadence: OpenskyCadence,
): number {
	if (h === null) return 0;
	if (h.pausedUntilMs !== null && h.pausedUntilMs > nowMs) {
		return Math.min(MAX_TIMER_DELAY_MS, h.pausedUntilMs - nowMs);
	}
	switch (h.state) {
		case 'HEALTHY':
			return cadence.healthyMs;
		case 'DEGRADED':
			return cadence.degradedMs;
		case 'RECOVERING':
			return cadence.recoveringMs;
		case 'UNAVAILABLE':
			return Math.min(
				cadence.backoffMaxMs,
				cadence.backoffBaseMs * 2 ** (Math.max(1, unavailableStep) - 1),
			);
	}
}
