// Unit tests for the provider health state machine (ADR-022 section 2). Pure:
// no Redis, no timers. Times are offsets from T so each value reads by eye.
import { describe, expect, it } from 'vitest';
import {
	applyEvidence,
	classifyRequestError,
	expireDegraded,
	nextOpenskyCheckDelayMs,
	parseHealthHash,
	restoreHealth,
	toHashFields,
	type HealthEvidence,
	type ProviderHealth,
} from './providerHealth.js';

const T = 1_790_400_000_000;
const TIMING = { degradedTimeoutMs: 60_000, recoveryWindowMs: 120_000 };

function health(overrides: Partial<ProviderHealth>): ProviderHealth {
	return {
		state: 'HEALTHY',
		stateSinceMs: T,
		lastSuccessMs: T,
		lastFailureMs: null,
		consecutiveFailures: 0,
		lastError: null,
		successStreakSinceMs: null,
		pausedUntilMs: null,
		creditsRemaining: null,
		lastProbeMs: null,
		...overrides,
	};
}

const ok = (atMs: number, extra: Partial<HealthEvidence> = {}): HealthEvidence =>
	({ kind: 'success', atMs, ...extra }) as HealthEvidence;
const fail = (atMs: number, error = 'timeout'): HealthEvidence => ({
	kind: 'failure',
	atMs,
	error,
});

describe('applyEvidence: transitions', () => {
	it('HEALTHY + success stays HEALTHY without moving state_since_ms', () => {
		const next = applyEvidence(health({}), ok(T + 5_000), TIMING);
		expect(next.state).toBe('HEALTHY');
		expect(next.stateSinceMs).toBe(T);
		expect(next.lastSuccessMs).toBe(T + 5_000);
	});

	it('HEALTHY + failure enters DEGRADED at the failure time', () => {
		const next = applyEvidence(health({}), fail(T + 10_000, 'http_503'), TIMING);
		expect(next).toMatchObject({
			state: 'DEGRADED',
			stateSinceMs: T + 10_000,
			lastFailureMs: T + 10_000,
			consecutiveFailures: 1,
			lastError: 'http_503',
		});
	});

	it('repeated failures in DEGRADED do not move the 60 s clock', () => {
		let h = applyEvidence(health({}), fail(T + 10_000), TIMING);
		h = applyEvidence(h, fail(T + 25_000, 'network:ECONNRESET'), TIMING);
		h = applyEvidence(h, fail(T + 59_000), TIMING);
		expect(h.state).toBe('DEGRADED');
		expect(h.stateSinceMs).toBe(T + 10_000);
		expect(h.consecutiveFailures).toBe(3);
		expect(h.lastFailureMs).toBe(T + 59_000);
	});

	it('DEGRADED + success at 59.9 s returns to HEALTHY', () => {
		const degraded = health({ state: 'DEGRADED', stateSinceMs: T, consecutiveFailures: 2 });
		const next = applyEvidence(degraded, ok(T + 59_900), TIMING);
		expect(next).toMatchObject({
			state: 'HEALTHY',
			stateSinceMs: T + 59_900,
			consecutiveFailures: 0,
		});
	});

	it('DEGRADED evidence at exactly 60 s first expires at the logical deadline', () => {
		const degraded = health({ state: 'DEGRADED', stateSinceMs: T });
		// A success exactly at the deadline is too late: UNAVAILABLE at T+60 s, then RECOVERING.
		const success = applyEvidence(degraded, ok(T + 60_000), TIMING);
		expect(success).toMatchObject({ state: 'RECOVERING', successStreakSinceMs: T + 60_000 });
		// A failure exactly at the deadline lands in UNAVAILABLE stamped at the deadline.
		const failure = applyEvidence(degraded, fail(T + 61_500), TIMING);
		expect(failure).toMatchObject({ state: 'UNAVAILABLE', stateSinceMs: T + 60_000 });
	});

	it('UNAVAILABLE + failure stays UNAVAILABLE; + success enters RECOVERING with a streak', () => {
		const unavailable = health({ state: 'UNAVAILABLE', stateSinceMs: T, consecutiveFailures: 4 });
		const still = applyEvidence(unavailable, fail(T + 5_000), TIMING);
		expect(still).toMatchObject({ state: 'UNAVAILABLE', stateSinceMs: T, consecutiveFailures: 5 });
		const recovering = applyEvidence(unavailable, ok(T + 9_000), TIMING);
		expect(recovering).toMatchObject({
			state: 'RECOVERING',
			stateSinceMs: T + 9_000,
			successStreakSinceMs: T + 9_000,
			consecutiveFailures: 0,
		});
	});

	it('RECOVERING becomes HEALTHY only at 120 s of continuous success', () => {
		const recovering = health({ state: 'RECOVERING', stateSinceMs: T, successStreakSinceMs: T });
		const early = applyEvidence(recovering, ok(T + 119_900), TIMING);
		expect(early).toMatchObject({ state: 'RECOVERING', stateSinceMs: T, successStreakSinceMs: T });
		const done = applyEvidence(recovering, ok(T + 120_000), TIMING);
		expect(done).toMatchObject({
			state: 'HEALTHY',
			stateSinceMs: T + 120_000,
			successStreakSinceMs: null,
		});
	});

	it('any failure while RECOVERING returns to UNAVAILABLE and clears the streak', () => {
		const recovering = health({ state: 'RECOVERING', stateSinceMs: T, successStreakSinceMs: T });
		const next = applyEvidence(recovering, fail(T + 119_000, 'frozen_feed'), TIMING);
		expect(next).toMatchObject({
			state: 'UNAVAILABLE',
			stateSinceMs: T + 119_000,
			successStreakSinceMs: null,
			lastError: 'frozen_feed',
		});
	});

	it('success keeps the last error but resets the failure count', () => {
		const degraded = health({
			state: 'DEGRADED',
			stateSinceMs: T,
			consecutiveFailures: 3,
			lastError: 'http_503',
			lastFailureMs: T + 1_000,
		});
		const next = applyEvidence(degraded, ok(T + 2_000), TIMING);
		expect(next).toMatchObject({
			consecutiveFailures: 0,
			lastError: 'http_503',
			lastFailureMs: T + 1_000,
			lastSuccessMs: T + 2_000,
		});
	});
});

describe('applyEvidence: unknown health and first deployment', () => {
	it('unknown + success enters RECOVERING', () => {
		expect(applyEvidence(null, ok(T), TIMING)).toMatchObject({
			state: 'RECOVERING',
			stateSinceMs: T,
			successStreakSinceMs: T,
			lastSuccessMs: T,
		});
	});

	it('unknown + failure enters UNAVAILABLE', () => {
		expect(applyEvidence(null, fail(T, 'network:ENOTFOUND'), TIMING)).toMatchObject({
			state: 'UNAVAILABLE',
			stateSinceMs: T,
			consecutiveFailures: 1,
			lastError: 'network:ENOTFOUND',
		});
	});

	it('true first deployment: the first valid response initializes HEALTHY', () => {
		expect(applyEvidence(null, ok(T), TIMING, { firstDeployment: true })).toMatchObject({
			state: 'HEALTHY',
			stateSinceMs: T,
			successStreakSinceMs: null,
		});
	});

	it('first deployment does not change a failure, or a provider that already has health', () => {
		expect(applyEvidence(null, fail(T), TIMING, { firstDeployment: true }).state).toBe(
			'UNAVAILABLE',
		);
		const unavailable = health({ state: 'UNAVAILABLE' });
		expect(applyEvidence(unavailable, ok(T + 1), TIMING, { firstDeployment: true }).state).toBe(
			'RECOVERING',
		);
	});
});

describe('applyEvidence: after stale health (CP3e)', () => {
	it('a success after staleness enters RECOVERING with a fresh streak, even from HEALTHY', () => {
		for (const state of ['HEALTHY', 'DEGRADED', 'RECOVERING', 'UNAVAILABLE'] as const) {
			const next = applyEvidence(
				health({ state, successStreakSinceMs: state === 'RECOVERING' ? T - 500_000 : null }),
				ok(T + 1_000),
				TIMING,
				{ stale: true },
			);
			expect(next).toMatchObject({
				state: 'RECOVERING',
				stateSinceMs: T + 1_000,
				successStreakSinceMs: T + 1_000,
			});
		}
	});

	it('a failure after staleness follows the stored state normally', () => {
		expect(
			applyEvidence(health({ state: 'HEALTHY' }), fail(T + 1), TIMING, { stale: true }).state,
		).toBe('DEGRADED');
		expect(
			applyEvidence(health({ state: 'UNAVAILABLE' }), fail(T + 1), TIMING, { stale: true }).state,
		).toBe('UNAVAILABLE');
	});
});

describe('applyEvidence: OpenSky pause, credits and probes', () => {
	const paused = (atMs: number, pausedUntilMs: number): HealthEvidence => ({
		kind: 'paused',
		atMs,
		error: 'rate_limited',
		pausedUntilMs,
		probe: true,
	});

	it('a 429 with a retry time goes straight to UNAVAILABLE from any state', () => {
		for (const state of ['HEALTHY', 'DEGRADED', 'RECOVERING'] as const) {
			const next = applyEvidence(
				health({ state, successStreakSinceMs: state === 'RECOVERING' ? T : null }),
				paused(T + 1_000, T + 31_952_000),
				TIMING,
			);
			expect(next).toMatchObject({
				state: 'UNAVAILABLE',
				stateSinceMs: T + 1_000,
				pausedUntilMs: T + 31_952_000,
				successStreakSinceMs: null,
				lastError: 'rate_limited',
				lastProbeMs: T + 1_000,
			});
		}
		const unknown = applyEvidence(null, paused(T, T + 5_000), TIMING);
		expect(unknown).toMatchObject({ state: 'UNAVAILABLE', pausedUntilMs: T + 5_000 });
	});

	it('a 429 without a retry time is an ordinary failure', () => {
		const next = applyEvidence(health({}), fail(T + 1_000, 'rate_limited'), TIMING);
		expect(next).toMatchObject({ state: 'DEGRADED', pausedUntilMs: null });
	});

	it('success and ordinary failures clear an obsolete pause', () => {
		const obsolete = health({ state: 'UNAVAILABLE', pausedUntilMs: T - 1 });
		expect(applyEvidence(obsolete, ok(T), TIMING).pausedUntilMs).toBeNull();
		expect(applyEvidence(obsolete, fail(T), TIMING).pausedUntilMs).toBeNull();
	});

	it('credits update only when the header was usable; probes stamp last_probe_ms', () => {
		const h = health({ creditsRemaining: 399 });
		const withHeader = applyEvidence(h, ok(T + 1, { creditsRemaining: 398, probe: true }), TIMING);
		expect(withHeader).toMatchObject({ creditsRemaining: 398, lastProbeMs: T + 1 });
		const noHeader = applyEvidence(h, ok(T + 2, { creditsRemaining: null, probe: true }), TIMING);
		expect(noHeader.creditsRemaining).toBe(399);
		const authFailure = applyEvidence(
			h,
			{ kind: 'failure', atMs: T + 3, error: 'auth: HTTP 401', probe: true },
			TIMING,
		);
		expect(authFailure).toMatchObject({
			lastProbeMs: T + 3,
			lastError: 'auth: HTTP 401',
			creditsRemaining: 399,
		});
	});
});

describe('expireDegraded: the deadline term guard', () => {
	const degraded = health({ state: 'DEGRADED', stateSinceMs: T });

	it('expires the same DEGRADED term, stamped at the logical deadline', () => {
		const next = expireDegraded(degraded, T, T + 71_500, TIMING);
		expect(next).toMatchObject({ state: 'UNAVAILABLE', stateSinceMs: T + 60_000 });
	});

	it('does nothing before the deadline', () => {
		expect(expireDegraded(degraded, T, T + 59_999, TIMING)).toBeNull();
	});

	it('a stale callback cannot overwrite a newer success', () => {
		const healthy = applyEvidence(degraded, ok(T + 59_900), TIMING);
		expect(expireDegraded(healthy, T, T + 60_000, TIMING)).toBeNull();
	});

	it('a stale callback cannot end a later DEGRADED term early', () => {
		const later = health({ state: 'DEGRADED', stateSinceMs: T + 70_000 });
		expect(expireDegraded(later, T, T + 71_000, TIMING)).toBeNull();
	});

	it('if the deadline wins first, a later success follows UNAVAILABLE to RECOVERING', () => {
		const unavailable = expireDegraded(degraded, T, T + 60_000, TIMING)!;
		const next = applyEvidence(unavailable, ok(T + 60_100), TIMING);
		expect(next).toMatchObject({ state: 'RECOVERING', successStreakSinceMs: T + 60_100 });
	});
});

describe('restoreHealth at lease acquisition', () => {
	const A = T + 1_000_000;

	it('HEALTHY and DEGRADED restore as DEGRADED with a fresh clock', () => {
		for (const state of ['HEALTHY', 'DEGRADED'] as const) {
			const { health: restored } = restoreHealth(health({ state, stateSinceMs: T + 980_000 }), A);
			expect(restored).toMatchObject({ state: 'DEGRADED', stateSinceMs: A });
		}
	});

	it('RECOVERING restores as UNAVAILABLE and loses its streak', () => {
		const { health: restored } = restoreHealth(
			health({ state: 'RECOVERING', stateSinceMs: T + 900_000, successStreakSinceMs: T + 900_000 }),
			A,
		);
		expect(restored).toMatchObject({
			state: 'UNAVAILABLE',
			stateSinceMs: A,
			successStreakSinceMs: null,
		});
	});

	it('UNAVAILABLE stays UNAVAILABLE with its stored state_since_ms', () => {
		const stored = health({
			state: 'UNAVAILABLE',
			stateSinceMs: T + 700_000,
			consecutiveFailures: 9,
		});
		expect(restoreHealth(stored, A)).toEqual({ health: stored, pauseExpired: false });
	});

	it('a future pause survives exactly; an expired one is cleared and allows a check', () => {
		const future = health({ state: 'UNAVAILABLE', pausedUntilMs: A + 3_600_000 });
		expect(restoreHealth(future, A)).toEqual({ health: future, pauseExpired: false });

		const expired = restoreHealth(health({ state: 'UNAVAILABLE', pausedUntilMs: A }), A);
		expect(expired.pauseExpired).toBe(true);
		expect(expired.health).toMatchObject({ state: 'UNAVAILABLE', pausedUntilMs: null });
	});

	it('unknown stays unknown', () => {
		expect(restoreHealth(null, A)).toEqual({ health: null, pauseExpired: false });
	});
});

describe('Redis hash fields', () => {
	it('uses exactly the ADR-022 field names, with OpenSky-only fields for OpenSky', () => {
		const h = health({ lastError: 'http_400', creditsRemaining: 12, lastProbeMs: T + 1 });
		const adsbfi = toHashFields('adsbfi', h);
		expect(adsbfi.filter((_, i) => i % 2 === 0)).toEqual([
			'state',
			'state_since_ms',
			'last_success_ms',
			'last_failure_ms',
			'consecutive_failures',
			'last_error',
			'success_streak_since_ms',
		]);
		const opensky = toHashFields('opensky', h);
		expect(opensky.filter((_, i) => i % 2 === 0).slice(7)).toEqual([
			'paused_until_ms',
			'credits_remaining',
			'last_probe_ms',
		]);
	});

	it('round-trips through the hash, with null stored as an empty string', () => {
		const h = health({
			state: 'RECOVERING',
			successStreakSinceMs: T,
			lastError: 'validation: adsb.fi response is not a JSON object',
			pausedUntilMs: null,
			creditsRemaining: 399,
			lastProbeMs: T + 2,
		});
		const fields = toHashFields('opensky', h);
		const record: Record<string, string> = {};
		for (let i = 0; i < fields.length; i += 2) record[fields[i]!] = fields[i + 1]!;
		expect(record['paused_until_ms']).toBe('');
		expect(parseHealthHash(record)).toEqual({ health: h, problem: null });
	});

	it('treats a missing or unusable record as unknown health', () => {
		expect(parseHealthHash({})).toEqual({ health: null, problem: null });
		expect(parseHealthHash({ state: 'SIDEWAYS', state_since_ms: '1' }).health).toBeNull();
		expect(parseHealthHash({ state: 'HEALTHY', state_since_ms: 'x' }).problem).not.toBeNull();
	});
});

describe('classifyRequestError: last_error from the shapes observed live', () => {
	it('classifies timeouts, network errors and anything else', () => {
		const timeout = Object.assign(new Error('The operation was aborted due to timeout'), {
			name: 'TimeoutError',
		});
		expect(classifyRequestError(timeout)).toBe('timeout');
		const dns = Object.assign(new TypeError('fetch failed'), { cause: { code: 'ENOTFOUND' } });
		expect(classifyRequestError(dns)).toBe('network:ENOTFOUND');
		expect(classifyRequestError(new TypeError('fetch failed'))).toBe('network');
		expect(classifyRequestError(new Error('boom'))).toBe('error: boom');
	});
});

describe('nextOpenskyCheckDelayMs: ADR-022 check rates', () => {
	const CADENCE = {
		healthyMs: 900_000,
		degradedMs: 30_000,
		recoveringMs: 25_000,
		backoffBaseMs: 60_000,
		backoffMaxMs: 900_000,
	};

	it('checks unknown health at once, and each state at its own rate', () => {
		expect(nextOpenskyCheckDelayMs(null, T, 0, CADENCE)).toBe(0);
		expect(nextOpenskyCheckDelayMs(health({}), T, 0, CADENCE)).toBe(900_000);
		expect(nextOpenskyCheckDelayMs(health({ state: 'DEGRADED' }), T, 0, CADENCE)).toBe(30_000);
		expect(nextOpenskyCheckDelayMs(health({ state: 'RECOVERING' }), T, 0, CADENCE)).toBe(25_000);
	});

	it('backs off from 60 s, doubling to 15 min, while UNAVAILABLE and not paused', () => {
		const unavailable = health({ state: 'UNAVAILABLE' });
		const delays = [1, 2, 3, 4, 5, 6].map((step) =>
			nextOpenskyCheckDelayMs(unavailable, T, step, CADENCE),
		);
		expect(delays).toEqual([60_000, 120_000, 240_000, 480_000, 900_000, 900_000]);
	});

	it('waits out an active pause, then checks at once', () => {
		const paused = health({ state: 'UNAVAILABLE', pausedUntilMs: T + 31_952_000 });
		expect(nextOpenskyCheckDelayMs(paused, T, 3, CADENCE)).toBe(31_952_000);
	});
});
