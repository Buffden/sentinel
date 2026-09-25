// Unit tests for selection rounds while authority is none (ADR-022 section 4).
// Pure: no Redis, no timers.
import { describe, expect, it } from 'vitest';
import type { ProviderHealth } from './providerHealth.js';
import {
	isStale,
	nextSelectionRetryMs,
	planSelectionRound,
	type Provider,
} from './providerSelection.js';

const T = 1_790_400_000_000;

function h(overrides: Partial<ProviderHealth>): ProviderHealth {
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

const fresh = { adsbfi: false, opensky: false };
const none = new Set<Provider>();
const order = (plan: { provider: Provider }[]) => plan.map((p) => p.provider);

describe('isStale', () => {
	it('is stale only after longer than the interval without any request', () => {
		expect(isStale(h({ lastSuccessMs: T }), T + 10_000, 10_000)).toBe(false);
		expect(isStale(h({ lastSuccessMs: T }), T + 10_001, 10_000)).toBe(true);
		// The newest of success, failure and probe counts as the last request.
		expect(isStale(h({ lastSuccessMs: T, lastFailureMs: T + 5_000 }), T + 12_000, 10_000)).toBe(
			false,
		);
		expect(isStale(h({ lastSuccessMs: null, lastProbeMs: T + 9_000 }), T + 12_000, 10_000)).toBe(
			false,
		);
	});

	it('unknown health is not "stale": it is its own case', () => {
		expect(isStale(null, T, 10_000)).toBe(false);
	});
});

describe('planSelectionRound', () => {
	it('tries HEALTHY providers first, adsb.fi before OpenSky', () => {
		const plan = planSelectionRound(
			{ adsbfi: h({ state: 'HEALTHY' }), opensky: h({ state: 'HEALTHY' }) },
			fresh,
			T,
			none,
		);
		expect(plan).toEqual([
			{ provider: 'adsbfi', tier: 1, oneShot: false },
			{ provider: 'opensky', tier: 1, oneShot: false },
		]);
	});

	it('a HEALTHY OpenSky goes before a degraded or recovering adsb.fi', () => {
		for (const state of ['DEGRADED', 'RECOVERING'] as const) {
			const plan = planSelectionRound(
				{ adsbfi: h({ state }), opensky: h({ state: 'HEALTHY' }) },
				fresh,
				T,
				none,
			);
			expect(order(plan)).toEqual(['opensky', 'adsbfi']);
			expect(plan[1]).toEqual({ provider: 'adsbfi', tier: 2, oneShot: false });
		}
	});

	it('unknown, stale and emergency UNAVAILABLE are one-shot tier-2 attempts', () => {
		const plan = planSelectionRound(
			{ adsbfi: h({ state: 'UNAVAILABLE' }), opensky: null },
			fresh,
			T,
			none,
		);
		expect(plan).toEqual([
			{ provider: 'adsbfi', tier: 2, oneShot: true },
			{ provider: 'opensky', tier: 2, oneShot: true },
		]);
	});

	it('a stale HEALTHY provider is a one-shot tier-2 attempt, not tier 1', () => {
		const plan = planSelectionRound(
			{ adsbfi: h({ state: 'HEALTHY' }), opensky: h({ state: 'RECOVERING' }) },
			{ adsbfi: true, opensky: false },
			T,
			none,
		);
		expect(plan).toEqual([
			{ provider: 'adsbfi', tier: 2, oneShot: true },
			{ provider: 'opensky', tier: 2, oneShot: false },
		]);
	});

	it('a used one-shot is not offered again in the same entry into none', () => {
		const plan = planSelectionRound(
			{ adsbfi: h({ state: 'UNAVAILABLE' }), opensky: null },
			fresh,
			T,
			new Set<Provider>(['adsbfi', 'opensky']),
		);
		expect(plan).toEqual([]);
	});

	it('a used one-shot does not exclude a provider whose health has since made it eligible', () => {
		const plan = planSelectionRound(
			{ adsbfi: h({ state: 'RECOVERING' }), opensky: null },
			fresh,
			T,
			new Set<Provider>(['adsbfi']),
		);
		expect(plan).toEqual([
			{ provider: 'adsbfi', tier: 2, oneShot: false },
			{ provider: 'opensky', tier: 2, oneShot: true },
		]);
	});

	it('a paused OpenSky is not eligible until its pause ends', () => {
		const paused = h({ state: 'UNAVAILABLE', pausedUntilMs: T + 1_000 });
		expect(planSelectionRound({ adsbfi: null, opensky: paused }, fresh, T, none)).toEqual([
			{ provider: 'adsbfi', tier: 2, oneShot: true },
		]);
		expect(
			order(planSelectionRound({ adsbfi: null, opensky: paused }, fresh, T + 1_000, none)),
		).toEqual(['adsbfi', 'opensky']);
	});

	it('both unavailable and both one-shots used: nothing to try', () => {
		const both = { adsbfi: h({ state: 'UNAVAILABLE' }), opensky: h({ state: 'UNAVAILABLE' }) };
		expect(planSelectionRound(both, fresh, T, new Set<Provider>(['adsbfi', 'opensky']))).toEqual(
			[],
		);
	});
});

describe('nextSelectionRetryMs', () => {
	it('starts at 60 s and doubles to a 15 min cap', () => {
		expect([1, 2, 3, 4, 5, 6].map((step) => nextSelectionRetryMs(step, 60_000, 900_000))).toEqual([
			60_000, 120_000, 240_000, 480_000, 900_000, 900_000,
		]);
	});
});
