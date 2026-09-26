// Selection rounds while authority is none (ADR-022 section 4). Pure: the
// coordinator supplies health, staleness and the clock, and runs the plan.
//
// A round tries eligible providers one after another and stops at the first
// commit. HEALTHY providers go first, adsb.fi before OpenSky; then the other
// eligible providers, adsb.fi first. Unknown, stale and emergency
// (UNAVAILABLE, not paused) providers get one immediate attempt per entry
// into none. That limit is about attempts made outside a provider's own
// rate: its later requests, at its own rate, are still candidates.

import type { ProviderHealth, Provider } from './providerHealth.js';

export type { Provider };

export interface PlannedAttempt {
	provider: Provider;
	tier: 1 | 2;
	// Uses the provider's one immediate attempt for this entry into none.
	oneShot: boolean;
}

// The newest request of any outcome, or null when none was ever made.
export function lastRequestMs(h: ProviderHealth): number | null {
	const times = [h.lastSuccessMs, h.lastFailureMs, h.lastProbeMs].filter(
		(t): t is number => t !== null,
	);
	return times.length === 0 ? null : Math.max(...times);
}

// Stale: no request for longer than the provider's current check interval.
// Derived, never stored. Unknown health is not stale; it is its own case.
export function isStale(h: ProviderHealth | null, nowMs: number, intervalMs: number): boolean {
	if (h === null) return false;
	const last = lastRequestMs(h);
	return last === null || nowMs - last > intervalMs;
}

type Eligibility = 'tier1' | 'tier2' | 'one_shot' | 'ineligible';

function eligibility(h: ProviderHealth | null, stale: boolean, nowMs: number): Eligibility {
	if (h === null) return 'one_shot';
	if (h.pausedUntilMs !== null && h.pausedUntilMs > nowMs) return 'ineligible';
	if (stale) return 'one_shot';
	switch (h.state) {
		case 'HEALTHY':
			return 'tier1';
		case 'DEGRADED':
		case 'RECOVERING':
			return 'tier2';
		case 'UNAVAILABLE':
			return 'one_shot';
	}
}

const ORDER: readonly Provider[] = ['adsbfi', 'opensky'];

export function planSelectionRound(
	health: Record<Provider, ProviderHealth | null>,
	stale: Record<Provider, boolean>,
	nowMs: number,
	oneShotUsed: ReadonlySet<Provider>,
): PlannedAttempt[] {
	const kinds = ORDER.map((p) => [p, eligibility(health[p], stale[p], nowMs)] as const);
	const tier1 = kinds.filter(([, k]) => k === 'tier1').map(([p]) => p);
	const tier2 = kinds.filter(
		([p, k]) => k === 'tier2' || (k === 'one_shot' && !oneShotUsed.has(p)),
	);
	return [
		...tier1.map((provider) => ({ provider, tier: 1 as const, oneShot: false })),
		...tier2.map(([provider, k]) => ({ provider, tier: 2 as const, oneShot: k === 'one_shot' })),
	];
}

// Bounded full-jitter backoff for a provider request. The normal cadence is
// the floor, so a failure never causes requests to run faster than healthy
// polling.
export function nextProviderDelayMs(
	consecutiveFailures: number,
	intervalMs: number,
	baseMs: number,
	maxMs: number,
	random: () => number = Math.random,
): number {
	if (consecutiveFailures <= 0) return intervalMs;
	const ceiling = Math.min(maxMs, baseMs * 2 ** (consecutiveFailures - 1));
	return Math.max(intervalMs, Math.floor(random() * ceiling));
}

// Backoff after a candidate delivery failure while authority is none (a
// publish failure or a commit refused for its time). It is per provider and
// separate from provider health because the upstream response was valid.
export function nextSelectionRetryMs(step: number, baseMs: number, maxMs: number): number {
	return Math.min(maxMs, baseMs * 2 ** (Math.max(1, step) - 1));
}
