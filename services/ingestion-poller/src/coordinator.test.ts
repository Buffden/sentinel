import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { AdsbfiFetchFailure, SplitResult } from './adsbfiPoller.js';
import { Coordinator, type CoordinatorDeps } from './coordinator.js';
import type { CloseResult, CoverageCloseReason, CreditResult } from './coverageTimeline.js';
import type { OpenskyCheckResult } from './poller.js';
import type { Provider, ProviderHealth } from './providerHealth.js';
import type {
	AcquisitionSnapshot,
	HealthWriteResult,
	StoredHealth,
} from './providerHealthStore.js';

// The lease scripts themselves are tested against real Redis in
// coordinatorLease.integration.test.ts. Here the lease is a controllable
// stand-in, so the coordinator's own decisions (when to poll, publish, stop,
// release) can be driven step by step with fake timers.
class FakeLease {
	token: string | null = null;
	acquireResult = true;
	renewResult: boolean | Error = true;
	acquisitions = 0;
	releases = 0;
	private next = 0;

	async tryAcquire(): Promise<boolean> {
		if (!this.acquireResult) return false;
		this.acquisitions++;
		this.token = `tok-${++this.next}`;
		return true;
	}
	async renew(): Promise<boolean> {
		if (this.token === null) return false;
		if (this.renewResult instanceof Error) throw this.renewResult;
		return this.renewResult;
	}
	forget(): void {
		this.token = null;
	}
	async release(): Promise<boolean> {
		this.releases++;
		const held = this.token !== null;
		this.token = null;
		return held;
	}
	async currentHolder(): Promise<string | null> {
		return 'tok-other';
	}
}

// The timeline scripts are tested against real Redis in
// coverageTimeline.integration.test.ts. This stand-in records what the
// coordinator asks for, and returns whatever the test sets.
class FakeTimeline {
	credits: number[] = [];
	closes: CoverageCloseReason[] = [];
	creditResult: CreditResult | Error = { status: 'extended', timelineVersion: 1 };
	closeResult: CloseResult | Error = { status: 'already_closed' };

	async credit(_token: string, activeSuccessMs: number): Promise<CreditResult> {
		events.push('credit');
		this.credits.push(activeSuccessMs);
		if (this.creditResult instanceof Error) throw this.creditResult;
		return this.creditResult;
	}
	async close(_token: string, reason: CoverageCloseReason): Promise<CloseResult> {
		events.push(`close:${reason}`);
		this.closes.push(reason);
		if (this.closeResult instanceof Error) throw this.closeResult;
		return this.closeResult;
	}
}

// The health scripts are tested against real Redis in
// providerHealthStore.integration.test.ts. This stand-in keeps what was
// "stored", records every write, and fails when told to.
const UNKNOWN: StoredHealth = { health: null, problem: null, present: false };

class FakeHealthStore {
	snapshot: AcquisitionSnapshot = {
		authorityInitialized: true,
		authorityProvider: 'adsbfi',
		stored: { adsbfi: UNKNOWN, opensky: UNKNOWN },
	};
	writes: { provider: Provider; health: ProviderHealth }[] = [];
	readResult: Error | null = null;
	writeResult: HealthWriteResult | Error = 'written';

	async readForAcquisition(): Promise<AcquisitionSnapshot> {
		events.push('health:read');
		if (this.readResult) throw this.readResult;
		return this.snapshot;
	}
	async write(
		_token: string,
		provider: Provider,
		health: ProviderHealth,
	): Promise<HealthWriteResult> {
		events.push(`health:${provider}:${health.state}`);
		if (this.writeResult instanceof Error) throw this.writeResult;
		if (this.writeResult === 'written') this.writes.push({ provider, health });
		return this.writeResult;
	}
	last(provider: Provider): ProviderHealth | undefined {
		return this.writes.filter((w) => w.provider === provider).at(-1)?.health;
	}
}

const DEGRADED_MS = 60_000;
const RECOVERY_MS = 120_000;
const RENEWAL_MS = 5_000;
const FOLLOWER_RETRY_MS = 5_000;
const POLL_MS = 2_000;
const FROZEN_MS = 10_000;

// adsb.fi `now` for the next response. The default fetch advances it by one
// second per cycle, so freshness is confirmed from the second cycle on.
let providerNow: number;

function split(count = 1, responseNowMs = (providerNow += 1_000)): SplitResult {
	return {
		messages: Array.from({ length: count }, (_, i) => ({ key: `ac${i}`, value: '{}' })),
		responseNowMs,
		total: count,
		skippedNonIcao: 0,
		skippedNoPosition: 0,
		skippedOutsideBox: 0,
	};
}

let lease: FakeLease;
let timeline: FakeTimeline;
let healthStore: FakeHealthStore;
let events: string[];
let fetchCycle: ReturnType<typeof vi.fn<() => Promise<SplitResult | AdsbfiFetchFailure>>>;
let publish: ReturnType<typeof vi.fn<CoordinatorDeps['publish']>>;
let coordinator: Coordinator;

beforeEach(() => {
	vi.useFakeTimers();
	lease = new FakeLease();
	timeline = new FakeTimeline();
	healthStore = new FakeHealthStore();
	events = [];
	providerNow = 1790283486000;
	fetchCycle = vi.fn(async () => {
		events.push('fetch');
		return split();
	});
	publish = vi.fn(async () => {
		events.push('publish');
		return '0';
	});
	const origRelease = lease.release.bind(lease);
	lease.release = async () => {
		events.push('release');
		return origRelease();
	};
	coordinator = build();
});

// Builds a coordinator over the shared fakes. OpenSky checks always run in
// production; here they succeed at once unless a test passes its own.
function build(overrides: Partial<CoordinatorDeps> = {}): Coordinator {
	return new Coordinator({
		lease,
		timeline,
		health: healthStore,
		fetchCycle,
		checkOpensky: async () => ({ kind: 'ok', creditsRemaining: null }),
		healthTiming: { degradedTimeoutMs: DEGRADED_MS, recoveryWindowMs: RECOVERY_MS },
		openskyCadence: {
			healthyMs: 900_000,
			degradedMs: 30_000,
			recoveringMs: 25_000,
			backoffBaseMs: 60_000,
			backoffMaxMs: 900_000,
		},
		publish,
		log: (_level, message) => events.push(message),
		renewalIntervalMs: RENEWAL_MS,
		followerRetryMs: FOLLOWER_RETRY_MS,
		pollIntervalMs: POLL_MS,
		backoffBaseMs: POLL_MS,
		backoffMaxMs: 60_000,
		frozenFeedMs: FROZEN_MS,
		...overrides,
	});
}

afterEach(() => {
	vi.useRealTimers();
});

describe('Coordinator', () => {
	it('polls and publishes while it holds the lease', async () => {
		coordinator.start();
		await vi.advanceTimersByTimeAsync(POLL_MS * 2 + 10);

		expect(coordinator.isLeader).toBe(true);
		expect(publish).toHaveBeenCalledTimes(3); // immediately, then every 2 s
	});

	it('never fetches or publishes while it is a follower', async () => {
		lease.acquireResult = false;
		coordinator.start();
		await vi.advanceTimersByTimeAsync(60_000);

		expect(coordinator.isLeader).toBe(false);
		expect(fetchCycle).not.toHaveBeenCalled();
		expect(publish).not.toHaveBeenCalled();
		// Waiting is logged once per follower period, not on every retry.
		expect(events.filter((e) => e.includes('waiting as follower'))).toHaveLength(1);
	});

	it('takes over when the lease becomes free, at the next follower retry', async () => {
		lease.acquireResult = false;
		coordinator.start();
		await vi.advanceTimersByTimeAsync(12_000);
		expect(publish).not.toHaveBeenCalled();

		lease.acquireResult = true;
		await vi.advanceTimersByTimeAsync(FOLLOWER_RETRY_MS);
		expect(coordinator.isLeader).toBe(true);
		expect(publish).toHaveBeenCalled();
	});

	it.each([
		['renewal returns 0 (token no longer matches)', false],
		['renewal errors or times out', new Error('Command timed out')],
	])('fails closed when %s', async (_label, renewResult) => {
		coordinator.start();
		await vi.advanceTimersByTimeAsync(10);
		expect(coordinator.isLeader).toBe(true);

		lease.renewResult = renewResult;
		lease.acquireResult = false; // the successor now holds it
		await vi.advanceTimersByTimeAsync(RENEWAL_MS);
		const publishedAtLoss = publish.mock.calls.length;
		const fetchedAtLoss = fetchCycle.mock.calls.length;

		await vi.advanceTimersByTimeAsync(60_000);
		expect(coordinator.isLeader).toBe(false);
		expect(publish).toHaveBeenCalledTimes(publishedAtLoss);
		expect(fetchCycle).toHaveBeenCalledTimes(fetchedAtLoss);
		// Never deletes the key: it may already belong to a successor.
		expect(lease.releases).toBe(0);
		expect(events).toContain('lease lost: stopped polling and publishing');
	});

	it('returns to follower mode after losing the lease and can lead again', async () => {
		coordinator.start();
		await vi.advanceTimersByTimeAsync(10);
		lease.renewResult = false;
		await vi.advanceTimersByTimeAsync(RENEWAL_MS);
		expect(coordinator.isLeader).toBe(false);

		lease.renewResult = true;
		await vi.advanceTimersByTimeAsync(FOLLOWER_RETRY_MS);
		expect(coordinator.isLeader).toBe(true);
		expect(lease.acquisitions).toBe(2);
	});

	it('discards a fetched cycle when the lease was lost during the fetch', async () => {
		let resolveFetch!: (value: SplitResult) => void;
		fetchCycle.mockImplementationOnce(
			() => new Promise<SplitResult>((resolve) => (resolveFetch = resolve)),
		);
		coordinator.start();
		await vi.advanceTimersByTimeAsync(10); // fetch now in flight

		lease.renewResult = false;
		await vi.advanceTimersByTimeAsync(RENEWAL_MS); // lease lost meanwhile
		resolveFetch(split(5));
		await vi.advanceTimersByTimeAsync(10);

		expect(publish).not.toHaveBeenCalled();
		expect(events).toContain('lease lost during cycle: fetched positions discarded, not published');
	});

	it('still waits for the new cycle at shutdown when an old cycle finishes after reacquisition', async () => {
		let resolveOld!: (value: SplitResult) => void;
		let resolveNew!: (value: SplitResult) => void;
		fetchCycle
			.mockImplementationOnce(() => new Promise<SplitResult>((resolve) => (resolveOld = resolve)))
			.mockImplementationOnce(() => new Promise<SplitResult>((resolve) => (resolveNew = resolve)));
		coordinator.start();
		await vi.advanceTimersByTimeAsync(10); // old cycle (tok-1) in flight

		lease.renewResult = false;
		await vi.advanceTimersByTimeAsync(RENEWAL_MS); // lease lost, old fetch still pending
		expect(coordinator.isLeader).toBe(false);

		lease.renewResult = true;
		await vi.advanceTimersByTimeAsync(FOLLOWER_RETRY_MS + 10); // reacquired, new cycle in flight
		expect(lease.token).toBe('tok-2');
		expect(fetchCycle).toHaveBeenCalledTimes(2);

		resolveOld(split(3)); // the old cycle finishes and discards its positions
		await vi.advanceTimersByTimeAsync(10);
		expect(events).toContain('lease lost during cycle: fetched positions discarded, not published');

		let shutdownDone = false;
		const done = coordinator.shutdown().then(() => (shutdownDone = true));
		await vi.advanceTimersByTimeAsync(10);
		// Shutdown must still be waiting on the new cycle, holding the lease.
		expect(shutdownDone).toBe(false);
		expect(lease.releases).toBe(0);

		resolveNew(split(2));
		await done;
		expect(publish).toHaveBeenCalledTimes(1); // only the new cycle published
		expect(events.indexOf('publish')).toBeLessThan(events.indexOf('release'));
	});

	it('shuts down cleanly: finishes the in-flight publish, then stops renewal, then releases', async () => {
		let resolveFetch!: (value: SplitResult) => void;
		fetchCycle.mockImplementationOnce(
			() => new Promise<SplitResult>((resolve) => (resolveFetch = resolve)),
		);
		coordinator.start();
		await vi.advanceTimersByTimeAsync(10); // cycle in flight

		const done = coordinator.shutdown();
		resolveFetch(split(2));
		await done;

		// The in-flight cycle still published, then coverage closed, and only
		// then was the lease released.
		expect(events.indexOf('publish')).toBeGreaterThanOrEqual(0);
		expect(events.indexOf('publish')).toBeLessThan(events.indexOf('close:coordinator_shutdown'));
		expect(events.indexOf('close:coordinator_shutdown')).toBeLessThan(events.indexOf('release'));
		expect(events).toContain('lease released');
		expect(lease.token).toBeNull();

		// Nothing new starts afterwards: no polls, renewals or acquisitions.
		const renew = vi.spyOn(lease, 'renew');
		await vi.advanceTimersByTimeAsync(60_000);
		expect(publish).toHaveBeenCalledTimes(1);
		expect(renew).not.toHaveBeenCalled();
		expect(lease.acquisitions).toBe(1);
	});

	it('does not release on shutdown when it is not the leader', async () => {
		lease.acquireResult = false;
		coordinator.start();
		await vi.advanceTimersByTimeAsync(10);
		await coordinator.shutdown();
		expect(lease.releases).toBe(0);
	});
});

describe('Coordinator coverage timeline', () => {
	// Cycles run at t = 0, 2, 4 ... s after acquisition (POLL_MS apart).
	async function runCycles(n: number): Promise<void> {
		await vi.advanceTimersByTimeAsync(10 + (n - 1) * POLL_MS);
	}

	it('closes stale coverage as coordinator_down on acquisition, before the first fetch', async () => {
		coordinator.start();
		await runCycles(1);
		expect(timeline.closes[0]).toBe('coordinator_down');
		expect(events.indexOf('close:coordinator_down')).toBeLessThan(events.indexOf('fetch'));
	});

	it.each([
		['the close errors', new Error('Command timed out')],
		['the close is refused by the lease check', { status: 'lease_mismatch' } as const],
	])('never polls under an acquisition when %s', async (_label, result) => {
		timeline.closeResult = result;
		coordinator.start();
		await vi.advanceTimersByTimeAsync(60_000);
		expect(coordinator.isLeader).toBe(false);
		expect(fetchCycle).not.toHaveBeenCalled();
		expect(publish).not.toHaveBeenCalled();
		expect(timeline.credits).toEqual([]);
	});

	it('seeds freshness on the first cycle: publishes it but does not credit it', async () => {
		coordinator.start();
		await runCycles(1);
		expect(publish).toHaveBeenCalledTimes(1);
		expect(timeline.credits).toEqual([]);
	});

	it('credits the next fresh cycle after publishing it, at the time the publish finished', async () => {
		let publishedAt = 0;
		publish.mockImplementation(async () => {
			events.push('publish');
			publishedAt = Date.now();
			return '0';
		});
		coordinator.start();
		await runCycles(2);
		expect(timeline.credits).toEqual([publishedAt]);
		expect(events.lastIndexOf('publish')).toBeLessThan(events.indexOf('credit'));
	});

	it('credits a fresh cycle with zero messages without publishing', async () => {
		fetchCycle.mockImplementation(async () => split(0));
		coordinator.start();
		await runCycles(2);
		expect(publish).not.toHaveBeenCalled();
		expect(timeline.credits).toHaveLength(1);
	});

	it('publishes repeated now values without crediting, and credits again once now advances', async () => {
		const stuck = providerNow + 1_000;
		fetchCycle.mockImplementation(async () => split(1, stuck));
		coordinator.start();
		await runCycles(4); // seed, then 3 repeats over 6 s: inside the 10 s window
		expect(publish).toHaveBeenCalledTimes(4);
		expect(timeline.credits).toEqual([]);
		expect(timeline.closes).toEqual(['coordinator_down']);

		fetchCycle.mockImplementation(async () => split(1, stuck + 1_000));
		await vi.advanceTimersByTimeAsync(POLL_MS);
		expect(timeline.credits).toHaveLength(1);
	});

	it('fails a cycle once now has not advanced for 10 s: closes as failure and does not publish it', async () => {
		const stuck = providerNow + 1_000;
		fetchCycle.mockImplementation(async () => split(1, stuck));
		coordinator.start();
		await runCycles(5); // t = 0 (seed) to 8 s: all published, none credited
		expect(publish).toHaveBeenCalledTimes(5);
		expect(timeline.closes).toEqual(['coordinator_down']);

		await vi.advanceTimersByTimeAsync(POLL_MS); // t = 10 s: frozen
		expect(timeline.closes).toEqual(['coordinator_down', 'failure']);
		expect(publish).toHaveBeenCalledTimes(5);
		expect(events).toContain('adsb.fi feed frozen: now has not advanced');

		// Every later frozen cycle calls the idempotent close again, and none
		// is ever credited, so the closed segment's end cannot move.
		await vi.advanceTimersByTimeAsync(120_000);
		expect(timeline.closes.filter((r) => r === 'failure').length).toBeGreaterThan(1);
		expect(timeline.credits).toEqual([]);
		expect(publish).toHaveBeenCalledTimes(5);
	});

	it('does not credit a cycle whose publish failed, and closes as failure', async () => {
		coordinator.start();
		await runCycles(1);
		publish.mockRejectedValueOnce(new Error('broker unavailable'));
		await vi.advanceTimersByTimeAsync(POLL_MS);
		expect(timeline.credits).toEqual([]);
		expect(timeline.closes).toEqual(['coordinator_down', 'failure']);
	});

	it('calls the failure close on every failed cycle', async () => {
		fetchCycle.mockImplementation(async () => {
			events.push('fetch');
			return { error: 'timeout' };
		});
		coordinator.start();
		await vi.advanceTimersByTimeAsync(60_000);
		const failures = timeline.closes.filter((r) => r === 'failure').length;
		expect(failures).toBe(fetchCycle.mock.calls.length);
		expect(failures).toBeGreaterThan(1);
	});

	it('neither credits nor closes when the lease was lost during the fetch', async () => {
		let resolveFetch!: (value: SplitResult) => void;
		coordinator.start();
		await runCycles(1);
		fetchCycle.mockImplementationOnce(
			() => new Promise<SplitResult>((resolve) => (resolveFetch = resolve)),
		);
		await vi.advanceTimersByTimeAsync(POLL_MS); // second fetch in flight
		lease.renewResult = false;
		lease.acquireResult = false;
		await vi.advanceTimersByTimeAsync(RENEWAL_MS); // lease lost meanwhile
		resolveFetch(split(3));
		await vi.advanceTimersByTimeAsync(10);

		expect(timeline.credits).toEqual([]);
		expect(timeline.closes).toEqual(['coordinator_down']);
	});

	it.each([
		['the credit errors', new Error('Command timed out')],
		['the credit is refused by the lease check', { status: 'lease_mismatch' } as const],
	])('fails closed when %s', async (_label, result) => {
		timeline.creditResult = result;
		coordinator.start();
		await runCycles(1); // seed cycle: no credit yet
		lease.acquireResult = false; // after the loss, a successor holds it
		await vi.advanceTimersByTimeAsync(POLL_MS);
		expect(timeline.credits).toHaveLength(1);
		expect(coordinator.isLeader).toBe(false);

		const publishedAtLoss = publish.mock.calls.length;
		await vi.advanceTimersByTimeAsync(60_000);
		expect(publish).toHaveBeenCalledTimes(publishedAtLoss);
		expect(lease.releases).toBe(0);
	});

	it('treats a zero-length close as a normal close: logged, lease kept', async () => {
		timeline.closeResult = { status: 'closed_empty', timelineVersion: 7 };
		coordinator.start();
		await runCycles(3);
		expect(coordinator.isLeader).toBe(true);
		expect(events).toContain('coverage closed with no length: no segment written');
		expect(publish).toHaveBeenCalledTimes(3);
	});

	it('fails closed when the failure close errors', async () => {
		coordinator.start();
		await runCycles(1);
		timeline.closeResult = new Error('Command timed out');
		lease.acquireResult = false;
		fetchCycle.mockImplementation(async () => ({ error: 'timeout' }));
		await vi.advanceTimersByTimeAsync(POLL_MS);
		expect(coordinator.isLeader).toBe(false);
		const fetchesAtLoss = fetchCycle.mock.calls.length;
		await vi.advanceTimersByTimeAsync(60_000);
		expect(fetchCycle).toHaveBeenCalledTimes(fetchesAtLoss);
	});

	it('resets freshness on reacquisition: the first cycle of the new acquisition is not credited', async () => {
		coordinator.start();
		await runCycles(2);
		expect(timeline.credits.length).toBeGreaterThan(0);

		lease.renewResult = false;
		while (coordinator.isLeader) await vi.advanceTimersByTimeAsync(100); // lost
		lease.renewResult = true;
		const creditsBeforeLoss = timeline.credits.length;
		const publishesBeforeLoss = publish.mock.calls.length;

		// Step until the new acquisition's first cycle has published.
		while (publish.mock.calls.length === publishesBeforeLoss) {
			await vi.advanceTimersByTimeAsync(10);
		}
		expect(lease.token).toBe('tok-2');
		expect(timeline.closes.filter((r) => r === 'coordinator_down')).toHaveLength(2);
		// That cycle's now is newer than anything the old acquisition saw, but
		// the tracker was reset, so it only seeds.
		expect(timeline.credits).toHaveLength(creditsBeforeLoss);

		await vi.advanceTimersByTimeAsync(POLL_MS);
		expect(timeline.credits).toHaveLength(creditsBeforeLoss + 1);
	});

	it('closes coverage on shutdown while the lease is still being renewed', async () => {
		coordinator.start();
		await runCycles(2);
		let finishClose!: () => void;
		timeline.close = async (_token, reason) => {
			events.push(`close:${reason}`);
			await new Promise<void>((resolve) => (finishClose = resolve));
			return { status: 'already_closed' };
		};
		const renew = vi.spyOn(lease, 'renew');
		const done = coordinator.shutdown();
		await vi.advanceTimersByTimeAsync(RENEWAL_MS + 10);
		// The close is still pending and renewal has kept the lease alive.
		expect(events).toContain('close:coordinator_shutdown');
		expect(renew).toHaveBeenCalled();
		expect(lease.releases).toBe(0);

		finishClose();
		await done;
		expect(lease.releases).toBe(1);
	});
});

describe('Coordinator provider health (ADR-022 section 2, CP3d)', () => {
	const hang = () => new Promise<never>(() => {});
	const stored = (health: Partial<ProviderHealth>): StoredHealth => ({
		health: {
			state: 'HEALTHY',
			stateSinceMs: 1,
			lastSuccessMs: 1,
			lastFailureMs: null,
			consecutiveFailures: 0,
			lastError: null,
			successStreakSinceMs: null,
			pausedUntilMs: null,
			creditsRemaining: null,
			lastProbeMs: null,
			...health,
		},
		problem: null,
		present: true,
	});
	const adsbfiWrites = () => healthStore.writes.filter((w) => w.provider === 'adsbfi');

	it('restores health before the first request, and records it before publishing', async () => {
		coordinator.start();
		await vi.advanceTimersByTimeAsync(10);
		expect(events.indexOf('health:read')).toBeLessThan(events.indexOf('fetch'));
		expect(events.indexOf('health:adsbfi:RECOVERING')).toBeLessThan(events.indexOf('publish'));
	});

	it('existing authority with no health record: the first success is RECOVERING, not HEALTHY', async () => {
		coordinator.start();
		await vi.advanceTimersByTimeAsync(10);
		expect(healthStore.last('adsbfi')?.state).toBe('RECOVERING');
		await vi.advanceTimersByTimeAsync(RECOVERY_MS + POLL_MS);
		expect(healthStore.last('adsbfi')?.state).toBe('HEALTHY');
	});

	it('true first deployment: HEALTHY on the first valid response, even if the publish then fails', async () => {
		healthStore.snapshot = {
			authorityInitialized: false,
			authorityProvider: null,
			stored: { adsbfi: UNKNOWN, opensky: UNKNOWN },
		};
		publish.mockRejectedValueOnce(new Error('broker unavailable'));
		coordinator.start();
		await vi.advanceTimersByTimeAsync(10);
		expect(healthStore.last('adsbfi')).toMatchObject({
			state: 'HEALTHY',
			consecutiveFailures: 0,
			lastFailureMs: null,
		});
		// Authority was not bootstrapped: the cycle failed at publish.
		expect(timeline.credits).toEqual([]);
		expect(timeline.closes).toContain('failure');
	});

	it('a Kafka failure after a provider success adds no health failure', async () => {
		healthStore.snapshot.stored.adsbfi = stored({ state: 'HEALTHY' });
		coordinator.start();
		await vi.advanceTimersByTimeAsync(10 + POLL_MS); // restored DEGRADED, then HEALTHY
		const before = healthStore.last('adsbfi')!;
		expect(before.state).toBe('HEALTHY');
		publish.mockRejectedValueOnce(new Error('broker unavailable'));
		await vi.advanceTimersByTimeAsync(POLL_MS);
		expect(timeline.closes).toContain('failure');
		const after = healthStore.last('adsbfi')!;
		expect(after).toMatchObject({ state: 'HEALTHY', consecutiveFailures: 0, lastFailureMs: null });
		expect(adsbfiWrites().some((w) => w.health.consecutiveFailures > 0)).toBe(false);
	});

	it('seeded and unconfirmed responses are health successes; a frozen feed is a failure', async () => {
		fetchCycle.mockImplementation(async () => {
			events.push('fetch');
			return split(1, 1790283486000); // `now` never advances
		});
		coordinator.start();
		await vi.advanceTimersByTimeAsync(FROZEN_MS - 1_000);
		expect(adsbfiWrites().every((w) => w.health.lastFailureMs === null)).toBe(true);
		expect(timeline.credits).toEqual([]); // CP3b: never credited
		await vi.advanceTimersByTimeAsync(4_000);
		expect(healthStore.last('adsbfi')).toMatchObject({ lastError: 'frozen_feed' });
		expect(timeline.closes).toContain('failure');
	});

	it('a zero-aircraft response is a success', async () => {
		fetchCycle.mockImplementation(async () => split(0));
		coordinator.start();
		await vi.advanceTimersByTimeAsync(10);
		expect(healthStore.last('adsbfi')).toMatchObject({ state: 'RECOVERING', lastFailureMs: null });
	});

	it('records the adapter failure class as last_error', async () => {
		healthStore.snapshot.stored.adsbfi = stored({ state: 'HEALTHY' });
		fetchCycle.mockImplementation(async () => ({ error: 'http_503' }));
		coordinator.start();
		await vi.advanceTimersByTimeAsync(10);
		expect(healthStore.last('adsbfi')).toMatchObject({
			state: 'DEGRADED',
			lastError: 'http_503',
			consecutiveFailures: 1,
		});
	});

	it('the deadline timer makes DEGRADED UNAVAILABLE at the logical deadline with no request finishing', async () => {
		healthStore.snapshot.stored.adsbfi = stored({ state: 'HEALTHY' });
		fetchCycle.mockImplementation(hang);
		coordinator.start();
		await vi.advanceTimersByTimeAsync(10);
		const restored = healthStore.last('adsbfi')!;
		expect(restored.state).toBe('DEGRADED');
		await vi.advanceTimersByTimeAsync(DEGRADED_MS);
		expect(healthStore.last('adsbfi')).toMatchObject({
			state: 'UNAVAILABLE',
			stateSinceMs: restored.stateSinceMs + DEGRADED_MS,
		});
		expect(fetchCycle).toHaveBeenCalledTimes(1); // still the one hanging request
	});

	it('repeated failures do not move the DEGRADED clock', async () => {
		healthStore.snapshot.stored.adsbfi = stored({ state: 'HEALTHY' });
		fetchCycle.mockImplementation(async () => ({ error: 'timeout' }));
		coordinator.start();
		await vi.advanceTimersByTimeAsync(10);
		const since = healthStore.last('adsbfi')!.stateSinceMs;
		await vi.advanceTimersByTimeAsync(DEGRADED_MS - 1_000);
		const degraded = adsbfiWrites().filter((w) => w.health.state === 'DEGRADED');
		expect(degraded.length).toBeGreaterThan(2);
		expect(degraded.every((w) => w.health.stateSinceMs === since)).toBe(true);
		await vi.advanceTimersByTimeAsync(1_000);
		expect(healthStore.last('adsbfi')).toMatchObject({
			state: 'UNAVAILABLE',
			stateSinceMs: since + DEGRADED_MS,
		});
	});

	it('a deadline callback queued behind a success cannot overwrite HEALTHY', async () => {
		healthStore.snapshot.stored.adsbfi = stored({ state: 'HEALTHY' });
		let resolveFetch!: (v: SplitResult) => void;
		fetchCycle.mockImplementation(
			() => new Promise<SplitResult>((resolve) => (resolveFetch = resolve)),
		);
		coordinator.start();
		await vi.advanceTimersByTimeAsync(10);
		const since = healthStore.last('adsbfi')!.stateSinceMs;
		// The success write stalls across the deadline, so the timer fires and
		// queues behind it.
		let releaseWrite!: () => void;
		const realWrite = healthStore.write.bind(healthStore);
		healthStore.write = async (token, provider, health) => {
			if (health.state === 'HEALTHY') await new Promise<void>((r) => (releaseWrite = r));
			return realWrite(token, provider, health);
		};
		await vi.advanceTimersByTimeAsync(DEGRADED_MS - 100 - 10);
		resolveFetch(split());
		await vi.advanceTimersByTimeAsync(500); // past the deadline, success still writing
		releaseWrite();
		await vi.advanceTimersByTimeAsync(10);
		expect(healthStore.last('adsbfi')).toMatchObject({ state: 'HEALTHY' });
		expect(adsbfiWrites().some((w) => w.health.state === 'UNAVAILABLE')).toBe(false);
		expect(healthStore.last('adsbfi')!.stateSinceMs).toBeGreaterThan(since);
	});

	it('if the deadline wins first, a later success goes UNAVAILABLE to RECOVERING', async () => {
		healthStore.snapshot.stored.adsbfi = stored({ state: 'HEALTHY' });
		let resolveFetch!: (v: SplitResult) => void;
		fetchCycle.mockImplementation(
			() => new Promise<SplitResult>((resolve) => (resolveFetch = resolve)),
		);
		coordinator.start();
		await vi.advanceTimersByTimeAsync(10 + DEGRADED_MS);
		expect(healthStore.last('adsbfi')?.state).toBe('UNAVAILABLE');
		resolveFetch(split());
		await vi.advanceTimersByTimeAsync(10);
		expect(healthStore.last('adsbfi')?.state).toBe('RECOVERING');
	});

	it('an UNAVAILABLE adsb.fi stays authoritative: it keeps polling, publishing and crediting', async () => {
		healthStore.snapshot.stored.adsbfi = stored({ state: 'UNAVAILABLE' });
		coordinator.start();
		await vi.advanceTimersByTimeAsync(10 + POLL_MS * 3);
		expect(publish).toHaveBeenCalledTimes(4);
		expect(timeline.credits.length).toBe(3); // the first cycle only seeds freshness
		expect(timeline.closes).toEqual(['coordinator_down']);
		expect(healthStore.last('adsbfi')?.state).toBe('RECOVERING');
	});

	it.each([
		['a refused health write', 'lease_mismatch' as const],
		['a health write that times out', new Error('Command timed out')],
	])('%s fails closed: no more requests or checks', async (_label, writeResult) => {
		const checkOpensky = vi.fn(async (): Promise<OpenskyCheckResult> => ({
			kind: 'ok',
			creditsRemaining: 399,
		}));
		coordinator = build({ checkOpensky });
		lease.acquireResult = false; // stays a follower after losing the lease
		healthStore.writeResult = writeResult;
		coordinator.start();
		await vi.advanceTimersByTimeAsync(10);
		expect(coordinator.isLeader).toBe(false);
		const fetches = fetchCycle.mock.calls.length;
		const checks = checkOpensky.mock.calls.length;
		await vi.advanceTimersByTimeAsync(10 * 60_000);
		expect(fetchCycle).toHaveBeenCalledTimes(fetches);
		expect(checkOpensky).toHaveBeenCalledTimes(checks);
		expect(publish).not.toHaveBeenCalled();
	});

	it('a health read error at acquisition fails closed before any request', async () => {
		healthStore.readResult = new Error('Command timed out');
		lease.acquireResult = false;
		coordinator.start();
		await vi.advanceTimersByTimeAsync(30_000);
		expect(coordinator.isLeader).toBe(false);
		expect(fetchCycle).not.toHaveBeenCalled();
	});

	describe('OpenSky standby checks', () => {
		let checkOpensky: ReturnType<typeof vi.fn<() => Promise<OpenskyCheckResult>>>;
		const openskyWrites = () => healthStore.writes.filter((w) => w.provider === 'opensky');

		beforeEach(() => {
			fetchCycle.mockImplementation(hang); // keep adsb.fi out of the way
			checkOpensky = vi.fn(async (): Promise<OpenskyCheckResult> => {
				events.push('opensky:check');
				return { kind: 'ok', creditsRemaining: 399 };
			});
			coordinator = build({ checkOpensky });
		});

		it('never publish, credit or close coverage', async () => {
			coordinator.start();
			await vi.advanceTimersByTimeAsync(20 * 60_000);
			expect(checkOpensky.mock.calls.length).toBeGreaterThan(3);
			expect(publish).not.toHaveBeenCalled();
			expect(timeline.credits).toEqual([]);
			expect(timeline.closes).toEqual(['coordinator_down']);
		});

		it('unknown checks at once, recovers at 25 s, then drops to 15 min once HEALTHY', async () => {
			coordinator.start();
			await vi.advanceTimersByTimeAsync(10);
			expect(checkOpensky).toHaveBeenCalledTimes(1);
			expect(healthStore.last('opensky')).toMatchObject({
				state: 'RECOVERING',
				creditsRemaining: 399,
			});
			// Checks at 0, 25, 50, 75, 100 s are inside the 120 s window; the
			// first success at or past it is the check at 125 s.
			await vi.advanceTimersByTimeAsync(RECOVERY_MS);
			expect(healthStore.last('opensky')?.state).toBe('RECOVERING');
			await vi.advanceTimersByTimeAsync(25_000);
			expect(healthStore.last('opensky')?.state).toBe('HEALTHY');
			const atHealthy = checkOpensky.mock.calls.length;
			expect(atHealthy).toBe(6);
			// HEALTHY was reached by the check at 125 s, now 20 s ago: the next
			// check is 15 min after that one.
			await vi.advanceTimersByTimeAsync(900_000 - 20_000 - 1_000);
			expect(checkOpensky).toHaveBeenCalledTimes(atHealthy);
			await vi.advanceTimersByTimeAsync(2_000);
			expect(checkOpensky).toHaveBeenCalledTimes(atHealthy + 1);
		});

		it('a 429 with a retry time pauses: no check until paused_until_ms, then one at once', async () => {
			checkOpensky.mockResolvedValueOnce({ kind: 'rate_limited', retryAfterSeconds: 3_600 });
			coordinator.start();
			await vi.advanceTimersByTimeAsync(10);
			const paused = healthStore.last('opensky')!;
			expect(paused).toMatchObject({ state: 'UNAVAILABLE', lastError: 'rate_limited' });
			expect(paused.pausedUntilMs).toBe(paused.lastProbeMs! + 3_600_000);
			await vi.advanceTimersByTimeAsync(3_600_000 - 100);
			expect(checkOpensky).toHaveBeenCalledTimes(1);
			await vi.advanceTimersByTimeAsync(200);
			expect(checkOpensky).toHaveBeenCalledTimes(2);
			expect(healthStore.last('opensky')).toMatchObject({
				state: 'RECOVERING',
				pausedUntilMs: null,
			});
		});

		it('a 429 without a retry time is an ordinary failure, rechecked in 30 s while DEGRADED', async () => {
			// A 25 s recovery window gets OpenSky to HEALTHY on its second check.
			coordinator = build({
				checkOpensky,
				healthTiming: { degradedTimeoutMs: DEGRADED_MS, recoveryWindowMs: 25_000 },
			});
			coordinator.start();
			await vi.advanceTimersByTimeAsync(10 + 25_000);
			expect(healthStore.last('opensky')?.state).toBe('HEALTHY');
			checkOpensky.mockResolvedValueOnce({ kind: 'rate_limited', retryAfterSeconds: null });
			await vi.advanceTimersByTimeAsync(900_000); // the next HEALTHY check fails
			expect(healthStore.last('opensky')).toMatchObject({
				state: 'DEGRADED',
				lastError: 'rate_limited',
				pausedUntilMs: null,
			});
			const failedAt = checkOpensky.mock.calls.length;
			await vi.advanceTimersByTimeAsync(29_000);
			expect(checkOpensky).toHaveBeenCalledTimes(failedAt);
			await vi.advanceTimersByTimeAsync(1_000);
			expect(checkOpensky).toHaveBeenCalledTimes(failedAt + 1);
			// The 30 s recheck lands inside the 60 s window: straight back to HEALTHY.
			expect(healthStore.last('opensky')?.state).toBe('HEALTHY');
		});

		it('a token failure is a health failure with its own class, and stamps last_probe_ms', async () => {
			checkOpensky.mockResolvedValueOnce({
				kind: 'failed',
				error: 'auth: OpenSky token request failed: HTTP 401',
			});
			coordinator.start();
			await vi.advanceTimersByTimeAsync(10);
			const h = healthStore.last('opensky')!;
			expect(h).toMatchObject({
				state: 'UNAVAILABLE',
				lastError: 'auth: OpenSky token request failed: HTTP 401',
			});
			expect(h.lastProbeMs).toBe(h.lastFailureMs);
		});

		it('keeps the last known credits when a response has no usable header', async () => {
			checkOpensky.mockResolvedValueOnce({ kind: 'ok', creditsRemaining: 250 });
			checkOpensky.mockResolvedValueOnce({ kind: 'ok', creditsRemaining: null });
			coordinator.start();
			await vi.advanceTimersByTimeAsync(10 + 25_000);
			expect(openskyWrites().map((w) => w.health.creditsRemaining)).toEqual([250, 250]);
		});

		it('restores a future pause exactly and waits it out', async () => {
			const pausedUntilMs = Date.now() + 3_600_000;
			healthStore.snapshot.stored.opensky = stored({ state: 'UNAVAILABLE', pausedUntilMs });
			coordinator.start();
			await vi.advanceTimersByTimeAsync(3_600_000 - 1_000);
			expect(checkOpensky).not.toHaveBeenCalled();
			expect(openskyWrites()).toEqual([]); // unchanged by restore: nothing to write
			await vi.advanceTimersByTimeAsync(2_000);
			expect(checkOpensky).toHaveBeenCalledTimes(1);
		});

		it('clears an expired pause in the restore write and checks at once', async () => {
			healthStore.snapshot.stored.opensky = stored({
				state: 'UNAVAILABLE',
				pausedUntilMs: Date.now() - 1,
			});
			coordinator.start();
			await vi.advanceTimersByTimeAsync(10);
			expect(openskyWrites()[0]!.health).toMatchObject({
				state: 'UNAVAILABLE',
				pausedUntilMs: null,
			});
			expect(checkOpensky).toHaveBeenCalledTimes(1);
		});

		it('restarts an unpaused UNAVAILABLE backoff at 60 s after acquisition', async () => {
			healthStore.snapshot.stored.opensky = stored({ state: 'UNAVAILABLE' });
			coordinator.start();
			await vi.advanceTimersByTimeAsync(60_000 - 100);
			expect(checkOpensky).not.toHaveBeenCalled();
			await vi.advanceTimersByTimeAsync(200);
			expect(checkOpensky).toHaveBeenCalledTimes(1);
		});

		it('restores RECOVERING as UNAVAILABLE, losing the streak', async () => {
			healthStore.snapshot.stored.opensky = stored({
				state: 'RECOVERING',
				successStreakSinceMs: 1,
			});
			coordinator.start();
			await vi.advanceTimersByTimeAsync(10);
			expect(openskyWrites()[0]!.health).toMatchObject({
				state: 'UNAVAILABLE',
				successStreakSinceMs: null,
			});
		});

		it('stops checking on shutdown, after the in-flight check finishes', async () => {
			fetchCycle.mockImplementation(async () => split()); // shutdown waits for an adsb.fi cycle too
			let finishCheck!: () => void;
			checkOpensky.mockImplementationOnce(
				() => new Promise((r) => (finishCheck = () => r({ kind: 'ok', creditsRemaining: 1 }))),
			);
			coordinator.start();
			await vi.advanceTimersByTimeAsync(10);
			const done = coordinator.shutdown();
			await vi.advanceTimersByTimeAsync(10);
			expect(lease.releases).toBe(0); // waits for the check
			finishCheck();
			await done;
			expect(lease.releases).toBe(1);
			await vi.advanceTimersByTimeAsync(30 * 60_000);
			expect(checkOpensky).toHaveBeenCalledTimes(1);
		});
	});
});
