import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { AdsbfiFetchFailure, SplitResult } from './adsbfiPoller.js';
import { Coordinator, type CoordinatorDeps } from './coordinator.js';
import type {
	CloseResult,
	CommitResult,
	CoverageCloseReason,
	CreditResult,
	RelinquishResult,
} from './coverageTimeline.js';
import type { OpenskyFetchResult } from './poller.js';
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
	creditProviders: string[] = [];
	closes: CoverageCloseReason[] = [];
	commits: { provider: string; atMs: number }[] = [];
	relinquishes: string[] = [];
	creditResult: CreditResult | Error = { status: 'extended', timelineVersion: 1 };
	closeResult: CloseResult | Error = { status: 'already_closed' };
	// Queued results for the next commits; afterwards every commit succeeds.
	commitResults: (CommitResult | Error)[] = [];
	relinquishResult: RelinquishResult | Error = {
		status: 'relinquished',
		timelineVersion: 9,
		member: null,
	};
	epoch = 1;

	async credit(_token: string, provider: string, activeSuccessMs: number): Promise<CreditResult> {
		events.push(`credit:${provider}`);
		this.credits.push(activeSuccessMs);
		this.creditProviders.push(provider);
		if (this.creditResult instanceof Error) throw this.creditResult;
		return this.creditResult;
	}
	async commit(_token: string, provider: string, atMs: number): Promise<CommitResult> {
		events.push(`commit:${provider}`);
		const queued = this.commitResults.shift();
		if (queued instanceof Error) throw queued;
		if (queued !== undefined) return queued;
		this.commits.push({ provider, atMs });
		return { status: 'committed', epoch: ++this.epoch, timelineVersion: 10 + this.epoch };
	}
	async relinquish(_token: string, expected: string): Promise<RelinquishResult> {
		events.push(`relinquish:${expected}`);
		this.relinquishes.push(expected);
		if (this.relinquishResult instanceof Error) throw this.relinquishResult;
		return this.relinquishResult;
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

// A provider that was HEALTHY when the previous holder stopped: restored as
// DEGRADED with a fresh 60 s, the usual state of a working authority.
function storedHealthy(): StoredHealth {
	return {
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
		},
		problem: null,
		present: true,
	};
}

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

// An OpenSky response that validated, with its mapped messages.
function osOk(credits: number | null, aircraft = 0): OpenskyFetchResult {
	return {
		kind: 'ok',
		messages: Array.from({ length: aircraft }, (_, i) => ({ key: `os${i}`, value: '{}' })),
		responseTime: 1790371086,
		creditsRemaining: credits,
	};
}

const DEGRADED_MS = 60_000;
const STANDBY_MS = 10_000;
const OPENSKY_ACTIVE_MS = 25_000;
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
// Every log line with its fields, for assertions on plans and epochs.
let logs: { message: string; extra: Record<string, unknown> }[];
let fetchCycle: ReturnType<typeof vi.fn<() => Promise<SplitResult | AdsbfiFetchFailure>>>;
let fetchOpenskyMock: ReturnType<typeof vi.fn<() => Promise<OpenskyFetchResult>>>;
let publish: ReturnType<typeof vi.fn<CoordinatorDeps['publish']>>;
let coordinator: Coordinator;

beforeEach(() => {
	vi.useFakeTimers();
	lease = new FakeLease();
	timeline = new FakeTimeline();
	healthStore = new FakeHealthStore();
	events = [];
	logs = [];
	providerNow = 1790283486000;
	fetchCycle = vi.fn(async () => {
		events.push('fetch');
		return split();
	});
	fetchOpenskyMock = vi.fn(async () => {
		events.push('opensky:fetch');
		return osOk(null);
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
		fetchOpensky: fetchOpenskyMock,
		openskyAuthenticated: true,
		healthTiming: { degradedTimeoutMs: DEGRADED_MS, recoveryWindowMs: RECOVERY_MS },
		openskyCadence: {
			healthyMs: 900_000,
			degradedMs: 30_000,
			recoveringMs: 25_000,
			backoffBaseMs: 60_000,
			backoffMaxMs: 900_000,
		},
		publish,
		log: (_level, message, extra) => {
			events.push(message);
			logs.push({ message, extra: extra ?? {} });
		},
		renewalIntervalMs: RENEWAL_MS,
		followerRetryMs: FOLLOWER_RETRY_MS,
		pollIntervalMs: POLL_MS,
		backoffBaseMs: POLL_MS,
		backoffMaxMs: 60_000,
		frozenFeedMs: FROZEN_MS,
		adsbfiStandbyIntervalMs: STANDBY_MS,
		openskyActiveIntervalMs: OPENSKY_ACTIVE_MS,
		selectionRetryBaseMs: 60_000,
		selectionRetryMaxMs: 900_000,
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
	// A working adsb.fi authority. With unknown health its first failure
	// would mean UNAVAILABLE and relinquish authority (CP3e), which these
	// coverage tests are not about.
	beforeEach(() => {
		healthStore.snapshot.stored.adsbfi = storedHealthy();
	});

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
		expect(events.lastIndexOf('publish')).toBeLessThan(events.indexOf('credit:adsbfi'));
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
		// Never credited for adsb.fi. (After 60 s frozen it is UNAVAILABLE and
		// relinquishes; OpenSky then takes authority, which is CP3e failover.)
		expect(timeline.creditProviders.filter((p) => p === 'adsbfi')).toEqual([]);
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
		// Stops before the 60 s deadline, after which authority would move.
		await vi.advanceTimersByTimeAsync(50_000);
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

	it('true first deployment: a seeded adsb.fi response may commit authority without opening coverage', async () => {
		healthStore.snapshot = {
			authorityInitialized: false,
			authorityProvider: null,
			stored: { adsbfi: UNKNOWN, opensky: UNKNOWN },
		};
		fetchOpenskyMock.mockImplementation(async () => ({ kind: 'failed', error: 'http_503' }));
		coordinator.start();
		await vi.advanceTimersByTimeAsync(10);
		expect(healthStore.last('adsbfi')).toMatchObject({
			state: 'HEALTHY',
			consecutiveFailures: 0,
			lastFailureMs: null,
		});
		// Freshness gates coverage, not authority: the first valid response is
		// published and commits authority, but seeded data does not CREDIT.
		expect(publish).toHaveBeenCalledTimes(1);
		expect(timeline.commits.map((entry) => entry.provider)).toEqual(['adsbfi']);
		expect(timeline.credits).toEqual([]);
		expect(coordinator.authority).toBe('adsbfi');
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
		healthStore.snapshot.stored.adsbfi = stored({ state: 'HEALTHY' });
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

	it('a stored authority restored UNAVAILABLE is relinquished before any adsb.fi active cycle', async () => {
		healthStore.snapshot.stored.adsbfi = stored({ state: 'UNAVAILABLE' });
		fetchOpenskyMock.mockImplementation(async () => ({ kind: 'failed', error: 'http_503' }));
		coordinator.start();
		await vi.advanceTimersByTimeAsync(10);
		expect(timeline.relinquishes).toEqual(['adsbfi']);
		expect(events.indexOf('relinquish:adsbfi')).toBeLessThan(events.indexOf('fetch'));
		// After relinquish, the emergency request is a candidate. Its seeded
		// response may restore authority, but it still cannot open coverage.
		expect(publish).toHaveBeenCalledTimes(1);
		expect(timeline.commits.map((entry) => entry.provider)).toEqual(['adsbfi']);
		expect(timeline.creditProviders).toEqual([]);
		expect(coordinator.authority).toBe('adsbfi');
	});

	it.each([
		['a refused health write', 'lease_mismatch' as const],
		['a health write that times out', new Error('Command timed out')],
	])('%s fails closed: no more requests or checks', async (_label, writeResult) => {
		const fetchOpensky = vi.fn(async (): Promise<OpenskyFetchResult> => osOk(399));
		coordinator = build({ fetchOpensky });
		lease.acquireResult = false; // stays a follower after losing the lease
		healthStore.writeResult = writeResult;
		coordinator.start();
		await vi.advanceTimersByTimeAsync(10);
		expect(coordinator.isLeader).toBe(false);
		const fetches = fetchCycle.mock.calls.length;
		const checks = fetchOpensky.mock.calls.length;
		await vi.advanceTimersByTimeAsync(10 * 60_000);
		expect(fetchCycle).toHaveBeenCalledTimes(fetches);
		expect(fetchOpensky).toHaveBeenCalledTimes(checks);
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
		let fetchOpensky: ReturnType<typeof vi.fn<() => Promise<OpenskyFetchResult>>>;
		const openskyWrites = () => healthStore.writes.filter((w) => w.provider === 'opensky');

		beforeEach(() => {
			fetchCycle.mockImplementation(hang); // keep adsb.fi out of the way
			fetchOpensky = vi.fn(async (): Promise<OpenskyFetchResult> => {
				events.push('opensky:check');
				return osOk(399);
			});
			coordinator = build({ fetchOpensky });
		});

		it('never publish, credit or close coverage', async () => {
			coordinator.start();
			await vi.advanceTimersByTimeAsync(20 * 60_000);
			expect(fetchOpensky.mock.calls.length).toBeGreaterThan(3);
			expect(publish).not.toHaveBeenCalled();
			expect(timeline.credits).toEqual([]);
			expect(timeline.closes).toEqual(['coordinator_down']);
		});

		it('unknown checks at once, recovers at 25 s, then drops to 15 min once HEALTHY', async () => {
			coordinator.start();
			await vi.advanceTimersByTimeAsync(10);
			expect(fetchOpensky).toHaveBeenCalledTimes(1);
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
			const atHealthy = fetchOpensky.mock.calls.length;
			expect(atHealthy).toBe(6);
			// HEALTHY was reached by the check at 125 s, now 20 s ago: the next
			// check is 15 min after that one.
			await vi.advanceTimersByTimeAsync(900_000 - 20_000 - 1_000);
			expect(fetchOpensky).toHaveBeenCalledTimes(atHealthy);
			await vi.advanceTimersByTimeAsync(2_000);
			expect(fetchOpensky).toHaveBeenCalledTimes(atHealthy + 1);
		});

		it('a 429 with a retry time pauses: no check until paused_until_ms, then one at once', async () => {
			fetchOpensky.mockResolvedValueOnce({ kind: 'rate_limited', retryAfterSeconds: 3_600 });
			coordinator.start();
			await vi.advanceTimersByTimeAsync(10);
			const paused = healthStore.last('opensky')!;
			expect(paused).toMatchObject({ state: 'UNAVAILABLE', lastError: 'rate_limited' });
			expect(paused.pausedUntilMs).toBe(paused.lastProbeMs! + 3_600_000);
			await vi.advanceTimersByTimeAsync(3_600_000 - 100);
			expect(fetchOpensky).toHaveBeenCalledTimes(1);
			await vi.advanceTimersByTimeAsync(200);
			expect(fetchOpensky).toHaveBeenCalledTimes(2);
			expect(healthStore.last('opensky')).toMatchObject({
				state: 'RECOVERING',
				pausedUntilMs: null,
			});
		});

		it('a 429 without a retry time is an ordinary failure, rechecked in 30 s while DEGRADED', async () => {
			// A 25 s recovery window gets OpenSky to HEALTHY on its second check.
			coordinator = build({
				fetchOpensky,
				healthTiming: { degradedTimeoutMs: DEGRADED_MS, recoveryWindowMs: 25_000 },
			});
			coordinator.start();
			await vi.advanceTimersByTimeAsync(10 + 25_000);
			expect(healthStore.last('opensky')?.state).toBe('HEALTHY');
			fetchOpensky.mockResolvedValueOnce({ kind: 'rate_limited', retryAfterSeconds: null });
			await vi.advanceTimersByTimeAsync(900_000); // the next HEALTHY check fails
			expect(healthStore.last('opensky')).toMatchObject({
				state: 'DEGRADED',
				lastError: 'rate_limited',
				pausedUntilMs: null,
			});
			const failedAt = fetchOpensky.mock.calls.length;
			await vi.advanceTimersByTimeAsync(29_000);
			expect(fetchOpensky).toHaveBeenCalledTimes(failedAt);
			await vi.advanceTimersByTimeAsync(1_000);
			expect(fetchOpensky).toHaveBeenCalledTimes(failedAt + 1);
			// The 30 s recheck lands inside the 60 s window: straight back to HEALTHY.
			expect(healthStore.last('opensky')?.state).toBe('HEALTHY');
		});

		it('a token failure is a health failure with its own class, and stamps last_probe_ms', async () => {
			fetchOpensky.mockResolvedValueOnce({
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
			fetchOpensky.mockResolvedValueOnce(osOk(250));
			fetchOpensky.mockResolvedValueOnce(osOk(null));
			coordinator.start();
			await vi.advanceTimersByTimeAsync(10 + 25_000);
			expect(openskyWrites().map((w) => w.health.creditsRemaining)).toEqual([250, 250]);
		});

		it('restores a future pause exactly and waits it out', async () => {
			const pausedUntilMs = Date.now() + 3_600_000;
			healthStore.snapshot.stored.opensky = stored({ state: 'UNAVAILABLE', pausedUntilMs });
			coordinator.start();
			await vi.advanceTimersByTimeAsync(3_600_000 - 1_000);
			expect(fetchOpensky).not.toHaveBeenCalled();
			expect(openskyWrites()).toEqual([]); // unchanged by restore: nothing to write
			await vi.advanceTimersByTimeAsync(2_000);
			expect(fetchOpensky).toHaveBeenCalledTimes(1);
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
			expect(fetchOpensky).toHaveBeenCalledTimes(1);
		});

		it('restarts an unpaused UNAVAILABLE backoff at 60 s after acquisition', async () => {
			healthStore.snapshot.stored.opensky = stored({ state: 'UNAVAILABLE' });
			coordinator.start();
			await vi.advanceTimersByTimeAsync(60_000 - 100);
			expect(fetchOpensky).not.toHaveBeenCalled();
			await vi.advanceTimersByTimeAsync(200);
			expect(fetchOpensky).toHaveBeenCalledTimes(1);
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
			fetchOpensky.mockImplementationOnce(
				() => new Promise((r) => (finishCheck = () => r(osOk(1)))),
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
			expect(fetchOpensky).toHaveBeenCalledTimes(1);
		});
	});
});

describe('Coordinator failover (ADR-022 section 4, CP3e)', () => {
	const hang = () => new Promise<never>(() => {});
	// Health stored by the previous holder, recent enough not to be stale.
	const recent = (
		state: ProviderHealth['state'],
		o: Partial<ProviderHealth> = {},
	): StoredHealth => ({
		health: {
			state,
			stateSinceMs: Date.now() - 1_000,
			lastSuccessMs: Date.now() - 500,
			lastFailureMs: null,
			consecutiveFailures: 0,
			lastError: null,
			successStreakSinceMs: state === 'RECOVERING' ? Date.now() - 1_000 : null,
			pausedUntilMs: null,
			creditsRemaining: null,
			lastProbeMs: Date.now() - 500,
			...o,
		},
		problem: null,
		present: true,
	});
	const authorityIs = (provider: string) => {
		healthStore.snapshot.authorityInitialized = true;
		healthStore.snapshot.authorityProvider = provider;
	};
	const plans = () =>
		logs.filter((l) => l.message === 'selection round').map((l) => l.extra['plan'] as string[]);
	const committed = () =>
		logs.filter((l) => l.message === 'authority committed').map((l) => l.extra);
	const opensky = (result: () => OpenskyFetchResult) =>
		fetchOpenskyMock.mockImplementation(async () => {
			events.push('opensky:fetch');
			return result();
		});
	const adsbfiFails = () =>
		fetchCycle.mockImplementation(async () => {
			events.push('fetch');
			return { error: 'timeout' };
		});

	it('a DEGRADED authority keeps authority until its 60 s deadline', async () => {
		authorityIs('adsbfi');
		healthStore.snapshot.stored.adsbfi = recent('HEALTHY');
		adsbfiFails();
		opensky(() => ({ kind: 'failed', error: 'http_503' }));
		coordinator.start();
		await vi.advanceTimersByTimeAsync(59_000);
		expect(timeline.relinquishes).toEqual([]);
		expect(coordinator.authority).toBe('adsbfi');
	});

	it('an UNAVAILABLE authority is relinquished to none and its active cycles stop', async () => {
		authorityIs('adsbfi');
		healthStore.snapshot.stored.adsbfi = recent('HEALTHY');
		adsbfiFails();
		opensky(() => ({ kind: 'failed', error: 'http_503' }));
		coordinator.start();
		await vi.advanceTimersByTimeAsync(61_000);
		expect(timeline.relinquishes).toEqual(['adsbfi']);
		expect(coordinator.authority).toBe('none');
		expect(plans()[0]).toEqual(['adsbfi:tier2:one_shot', 'opensky:tier2:one_shot']);
		// No longer every 2 s: adsb.fi is now asked at its standby rate.
		const before = fetchCycle.mock.calls.length;
		await vi.advanceTimersByTimeAsync(20_000);
		expect(fetchCycle.mock.calls.length - before).toBeLessThanOrEqual(2);
		expect(publish).not.toHaveBeenCalled();
	});

	it('a stored none lets the preferred adsb.fi candidate commit on its first valid response', async () => {
		authorityIs('none');
		healthStore.snapshot.stored.adsbfi = recent('HEALTHY');
		opensky(() => ({ kind: 'failed', error: 'http_503' }));
		coordinator.start();
		await vi.advanceTimersByTimeAsync(10);
		// Stored HEALTHY restores as DEGRADED (downtime proves nothing): tier 2.
		expect(plans()[0]).toEqual(['adsbfi:tier2', 'opensky:tier2:one_shot']);
		expect(timeline.commits.map((entry) => entry.provider)).toEqual(['adsbfi']);
		expect(timeline.creditProviders).toEqual([]); // seeded: authority only
		expect(coordinator.authority).toBe('adsbfi');
		expect(publish).toHaveBeenCalledTimes(1);
	});

	it('an OpenSky candidate records health, then publishes, then commits: epoch +1 once', async () => {
		authorityIs('none');
		healthStore.snapshot.stored.adsbfi = recent('UNAVAILABLE');
		healthStore.snapshot.stored.opensky = recent('HEALTHY');
		adsbfiFails();
		opensky(() => osOk(398, 3));
		coordinator.start();
		await vi.advanceTimersByTimeAsync(10);
		const health = events.indexOf('health:opensky:HEALTHY');
		const pub = events.indexOf('publish');
		const commit = events.indexOf('commit:opensky');
		const credit = events.indexOf('credit:opensky');
		expect(health).toBeGreaterThanOrEqual(0);
		expect(health).toBeLessThan(pub);
		expect(pub).toBeLessThan(commit);
		expect(commit).toBeLessThan(credit);
		expect(committed()).toEqual([expect.objectContaining({ provider: 'opensky', epoch: 2 })]);
		expect(timeline.creditProviders).toEqual(['opensky']);
		expect(coordinator.authority).toBe('opensky');
	});

	it('a valid zero-aircraft OpenSky response commits without a publish call', async () => {
		authorityIs('none');
		healthStore.snapshot.stored.adsbfi = recent('UNAVAILABLE');
		healthStore.snapshot.stored.opensky = recent('HEALTHY');
		adsbfiFails();
		opensky(() => osOk(398, 0));
		coordinator.start();
		await vi.advanceTimersByTimeAsync(10);
		expect(publish).not.toHaveBeenCalled();
		expect(timeline.commits.map((c) => c.provider)).toEqual(['opensky']);
		expect(timeline.creditProviders).toEqual(['opensky']);
	});

	it('a Kafka failure keeps health success and backs off that candidate without a retry timer', async () => {
		authorityIs('none');
		healthStore.snapshot.stored.adsbfi = recent('UNAVAILABLE');
		healthStore.snapshot.stored.opensky = recent('HEALTHY');
		adsbfiFails();
		opensky(() => osOk(398, 2));
		publish.mockRejectedValue(new Error('broker unavailable'));
		coordinator.start();
		await vi.advanceTimersByTimeAsync(10);
		expect(coordinator.authority).toBe('none');
		expect(timeline.commits).toEqual([]);
		expect(healthStore.last('opensky')).toMatchObject({
			state: 'HEALTHY',
			consecutiveFailures: 0,
			lastFailureMs: null,
		});
		expect(logs.find((l) => l.message === 'candidate delivery backing off')?.extra).toMatchObject({
			provider: 'opensky',
			retry_in_ms: 60_000,
		});
		// No second selection-round timer exists. OpenSky retries on its own
		// candidate schedule, and a second delivery failure doubles its backoff.
		await vi.advanceTimersByTimeAsync(60_000);
		expect(plans()).toEqual([['adsbfi:tier2:one_shot', 'opensky:tier2']]);
		expect(
			logs
				.filter((l) => l.message === 'candidate delivery backing off')
				.map((l) => l.extra['retry_in_ms']),
		).toEqual([60_000, 120_000]);
	});

	it('after its one-shot, a provider is still a candidate at its own rate', async () => {
		authorityIs('none');
		healthStore.snapshot.stored.adsbfi = recent('UNAVAILABLE');
		opensky(() => ({ kind: 'failed', error: 'http_503' }));
		fetchCycle.mockImplementationOnce(async () => ({ error: 'timeout' })); // the one-shot fails
		coordinator.start();
		await vi.advanceTimersByTimeAsync(10);
		expect(timeline.commits).toEqual([]);
		// Its next successful request may commit even though it only seeds
		// freshness; coverage waits for a later fresh cycle.
		await vi.advanceTimersByTimeAsync(STANDBY_MS + 1_000);
		expect(timeline.commits.map((c) => c.provider)).toEqual(['adsbfi']);
		expect(healthStore.last('adsbfi')?.state).toBe('RECOVERING');
	});

	it('a commit refused for its time keeps authority none and retries on the provider schedule', async () => {
		authorityIs('none');
		healthStore.snapshot.stored.adsbfi = recent('UNAVAILABLE');
		healthStore.snapshot.stored.opensky = recent('HEALTHY');
		adsbfiFails();
		opensky(() => osOk(398, 1));
		timeline.commitResults = [{ status: 'stale_clock' }];
		coordinator.start();
		await vi.advanceTimersByTimeAsync(10);
		expect(coordinator.authority).toBe('none');
		expect(events).toContain('authority commit refused: time is not after the last success');
		expect(logs.find((l) => l.message === 'candidate delivery backing off')?.extra).toMatchObject({
			provider: 'opensky',
			retry_in_ms: 60_000,
		});
		expect(healthStore.last('opensky')?.lastFailureMs).toBeNull();
		expect(coordinator.isLeader).toBe(true);
		await vi.advanceTimersByTimeAsync(60_000);
		expect(timeline.commits.map((c) => c.provider)).toEqual(['opensky']);
		expect(coordinator.authority).toBe('opensky');
	});

	it('a commit whose result is unknown fails closed', async () => {
		authorityIs('none');
		healthStore.snapshot.stored.opensky = recent('HEALTHY');
		adsbfiFails();
		opensky(() => osOk(398, 1));
		timeline.commitResults = [new Error('Command timed out')];
		lease.acquireResult = false;
		coordinator.start();
		await vi.advanceTimersByTimeAsync(10);
		expect(coordinator.isLeader).toBe(false);
	});

	it('a paused OpenSky is not selected, and not asked, until its pause ends', async () => {
		authorityIs('none');
		healthStore.snapshot.stored.adsbfi = recent('UNAVAILABLE');
		healthStore.snapshot.stored.opensky = recent('UNAVAILABLE', {
			pausedUntilMs: Date.now() + 3_600_000,
			lastError: 'rate_limited',
		});
		adsbfiFails();
		coordinator.start();
		await vi.advanceTimersByTimeAsync(3_600_000 - 1_000);
		expect(plans()[0]).toEqual(['adsbfi:tier2:one_shot']);
		expect(fetchOpenskyMock).not.toHaveBeenCalled();
	});

	it('a stale HEALTHY provider is a tier-2 one-shot, and its success is RECOVERING', async () => {
		authorityIs('none');
		healthStore.snapshot.stored.adsbfi = recent('HEALTHY', {
			lastSuccessMs: Date.now() - 3_600_000,
			lastProbeMs: null,
		});
		healthStore.snapshot.stored.opensky = recent('HEALTHY');
		opensky(() => ({ kind: 'failed', error: 'http_503' }));
		coordinator.start();
		await vi.advanceTimersByTimeAsync(10);
		// Both restore as DEGRADED; adsb.fi's staleness makes it a one-shot.
		expect(plans()[0]).toEqual(['adsbfi:tier2:one_shot', 'opensky:tier2']);
		expect(healthStore.last('adsbfi')).toMatchObject({ state: 'RECOVERING' });
		expect(healthStore.last('adsbfi')?.successStreakSinceMs).toBe(
			healthStore.last('adsbfi')?.lastSuccessMs,
		);
	});

	it('OpenSky authoritative: one request per active cycle, adsb.fi standby never publishes, and no failback', async () => {
		authorityIs('opensky');
		healthStore.snapshot.stored.opensky = recent('HEALTHY');
		healthStore.snapshot.stored.adsbfi = recent('HEALTHY');
		opensky(() => osOk(398, 2));
		coordinator.start();
		await vi.advanceTimersByTimeAsync(10 * 60_000);
		// Active cycles every 25 s after the last one finished: no separate
		// OpenSky check ever runs alongside them.
		const calls = fetchOpenskyMock.mock.calls.length;
		expect(calls).toBeGreaterThanOrEqual(24);
		expect(calls).toBeLessThanOrEqual(25);
		// adsb.fi was asked at its standby rate, is HEALTHY, and never took over.
		expect(fetchCycle.mock.calls.length).toBeGreaterThanOrEqual(55);
		expect(healthStore.last('adsbfi')?.state).toBe('HEALTHY');
		expect(timeline.commits).toEqual([]);
		expect(timeline.relinquishes).toEqual([]);
		expect(new Set(timeline.creditProviders)).toEqual(new Set(['opensky']));
		for (const [messages] of publish.mock.calls) {
			expect(messages.every((m) => m.key.startsWith('os'))).toBe(true);
		}
	});

	it('an OpenSky health success never commits while adsb.fi holds authority', async () => {
		authorityIs('adsbfi');
		healthStore.snapshot.stored.adsbfi = recent('HEALTHY');
		opensky(() => osOk(398, 2));
		coordinator.start();
		await vi.advanceTimersByTimeAsync(20 * 60_000);
		expect(fetchOpenskyMock.mock.calls.length).toBeGreaterThan(1);
		expect(timeline.commits).toEqual([]);
		expect(new Set(timeline.creditProviders)).toEqual(new Set(['adsbfi']));
	});

	it('an OpenSky 429 while authoritative pauses it and relinquishes at once; a warm adsb.fi then commits', async () => {
		authorityIs('opensky');
		healthStore.snapshot.stored.opensky = recent('HEALTHY');
		opensky(() => osOk(398, 2));
		coordinator.start();
		// adsb.fi standby checks at 0, 10, 20 s warm the freshness tracker.
		await vi.advanceTimersByTimeAsync(24_000);
		opensky(() => ({ kind: 'rate_limited', retryAfterSeconds: 3_600 }));
		await vi.advanceTimersByTimeAsync(2_000); // the 25 s active cycle gets the 429
		expect(timeline.relinquishes).toEqual(['opensky']);
		expect(healthStore.last('opensky')?.pausedUntilMs).not.toBeNull();
		// The round's first adsb.fi request is already fresh: it commits at once.
		expect(timeline.commits.map((c) => c.provider)).toEqual(['adsbfi']);
		const openskyCalls = fetchOpenskyMock.mock.calls.length;
		await vi.advanceTimersByTimeAsync(10 * 60_000);
		expect(fetchOpenskyMock.mock.calls.length).toBe(openskyCalls); // paused
	});

	it('OpenSky failing past its deadline: none, then a HEALTHY adsb.fi commits (recovery, not failback)', async () => {
		authorityIs('opensky');
		healthStore.snapshot.stored.opensky = recent('HEALTHY');
		healthStore.snapshot.stored.adsbfi = recent('HEALTHY');
		opensky(() => osOk(398, 1));
		coordinator.start();
		await vi.advanceTimersByTimeAsync(30_000);
		opensky(() => ({ kind: 'failed', error: 'http_503' }));
		await vi.advanceTimersByTimeAsync(100_000);
		expect(timeline.relinquishes).toEqual(['opensky']);
		expect(plans()[0]?.[0]).toBe('adsbfi:tier1');
		expect(committed().map((c) => [c['provider'], c['epoch']])).toEqual([['adsbfi', 2]]);
		expect(coordinator.authority).toBe('adsbfi');
	});

	it("a relinquished provider's send already in flight settles before a candidate publishes", async () => {
		authorityIs('adsbfi');
		healthStore.snapshot.stored.adsbfi = recent('HEALTHY');
		healthStore.snapshot.stored.opensky = recent('HEALTHY');
		opensky(() => osOk(398, 1));
		const order: string[] = [];
		let releaseOld!: () => void;
		publish.mockImplementation(async (messages) => {
			const who = messages[0]!.key.startsWith('os') ? 'opensky' : 'adsbfi';
			order.push(`${who}:start`);
			if (who === 'adsbfi') await new Promise<void>((r) => (releaseOld = r));
			order.push(`${who}:end`);
			return '0';
		});
		coordinator.start();
		await vi.advanceTimersByTimeAsync(10); // adsb.fi's first cycle is publishing
		expect(order).toEqual(['adsbfi:start']);
		adsbfiFails(); // so the round's candidate is OpenSky
		// Artificially relinquish while that send is in flight.
		const c = coordinator as unknown as {
			relinquish(p: string, t: string, term: number): Promise<void>;
		};
		await c.relinquish('adsbfi', lease.token!, 1);
		// The round's adsb.fi attempt waits behind adsb.fi's own stuck cycle,
		// but OpenSky's own next request (30 s, DEGRADED) does not: it becomes
		// a candidate, fetches and records health, and then must wait for the
		// old send before publishing.
		await vi.advanceTimersByTimeAsync(31_000);
		expect(fetchOpenskyMock).toHaveBeenCalled();
		expect(order).toEqual(['adsbfi:start']);
		releaseOld();
		await vi.advanceTimersByTimeAsync(10);
		expect(order).toEqual(['adsbfi:start', 'adsbfi:end', 'opensky:start', 'opensky:end']);
		expect(timeline.commits.map((c2) => c2.provider)).toEqual(['opensky']);
		// The old cycle's credit after authority moved is benign.
		expect(coordinator.isLeader).toBe(true);
	});

	it('an active cycle that has not reached its publish drops out after a relinquish', async () => {
		authorityIs('adsbfi');
		healthStore.snapshot.stored.adsbfi = recent('HEALTHY');
		opensky(() => ({ kind: 'failed', error: 'http_503' }));
		let resolveFetch!: (v: SplitResult) => void;
		fetchCycle.mockImplementationOnce(
			() => new Promise<SplitResult>((resolve) => (resolveFetch = resolve)),
		);
		coordinator.start();
		await vi.advanceTimersByTimeAsync(10);
		const c = coordinator as unknown as {
			relinquish(p: string, t: string, term: number): Promise<void>;
		};
		await c.relinquish('adsbfi', lease.token!, 1);
		resolveFetch(split(2));
		await vi.advanceTimersByTimeAsync(10);
		expect(events).toContain('adsb.fi cycle dropped: authority changed before publishing');
		// The dropped cycle's two positions were never published. (A later
		// candidate may publish its own delivery.)
		expect(publish.mock.calls.some(([m]) => m.length === 2)).toBe(false);
	});

	it('CREDIT reporting another authority while this provider is believed authoritative fails closed', async () => {
		authorityIs('adsbfi');
		healthStore.snapshot.stored.adsbfi = recent('HEALTHY');
		timeline.creditResult = { status: 'authority_changed', authority: 'opensky' };
		lease.acquireResult = false;
		coordinator.start();
		await vi.advanceTimersByTimeAsync(10 + POLL_MS); // seed, then a fresh cycle credits
		expect(coordinator.isLeader).toBe(false);
	});

	it('warns when OpenSky becomes authoritative without credentials', async () => {
		coordinator = build({ openskyAuthenticated: false });
		authorityIs('none');
		healthStore.snapshot.stored.opensky = recent('HEALTHY');
		adsbfiFails();
		opensky(() => osOk(398, 1));
		coordinator.start();
		await vi.advanceTimersByTimeAsync(10);
		expect(events).toContain(
			'opensky authoritative without credentials: the anonymous budget will run out',
		);
	});

	it('a timeline that never had an authority is none in memory: OpenSky may make the first commit', async () => {
		healthStore.snapshot = {
			authorityInitialized: false,
			authorityProvider: null,
			stored: { adsbfi: UNKNOWN, opensky: UNKNOWN },
		};
		adsbfiFails();
		opensky(() => osOk(398, 1));
		coordinator.start();
		await vi.advanceTimersByTimeAsync(10);
		expect(timeline.relinquishes).toEqual([]); // nothing is written for virgin none
		expect(timeline.commits.map((c) => c.provider)).toEqual(['opensky']);
		// OpenSky's health follows the unknown rule: RECOVERING, not HEALTHY.
		expect(healthStore.last('opensky')?.state).toBe('RECOVERING');
	});
});
