import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { SplitResult } from './adsbfiPoller.js';
import { Coordinator, type CoordinatorDeps } from './coordinator.js';
import type { CloseResult, CoverageCloseReason, CreditResult } from './coverageTimeline.js';

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
let events: string[];
let fetchCycle: ReturnType<typeof vi.fn<() => Promise<SplitResult | null>>>;
let publish: ReturnType<typeof vi.fn<CoordinatorDeps['publish']>>;
let coordinator: Coordinator;

beforeEach(() => {
	vi.useFakeTimers();
	lease = new FakeLease();
	timeline = new FakeTimeline();
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
	coordinator = new Coordinator({
		lease,
		timeline,
		fetchCycle,
		publish,
		log: (_level, message) => events.push(message),
		renewalIntervalMs: RENEWAL_MS,
		followerRetryMs: FOLLOWER_RETRY_MS,
		pollIntervalMs: POLL_MS,
		backoffBaseMs: POLL_MS,
		backoffMaxMs: 60_000,
		frozenFeedMs: FROZEN_MS,
	});
});

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
			return null;
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

	it('fails closed when the failure close errors', async () => {
		coordinator.start();
		await runCycles(1);
		timeline.closeResult = new Error('Command timed out');
		lease.acquireResult = false;
		fetchCycle.mockImplementation(async () => null);
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
