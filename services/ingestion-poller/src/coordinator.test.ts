import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { SplitResult } from './adsbfiPoller.js';
import { Coordinator, type CoordinatorDeps } from './coordinator.js';

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

const RENEWAL_MS = 5_000;
const FOLLOWER_RETRY_MS = 5_000;
const POLL_MS = 2_000;

function split(count = 1): SplitResult {
	return {
		messages: Array.from({ length: count }, (_, i) => ({ key: `ac${i}`, value: '{}' })),
		total: count,
		skippedNonIcao: 0,
		skippedNoPosition: 0,
		skippedOutsideBox: 0,
	};
}

let lease: FakeLease;
let events: string[];
let fetchCycle: ReturnType<typeof vi.fn<() => Promise<SplitResult | null>>>;
let publish: ReturnType<typeof vi.fn<CoordinatorDeps['publish']>>;
let coordinator: Coordinator;

beforeEach(() => {
	vi.useFakeTimers();
	lease = new FakeLease();
	events = [];
	fetchCycle = vi.fn(async () => split());
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
		fetchCycle,
		publish,
		log: (_level, message) => events.push(message),
		renewalIntervalMs: RENEWAL_MS,
		followerRetryMs: FOLLOWER_RETRY_MS,
		pollIntervalMs: POLL_MS,
		backoffBaseMs: POLL_MS,
		backoffMaxMs: 60_000,
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

		// The in-flight cycle still published, and only then was the lease released.
		expect(events.indexOf('publish')).toBeGreaterThanOrEqual(0);
		expect(events.indexOf('publish')).toBeLessThan(events.indexOf('release'));
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
