import type { Redis } from 'ioredis';

// Lua script for safe renewal: only extend the TTL if this
// instance still owns the key. A different owner must not be
// evicted by a stale renew call.
const RENEW_SCRIPT = `
	if redis.call('GET', KEYS[1]) == ARGV[1] then
		return redis.call('PEXPIRE', KEYS[1], ARGV[2])
	else
		return 0
	end
`;

// Lua script for safe release: only delete the key if this
// instance owns it. Prevents releasing another instance's lease
// after a network partition or restart.
const RELEASE_SCRIPT = `
	if redis.call('GET', KEYS[1]) == ARGV[1] then
		return redis.call('DEL', KEYS[1])
	else
		return 0
	end
`;

export class LeaderElection {
	private renewalTimer: NodeJS.Timeout | null = null;

	constructor(
		private readonly redis: Redis,
		private readonly instanceId: string,
		private readonly leaderKey: string,
		private readonly leaseTtlMs: number,
		private readonly renewalIntervalMs: number,
	) {}

	// Attempt to acquire the leader lease.
	// Returns true if this instance is now the leader.
	async tryAcquire(): Promise<boolean> {
		const result = await this.redis.set(
			this.leaderKey,
			this.instanceId,
			'PX',
			this.leaseTtlMs,
			'NX',
		);
		return result === 'OK';
	}

	// Start the background renewal loop.
	// Must only be called after tryAcquire() returns true.
	startRenewal(onLeaseLost: () => void): void {
		this.renewalTimer = setInterval(async () => {
			try {
				const renewed = await this.renew();
				if (!renewed) {
					console.warn({ instanceId: this.instanceId }, 'leader lease lost — stopping renewal');
					this.stopRenewal();
					onLeaseLost();
				}
			} catch (err) {
				// Redis error means we cannot confirm ownership. Fail closed: treat as lease loss.
				// Continuing to scan without confirmed ownership would risk duplicate work.
				console.error(
					{ instanceId: this.instanceId, err },
					'renewal error — treating as lease loss',
				);
				this.stopRenewal();
				onLeaseLost();
			}
		}, this.renewalIntervalMs);
	}

	stopRenewal(): void {
		if (this.renewalTimer !== null) {
			clearInterval(this.renewalTimer);
			this.renewalTimer = null;
		}
	}

	// Extend the TTL only if this instance still owns the key.
	// Returns true if the renewal succeeded.
	async renew(): Promise<boolean> {
		const result = await this.redis.eval(
			RENEW_SCRIPT,
			1,
			this.leaderKey,
			this.instanceId,
			String(this.leaseTtlMs),
		);
		return result === 1;
	}

	// Release the lease only if this instance owns it.
	async release(): Promise<void> {
		await this.redis.eval(RELEASE_SCRIPT, 1, this.leaderKey, this.instanceId);
	}
}
