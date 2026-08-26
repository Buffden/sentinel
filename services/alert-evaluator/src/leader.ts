import type { Redis } from 'ioredis';

// How long the leader key lives in Redis without renewal.
// Must be longer than one scan cycle so a slow scan does not
// cause the key to expire mid-run and trigger a false takeover.
const LEADER_LEASE_TTL_MS = 15_000;

// How often the leader renews the lease. Must be well under
// LEADER_LEASE_TTL_MS so a slow renewal does not cause expiry.
const LEADER_RENEWAL_INTERVAL_MS = 5_000;

const LEADER_KEY = 'alert-evaluator:leader';

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
  ) {}

  // Attempt to acquire the leader lease.
  // Returns true if this instance is now the leader.
  async tryAcquire(): Promise<boolean> {
    const result = await this.redis.set(
      LEADER_KEY,
      this.instanceId,
      'PX',
      LEADER_LEASE_TTL_MS,
      'NX',
    );
    return result === 'OK';
  }

  // Start the background renewal loop.
  // Must only be called after tryAcquire() returns true.
  startRenewal(onLeaseLost: () => void): void {
    this.renewalTimer = setInterval(async () => {
      const renewed = await this.renew();
      if (!renewed) {
        console.warn({ instanceId: this.instanceId }, 'leader lease lost — stopping renewal');
        this.stopRenewal();
        onLeaseLost();
      }
    }, LEADER_RENEWAL_INTERVAL_MS);
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
      LEADER_KEY,
      this.instanceId,
      String(LEADER_LEASE_TTL_MS),
    );
    return result === 1;
  }

  // Release the lease only if this instance owns it.
  async release(): Promise<void> {
    await this.redis.eval(RELEASE_SCRIPT, 1, LEADER_KEY, this.instanceId);
  }
}
