import { randomUUID } from 'node:crypto';
import { Redis } from 'ioredis';
import { LeaderElection } from './leader.js';

// How often the leader scans all entity:live:* keys.
const SCAN_INTERVAL_MS = 30_000;

// How long before a follower retries acquiring the leader lease.
const FOLLOWER_RETRY_INTERVAL_MS = 5_000;

const instanceId = randomUUID();
const redis = new Redis({ host: 'localhost', port: 6379 });
const leader = new LeaderElection(redis, instanceId);

async function runScan(): Promise<void> {
  // Scan placeholder — signal-loss detection added in CP2.
  console.info({ instanceId }, 'leader scan tick');
}

// Each time this instance becomes leader it gets a fresh AbortController.
// When the lease is lost (or revoked), the controller is aborted, which
// unblocks the sleeping loop immediately and stops it before the next tick.
// This prevents the previous-session loop from waking up and running
// alongside a newly started session.
async function runLeaderSession(): Promise<void> {
  const ac = new AbortController();

  leader.startRenewal(() => {
    console.warn({ instanceId }, 'lease lost — aborting leader session');
    ac.abort();
  });

  console.info({ instanceId }, 'acquired leader lease — starting scan loop');

  while (!ac.signal.aborted) {
    await runScan();
    await sleep(SCAN_INTERVAL_MS, ac.signal);
  }

  // Stop the renewal timer now that the loop has exited cleanly.
  leader.stopRenewal();
}

async function main(): Promise<void> {
  console.info({ instanceId }, 'alert evaluator starting');

  // Single loop: try to acquire, run as leader, then fall back to polling.
  // No separate startLeader / startFollower distinction needed — the main
  // loop already serialises the two roles cleanly.
  while (true) {
    const acquired = await leader.tryAcquire();
    if (acquired) {
      await runLeaderSession();
    } else {
      console.info({ instanceId }, 'running as follower — waiting for leader lease');
    }
    await sleep(FOLLOWER_RETRY_INTERVAL_MS);
  }
}

async function shutdown(): Promise<void> {
  console.info({ instanceId }, 'shutting down');
  leader.stopRenewal();
  await leader.release();
  await redis.quit();
}

process.on('SIGINT', () => { shutdown().then(() => process.exit(0)); });
process.on('SIGTERM', () => { shutdown().then(() => process.exit(0)); });

main().catch((err) => {
  console.error({ err }, 'fatal error');
  process.exit(1);
});

// Resolves after `ms` milliseconds, or immediately if the signal is already
// aborted or fires before the timer expires.
function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise<void>((resolve) => {
    if (signal?.aborted) { resolve(); return; }
    const timer = setTimeout(resolve, ms);
    signal?.addEventListener('abort', () => { clearTimeout(timer); resolve(); }, { once: true });
  });
}
