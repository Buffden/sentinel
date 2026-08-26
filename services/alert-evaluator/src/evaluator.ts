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

let scanning = false;

async function runScan(): Promise<void> {
  // Scan placeholder — signal-loss detection added in CP2.
  console.info({ instanceId }, 'leader scan tick');
}

async function startLeader(): Promise<void> {
  scanning = true;
  console.info({ instanceId }, 'acquired leader lease — starting scan loop');

  leader.startRenewal(() => {
    // Lease lost mid-run: stop the scan loop immediately.
    scanning = false;
    console.warn({ instanceId }, 'lease lost — reverting to follower');
    startFollower();
  });

  while (scanning) {
    await runScan();
    await sleep(SCAN_INTERVAL_MS);
  }
}

async function startFollower(): Promise<void> {
  console.info({ instanceId }, 'running as follower — waiting for leader lease');

  while (true) {
    await sleep(FOLLOWER_RETRY_INTERVAL_MS);
    const acquired = await leader.tryAcquire();
    if (acquired) {
      await startLeader();
      return;
    }
  }
}

async function main(): Promise<void> {
  console.info({ instanceId }, 'alert evaluator starting');

  const acquired = await leader.tryAcquire();
  if (acquired) {
    await startLeader();
  } else {
    await startFollower();
  }
}

async function shutdown(): Promise<void> {
  console.info({ instanceId }, 'shutting down');
  scanning = false;
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

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
