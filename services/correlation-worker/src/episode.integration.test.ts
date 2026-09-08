// Runs against a REAL Redis, not a mock: the guarantee under test is real
// Lua-script atomicity and real TTL expiry deciding when an episode ends --
// a fake eval/PEXPIRE would only prove the fake behaves as coded, not that
// concurrent instances or real expiry are handled correctly.
//
// Requires: `make up`.
import { randomUUID } from 'node:crypto';
import { Redis } from 'ioredis';
import { afterAll, afterEach, describe, expect, it } from 'vitest';
import { config } from './config.js';
import { touchProximityEpisode } from './episode.js';

const redis = new Redis(config.REDIS_URL);
const testPairKey = () => `test-episode-${randomUUID()}`;

const seededKeys = new Set<string>();

afterEach(async () => {
	if (seededKeys.size > 0) {
		await redis.del(...seededKeys);
		seededKeys.clear();
	}
});

afterAll(async () => {
	await redis.quit();
});

describe('touchProximityEpisode', () => {
	it('starts a new episode on the first confirmation for a pair', async () => {
		const pairKey = testPairKey();
		seededKeys.add(`proximity-episode:${pairKey}`);

		const result = await touchProximityEpisode(redis, pairKey, 1_700_000_000_000, 60_000);

		expect(result).toEqual({ isNewEpisode: true, episodeStartMs: 1_700_000_000_000 });
	});

	it('sets the gap TTL on a new episode', async () => {
		const pairKey = testPairKey();
		seededKeys.add(`proximity-episode:${pairKey}`);

		await touchProximityEpisode(redis, pairKey, 1_700_000_000_000, 60_000);

		const ttlMs = await redis.pttl(`proximity-episode:${pairKey}`);
		expect(ttlMs).toBeGreaterThan(0);
		expect(ttlMs).toBeLessThanOrEqual(60_000);
	});

	it('reports an existing episode and keeps its original start time', async () => {
		const pairKey = testPairKey();
		seededKeys.add(`proximity-episode:${pairKey}`);

		await touchProximityEpisode(redis, pairKey, 1_700_000_000_000, 60_000);
		const result = await touchProximityEpisode(redis, pairKey, 1_700_000_010_000, 60_000);

		expect(result).toEqual({ isNewEpisode: false, episodeStartMs: 1_700_000_000_000 });

		const lastSeen = await redis.hget(`proximity-episode:${pairKey}`, 'last_seen_ms');
		expect(lastSeen).toBe('1700000010000');
	});

	it('renews the TTL on an existing episode without moving last_seen_ms backward for an out-of-order confirmation', async () => {
		const pairKey = testPairKey();
		seededKeys.add(`proximity-episode:${pairKey}`);

		await touchProximityEpisode(redis, pairKey, 1_700_000_010_000, 60_000);
		// Deliberately older than the last_seen_ms already stored.
		const result = await touchProximityEpisode(redis, pairKey, 1_700_000_005_000, 60_000);

		expect(result.isNewEpisode).toBe(false);
		const lastSeen = await redis.hget(`proximity-episode:${pairKey}`, 'last_seen_ms');
		expect(lastSeen).toBe('1700000010000'); // unchanged, not moved backward

		const ttlMs = await redis.pttl(`proximity-episode:${pairKey}`);
		expect(ttlMs).toBeGreaterThan(0); // still renewed despite the out-of-order value
	});

	it('starts a fresh episode once the previous one has expired', async () => {
		const pairKey = testPairKey();
		seededKeys.add(`proximity-episode:${pairKey}`);

		await touchProximityEpisode(redis, pairKey, 1_700_000_000_000, 50); // 50ms gap
		await new Promise((resolve) => setTimeout(resolve, 150));

		const result = await touchProximityEpisode(redis, pairKey, 1_700_000_005_000, 60_000);

		expect(result).toEqual({ isNewEpisode: true, episodeStartMs: 1_700_000_005_000 });
	});
});
