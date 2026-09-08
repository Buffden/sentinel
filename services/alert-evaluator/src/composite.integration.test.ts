// Integration tests for composite.ts's read-only eligibility resolver.
// Run against REAL Redis (docker-compose), not mocks: the guarantee under
// test is "recent-loss's mere existence must not be mistaken for
// eligibility" -- a fake store built from the same assumptions as the
// implementation would not catch a regression back to EXISTS-based logic.
//
// Requires: `make up` (locally) or the CI service containers.
import { randomUUID } from 'node:crypto';
import { afterEach, beforeAll, describe, expect, it } from 'vitest';
import { redis } from './evaluator.js';
import {
	resolveCompositeEligibility,
	resolveEntityLossEpisode,
	selectWinningEpisode,
} from './composite.js';
import type { QualifyingLossEpisode } from './composite.js';

const WINDOW_MS = 60_000;

describe('composite.ts eligibility resolution (integration)', () => {
	beforeAll(async () => {
		await redis.ping();
	});

	describe('resolveEntityLossEpisode — active-dark (alert-state)', () => {
		const entityId = `test-entity-${randomUUID()}`;
		const alertStateKey = `alert-state:${entityId}`;

		afterEach(async () => {
			await redis.del(alertStateKey);
		});

		it('qualifies when composite_issued=0 and the gap is within the window', async () => {
			await redis.hset(
				alertStateKey,
				'dark_since_ms',
				'1700000000000',
				'signal_loss_alert_id',
				`${entityId}:SIGNAL_LOSS:1700000000000`,
				'composite_issued',
				'0',
			);

			const result = await resolveEntityLossEpisode(redis, entityId, 1_700_000_030_000, WINDOW_MS);

			expect(result).toEqual<QualifyingLossEpisode>({
				entity_id: entityId,
				source: 'ACTIVE',
				dark_since_ms: 1700000000000,
				signal_loss_alert_id: `${entityId}:SIGNAL_LOSS:1700000000000`,
				resumed_at_ms: null,
				gap_ms: 30_000,
			});
		});

		it('does not qualify when composite_issued is already 1', async () => {
			await redis.hset(
				alertStateKey,
				'dark_since_ms',
				'1700000000000',
				'signal_loss_alert_id',
				`${entityId}:SIGNAL_LOSS:1700000000000`,
				'composite_issued',
				'1',
			);

			const result = await resolveEntityLossEpisode(redis, entityId, 1_700_000_030_000, WINDOW_MS);
			expect(result).toBeNull();
		});

		it('does not qualify a negative gap (candidate predates the loss)', async () => {
			await redis.hset(
				alertStateKey,
				'dark_since_ms',
				'1700000000000',
				'signal_loss_alert_id',
				`${entityId}:SIGNAL_LOSS:1700000000000`,
				'composite_issued',
				'0',
			);

			const result = await resolveEntityLossEpisode(redis, entityId, 1_699_999_999_000, WINDOW_MS);
			expect(result).toBeNull();
		});

		it('qualifies at exactly the window boundary (inclusive)', async () => {
			await redis.hset(
				alertStateKey,
				'dark_since_ms',
				'1700000000000',
				'signal_loss_alert_id',
				`${entityId}:SIGNAL_LOSS:1700000000000`,
				'composite_issued',
				'0',
			);

			const result = await resolveEntityLossEpisode(
				redis,
				entityId,
				1_700_000_000_000 + WINDOW_MS,
				WINDOW_MS,
			);
			expect(result?.gap_ms).toBe(WINDOW_MS);
		});

		it('does not qualify one millisecond past the window boundary', async () => {
			await redis.hset(
				alertStateKey,
				'dark_since_ms',
				'1700000000000',
				'signal_loss_alert_id',
				`${entityId}:SIGNAL_LOSS:1700000000000`,
				'composite_issued',
				'0',
			);

			const result = await resolveEntityLossEpisode(
				redis,
				entityId,
				1_700_000_000_000 + WINDOW_MS + 1,
				WINDOW_MS,
			);
			expect(result).toBeNull();
		});

		it('does not qualify with a missing signal_loss_alert_id -- CP3 needs it for supersedes_alert_ids', async () => {
			await redis.hset(alertStateKey, 'dark_since_ms', '1700000000000', 'composite_issued', '0');

			const result = await resolveEntityLossEpisode(redis, entityId, 1_700_000_030_000, WINDOW_MS);
			expect(result).toBeNull();
		});
	});

	describe('resolveEntityLossEpisode — recent-loss', () => {
		const entityId = `test-entity-${randomUUID()}`;
		const recentLossKey = `recent-loss:${entityId}`;

		afterEach(async () => {
			await redis.del(recentLossKey);
		});

		it('qualifies within the window and carries resumed_at_ms as evidence', async () => {
			await redis.hset(
				recentLossKey,
				'dark_since_ms',
				'1700000000000',
				'resumed_at_ms',
				'1700000010000',
				'signal_loss_alert_id',
				`${entityId}:SIGNAL_LOSS:1700000000000`,
			);

			const result = await resolveEntityLossEpisode(redis, entityId, 1_700_000_030_000, WINDOW_MS);

			expect(result).toEqual<QualifyingLossEpisode>({
				entity_id: entityId,
				source: 'RECENT',
				dark_since_ms: 1700000000000,
				signal_loss_alert_id: `${entityId}:SIGNAL_LOSS:1700000000000`,
				resumed_at_ms: 1700000010000,
				gap_ms: 30_000,
			});
		});

		// The specific regression this checkpoint exists to prevent: a live,
		// unexpired recent-loss key is not by itself eligibility. The entity
		// was dark for a long stretch before resuming, so the TTL (bounded
		// from resumed_at_ms) is still well within its retention period even
		// though the true source-time gap from dark_since_ms already exceeds
		// the window -- DATA_MODEL.md's "Composite eligibility rule".
		it('does NOT qualify when the key exists with a live TTL but the dark_since_ms gap exceeds the window', async () => {
			const darkSinceMs = 1_700_000_000_000;
			const resumedAtMs = darkSinceMs + 10 * WINDOW_MS; // dark for a long stretch
			await redis.hset(
				recentLossKey,
				'dark_since_ms',
				String(darkSinceMs),
				'resumed_at_ms',
				String(resumedAtMs),
				'signal_loss_alert_id',
				`${entityId}:SIGNAL_LOSS:${darkSinceMs}`,
			);
			await redis.pexpire(recentLossKey, WINDOW_MS); // key is genuinely still alive

			// A candidate arriving shortly after resume: well within the key's
			// TTL, but its gap from dark_since_ms is far beyond the window.
			const candidateEpisodeStartMs = resumedAtMs + 1_000;

			const ttlBefore = await redis.pttl(recentLossKey);
			expect(ttlBefore).toBeGreaterThan(0); // key is genuinely live, not a setup bug

			const result = await resolveEntityLossEpisode(
				redis,
				entityId,
				candidateEpisodeStartMs,
				WINDOW_MS,
			);
			expect(result).toBeNull();
		});

		it('does not qualify with missing/malformed dark_since_ms', async () => {
			await redis.hset(recentLossKey, 'resumed_at_ms', '1700000010000');

			const result = await resolveEntityLossEpisode(redis, entityId, 1_700_000_030_000, WINDOW_MS);
			expect(result).toBeNull();
		});

		it('does not qualify with a missing signal_loss_alert_id -- CP3 needs it for supersedes_alert_ids', async () => {
			await redis.hset(
				recentLossKey,
				'dark_since_ms',
				'1700000000000',
				'resumed_at_ms',
				'1700000010000',
			);

			const result = await resolveEntityLossEpisode(redis, entityId, 1_700_000_030_000, WINDOW_MS);
			expect(result).toBeNull();
		});

		it('does not qualify with a missing/malformed resumed_at_ms -- DATA_MODEL.md defines all three fields together', async () => {
			await redis.hset(
				recentLossKey,
				'dark_since_ms',
				'1700000000000',
				'signal_loss_alert_id',
				`${entityId}:SIGNAL_LOSS:1700000000000`,
			);

			const result = await resolveEntityLossEpisode(redis, entityId, 1_700_000_030_000, WINDOW_MS);
			expect(result).toBeNull();
		});
	});

	describe('resolveCompositeEligibility — both-entities-qualify tie-break', () => {
		const entityA = `test-entity-a-${randomUUID()}`;
		const entityB = `test-entity-b-${randomUUID()}`;

		afterEach(async () => {
			await redis.del(
				`alert-state:${entityA}`,
				`alert-state:${entityB}`,
				`recent-loss:${entityA}`,
				`recent-loss:${entityB}`,
			);
		});

		it('only one entity qualifying returns that episode', async () => {
			await redis.hset(
				`alert-state:${entityA}`,
				'dark_since_ms',
				'1700000000000',
				'signal_loss_alert_id',
				`${entityA}:SIGNAL_LOSS:1700000000000`,
				'composite_issued',
				'0',
			);

			const result = await resolveCompositeEligibility(
				redis,
				entityA,
				entityB,
				1_700_000_010_000,
				WINDOW_MS,
			);
			expect(result?.entity_id).toBe(entityA);
		});

		it('when both qualify, the smaller gap wins', async () => {
			const candidateEpisodeStartMs = 1_700_000_050_000;
			await redis.hset(
				`alert-state:${entityA}`,
				'dark_since_ms',
				'1700000000000', // gap 50_000
				'signal_loss_alert_id',
				`${entityA}:SIGNAL_LOSS:1700000000000`,
				'composite_issued',
				'0',
			);
			await redis.hset(
				`alert-state:${entityB}`,
				'dark_since_ms',
				'1700000030000', // gap 20_000 -- smaller, should win
				'signal_loss_alert_id',
				`${entityB}:SIGNAL_LOSS:1700000030000`,
				'composite_issued',
				'0',
			);

			const result = await resolveCompositeEligibility(
				redis,
				entityA,
				entityB,
				candidateEpisodeStartMs,
				WINDOW_MS,
			);
			expect(result?.entity_id).toBe(entityB);
			expect(result?.gap_ms).toBe(20_000);
		});

		it('an exact gap tie is broken by the lexicographically smaller entity_id', async () => {
			const candidateEpisodeStartMs = 1_700_000_030_000;
			await redis.hset(
				`alert-state:${entityA}`,
				'dark_since_ms',
				'1700000000000',
				'signal_loss_alert_id',
				`${entityA}:SIGNAL_LOSS:1700000000000`,
				'composite_issued',
				'0',
			);
			await redis.hset(
				`alert-state:${entityB}`,
				'dark_since_ms',
				'1700000000000', // identical gap
				'signal_loss_alert_id',
				`${entityB}:SIGNAL_LOSS:1700000000000`,
				'composite_issued',
				'0',
			);

			const result = await resolveCompositeEligibility(
				redis,
				entityA,
				entityB,
				candidateEpisodeStartMs,
				WINDOW_MS,
			);
			const expectedWinner = entityA <= entityB ? entityA : entityB;
			expect(result?.entity_id).toBe(expectedWinner);
		});

		it('neither entity qualifying returns null', async () => {
			const result = await resolveCompositeEligibility(
				redis,
				entityA,
				entityB,
				1_700_000_010_000,
				WINDOW_MS,
			);
			expect(result).toBeNull();
		});

		it('is read-only: Redis state is unchanged after resolution, including the losing episode', async () => {
			await redis.hset(
				`alert-state:${entityA}`,
				'dark_since_ms',
				'1700000000000',
				'signal_loss_alert_id',
				`${entityA}:SIGNAL_LOSS:1700000000000`,
				'composite_issued',
				'0',
			);
			await redis.hset(
				`alert-state:${entityB}`,
				'dark_since_ms',
				'1700000030000',
				'signal_loss_alert_id',
				`${entityB}:SIGNAL_LOSS:1700000030000`,
				'composite_issued',
				'0',
			);

			const before = {
				a: await redis.hgetall(`alert-state:${entityA}`),
				b: await redis.hgetall(`alert-state:${entityB}`),
			};

			const result = await resolveCompositeEligibility(
				redis,
				entityA,
				entityB,
				1_700_000_050_000,
				WINDOW_MS,
			);
			expect(result).not.toBeNull(); // sanity: a real winner was resolved

			const after = {
				a: await redis.hgetall(`alert-state:${entityA}`),
				b: await redis.hgetall(`alert-state:${entityB}`),
			};
			expect(after).toEqual(before);
		});
	});

	describe('selectWinningEpisode — pure tie-break (no Redis)', () => {
		function episode(overrides: Partial<QualifyingLossEpisode>): QualifyingLossEpisode {
			return {
				entity_id: 'e',
				source: 'ACTIVE',
				dark_since_ms: 0,
				signal_loss_alert_id: 'alert',
				resumed_at_ms: null,
				gap_ms: 0,
				...overrides,
			};
		}

		it('returns the only non-null candidate', () => {
			const a = episode({ entity_id: 'a', gap_ms: 10 });
			expect(selectWinningEpisode(a, null)).toBe(a);
			expect(selectWinningEpisode(null, a)).toBe(a);
		});

		it('returns null when neither qualifies', () => {
			expect(selectWinningEpisode(null, null)).toBeNull();
		});

		it('picks the smaller gap_ms', () => {
			const a = episode({ entity_id: 'a', gap_ms: 50 });
			const b = episode({ entity_id: 'b', gap_ms: 10 });
			expect(selectWinningEpisode(a, b)).toBe(b);
		});

		it('breaks an exact tie by the lexicographically smaller entity_id', () => {
			const a = episode({ entity_id: 'zzz', gap_ms: 10 });
			const b = episode({ entity_id: 'aaa', gap_ms: 10 });
			expect(selectWinningEpisode(a, b)).toBe(b);
		});
	});
});
