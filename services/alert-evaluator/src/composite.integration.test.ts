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
	claimCompositeEpisode,
	finalizeCompositeEpisode,
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

// CP3B: claimCompositeEpisode / finalizeCompositeEpisode. Real Redis, not
// mocks -- the guarantee under test is representation-independence (a claim
// must survive the episode moving from alert-state to recent-loss) and real
// concurrent-caller exclusivity, neither of which a fake store built from
// the same assumptions as the implementation would actually prove.
//
// No Kafka anywhere in this suite -- these primitives are coordination only.
describe('composite.ts claim/finalize primitives (integration)', () => {
	const entityId = `test-entity-${randomUUID()}`;
	const alertStateKey = `alert-state:${entityId}`;
	const recentLossKey = `recent-loss:${entityId}`;
	const darkSinceMs = 1_700_000_000_000;

	afterEach(async () => {
		await redis.del(alertStateKey, recentLossKey);
	});

	function seedActive(overrides: Partial<Record<string, string>> = {}): Promise<unknown> {
		return redis.hset(alertStateKey, {
			dark_since_ms: String(darkSinceMs),
			signal_loss_alert_id: `${entityId}:SIGNAL_LOSS:${darkSinceMs}`,
			composite_issued: '0',
			...overrides,
		});
	}

	function seedRecent(overrides: Partial<Record<string, string>> = {}): Promise<unknown> {
		return redis.hset(recentLossKey, {
			dark_since_ms: String(darkSinceMs),
			resumed_at_ms: String(darkSinceMs + 10_000),
			signal_loss_alert_id: `${entityId}:SIGNAL_LOSS:${darkSinceMs}`,
			composite_issued: '0',
			...overrides,
		});
	}

	// Simulates CP3A's real atomic Lua transfer (already verified for its own
	// correctness in position-consumer's consumer.integration.test.ts) using
	// plain commands, so these tests can isolate CP3B's own guarantee:
	// finding the episode regardless of which representation currently holds
	// it. Mirrors the same simulate-the-upstream-effect pattern already used
	// elsewhere in this file (see the SIGNAL_LOSS episode idempotency suite's
	// "clear the episode gate" comment).
	async function simulateCp3aHandoff(): Promise<void> {
		const fields = await redis.hgetall(alertStateKey);
		await redis.hset(recentLossKey, {
			dark_since_ms: fields['dark_since_ms'] ?? '',
			resumed_at_ms: String(darkSinceMs + 10_000),
			signal_loss_alert_id: fields['signal_loss_alert_id'] ?? '',
			composite_issued: fields['composite_issued'] ?? '0',
			composite_claim_candidate_id: fields['composite_claim_candidate_id'] ?? '',
		});
		await redis.del(alertStateKey);
	}

	describe('claimCompositeEpisode', () => {
		it('claims an active episode with no prior claim', async () => {
			await seedActive();
			const candidateId = 'a:b:1700000030000';

			const claimed = await claimCompositeEpisode(redis, entityId, darkSinceMs, candidateId);
			expect(claimed).toBe(true);

			const state = await redis.hgetall(alertStateKey);
			expect(state['composite_claim_candidate_id']).toBe(candidateId);
		});

		it('claims a recent episode with no prior claim', async () => {
			await seedRecent();
			const candidateId = 'a:b:1700000030000';

			const claimed = await claimCompositeEpisode(redis, entityId, darkSinceMs, candidateId);
			expect(claimed).toBe(true);

			const state = await redis.hgetall(recentLossKey);
			expect(state['composite_claim_candidate_id']).toBe(candidateId);
		});

		it('the same candidate calling CLAIM twice succeeds both times', async () => {
			await seedActive();
			const candidateId = 'a:b:1700000030000';

			expect(await claimCompositeEpisode(redis, entityId, darkSinceMs, candidateId)).toBe(true);
			expect(await claimCompositeEpisode(redis, entityId, darkSinceMs, candidateId)).toBe(true);

			const state = await redis.hgetall(alertStateKey);
			expect(state['composite_claim_candidate_id']).toBe(candidateId);
		});

		it('a different candidate is rejected once the episode is claimed', async () => {
			await seedActive();
			const first = 'a:b:1700000030000';
			const second = 'c:d:1700000031000';

			expect(await claimCompositeEpisode(redis, entityId, darkSinceMs, first)).toBe(true);
			expect(await claimCompositeEpisode(redis, entityId, darkSinceMs, second)).toBe(false);

			const state = await redis.hgetall(alertStateKey);
			expect(state['composite_claim_candidate_id']).toBe(first);
		});

		it('rejects a mismatched dark_since_ms and leaves state unchanged', async () => {
			await seedActive();
			const before = await redis.hgetall(alertStateKey);

			const claimed = await claimCompositeEpisode(
				redis,
				entityId,
				darkSinceMs + 1,
				'a:b:1700000030000',
			);
			expect(claimed).toBe(false);

			const after = await redis.hgetall(alertStateKey);
			expect(after).toEqual(before);
		});

		it('rejects an episode that is already composite_issued=1', async () => {
			await seedActive({ composite_issued: '1' });

			const claimed = await claimCompositeEpisode(
				redis,
				entityId,
				darkSinceMs,
				'a:b:1700000030000',
			);
			expect(claimed).toBe(false);
		});

		it('finds the episode in recent-loss when CP3A handed it off before CLAIM ran', async () => {
			// This is the scenario CP2's snapshot vs. CP3B's claim call must
			// survive: CP2 observed alert-state, but by the time CLAIM runs the
			// episode has already moved to recent-loss.
			await seedActive();
			await simulateCp3aHandoff();
			const candidateId = 'a:b:1700000030000';

			const claimed = await claimCompositeEpisode(redis, entityId, darkSinceMs, candidateId);
			expect(claimed).toBe(true);

			expect(await redis.exists(alertStateKey)).toBe(0);
			const state = await redis.hgetall(recentLossKey);
			expect(state['composite_claim_candidate_id']).toBe(candidateId);
		});

		it('two concurrent different-candidate CLAIM calls: exactly one succeeds', async () => {
			await seedActive();
			const candidateA = 'a:b:1700000030000';
			const candidateB = 'c:d:1700000031000';

			const [resultA, resultB] = await Promise.all([
				claimCompositeEpisode(redis, entityId, darkSinceMs, candidateA),
				claimCompositeEpisode(redis, entityId, darkSinceMs, candidateB),
			]);

			expect([resultA, resultB].filter(Boolean)).toHaveLength(1);

			const state = await redis.hgetall(alertStateKey);
			const winner = resultA ? candidateA : candidateB;
			expect(state['composite_claim_candidate_id']).toBe(winner);
		});

		it('does not touch recent-loss TTL', async () => {
			await seedRecent();
			await redis.pexpire(recentLossKey, 60_000);
			const ttlBefore = await redis.pttl(recentLossKey);

			await claimCompositeEpisode(redis, entityId, darkSinceMs, 'a:b:1700000030000');

			const ttlAfter = await redis.pttl(recentLossKey);
			expect(ttlAfter).toBeGreaterThan(0);
			expect(ttlAfter).toBeLessThanOrEqual(ttlBefore);
		});
	});

	describe('finalizeCompositeEpisode', () => {
		it('fails when there is no prior claim', async () => {
			await seedActive();

			const finalized = await finalizeCompositeEpisode(
				redis,
				entityId,
				darkSinceMs,
				'a:b:1700000030000',
			);
			expect(finalized).toBe(false);

			const state = await redis.hgetall(alertStateKey);
			expect(state['composite_issued']).toBe('0');
		});

		it('fails for a candidate that does not hold the claim', async () => {
			await seedActive();
			const claimant = 'a:b:1700000030000';
			const impostor = 'c:d:1700000031000';
			await claimCompositeEpisode(redis, entityId, darkSinceMs, claimant);

			const finalized = await finalizeCompositeEpisode(redis, entityId, darkSinceMs, impostor);
			expect(finalized).toBe(false);

			const state = await redis.hgetall(alertStateKey);
			expect(state['composite_issued']).toBe('0');
		});

		it('succeeds for the candidate that holds the claim', async () => {
			await seedActive();
			const candidateId = 'a:b:1700000030000';
			await claimCompositeEpisode(redis, entityId, darkSinceMs, candidateId);

			const finalized = await finalizeCompositeEpisode(redis, entityId, darkSinceMs, candidateId);
			expect(finalized).toBe(true);

			const state = await redis.hgetall(alertStateKey);
			expect(state['composite_issued']).toBe('1');
		});

		it('is idempotent: the same candidate finalizing twice succeeds both times', async () => {
			await seedActive();
			const candidateId = 'a:b:1700000030000';
			await claimCompositeEpisode(redis, entityId, darkSinceMs, candidateId);

			expect(await finalizeCompositeEpisode(redis, entityId, darkSinceMs, candidateId)).toBe(true);
			expect(await finalizeCompositeEpisode(redis, entityId, darkSinceMs, candidateId)).toBe(true);

			const state = await redis.hgetall(alertStateKey);
			expect(state['composite_issued']).toBe('1');
		});

		it('finds and finalizes the episode after it moved to recent-loss between CLAIM and FINALIZE', async () => {
			await seedActive();
			const candidateId = 'a:b:1700000030000';
			expect(await claimCompositeEpisode(redis, entityId, darkSinceMs, candidateId)).toBe(true);

			await simulateCp3aHandoff();

			const finalized = await finalizeCompositeEpisode(redis, entityId, darkSinceMs, candidateId);
			expect(finalized).toBe(true);

			expect(await redis.exists(alertStateKey)).toBe(0);
			const state = await redis.hgetall(recentLossKey);
			expect(state['composite_issued']).toBe('1');
			expect(state['composite_claim_candidate_id']).toBe(candidateId);
		});

		it('does not touch recent-loss TTL', async () => {
			await seedRecent({ composite_claim_candidate_id: 'a:b:1700000030000' });
			await redis.pexpire(recentLossKey, 60_000);
			const ttlBefore = await redis.pttl(recentLossKey);

			await finalizeCompositeEpisode(redis, entityId, darkSinceMs, 'a:b:1700000030000');

			const ttlAfter = await redis.pttl(recentLossKey);
			expect(ttlAfter).toBeGreaterThan(0);
			expect(ttlAfter).toBeLessThanOrEqual(ttlBefore);
		});
	});
});
