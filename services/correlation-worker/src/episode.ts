import type { Redis } from 'ioredis';

// Atomic check-and-create-or-refresh: two Correlation Worker instances can
// process A's ping and B's ping for the same pair at nearly the same time,
// so "does an episode exist" and "create/refresh it" must be one Redis
// operation, not a separate EXISTS then HSET that could race.
const TOUCH_EPISODE_SCRIPT = `
	local existingStart = redis.call('HGET', KEYS[1], 'episode_start_ms')
	if existingStart == false then
		redis.call('HSET', KEYS[1], 'episode_start_ms', ARGV[1], 'last_seen_ms', ARGV[1])
		redis.call('PEXPIRE', KEYS[1], ARGV[2])
		return {1, ARGV[1]}
	end
	local existingLastSeen = redis.call('HGET', KEYS[1], 'last_seen_ms')
	if tonumber(ARGV[1]) >= tonumber(existingLastSeen) then
		redis.call('HSET', KEYS[1], 'last_seen_ms', ARGV[1])
	end
	redis.call('PEXPIRE', KEYS[1], ARGV[2])
	return {0, existingStart}
`;

export interface ProximityEpisode {
	isNewEpisode: boolean;
	episodeStartMs: number;
}

// Decides whether observedAtMs belongs to an existing proximity episode for
// pairKey or starts a new one, and (re)applies the gap TTL either way. The
// TTL -- not this function -- is what ends an episode: if no confirming ping
// arrives within gapMs of wall-clock time, Redis expires the key on its own,
// and the next confirmation for this pair starts fresh with a new
// episode_start_ms. A confirmation older than the episode's current
// last_seen_ms still renews the TTL (processing it at all means the
// encounter is evidently still active) but does not move last_seen_ms
// backward.
export async function touchProximityEpisode(
	redis: Redis,
	pairKey: string,
	observedAtMs: number,
	gapMs: number,
): Promise<ProximityEpisode> {
	const [isNew, episodeStartMs] = (await redis.eval(
		TOUCH_EPISODE_SCRIPT,
		1,
		`proximity-episode:${pairKey}`,
		String(observedAtMs),
		String(gapMs),
	)) as [number, string];

	return {
		isNewEpisode: isNew === 1,
		episodeStartMs: Number(episodeStartMs),
	};
}

// candidate_published is written only for unscheduled pairs -- a known
// associate's episode never gets this field at all, so its absence on an
// existing episode is how a later ping recognizes "this pair was already
// established as known" without re-checking KNOWN_ASSOCIATE every time.
export type CandidatePublishState = '0' | '1' | null;

export async function getCandidatePublishState(
	redis: Redis,
	pairKey: string,
): Promise<CandidatePublishState> {
	const value = await redis.hget(`proximity-episode:${pairKey}`, 'candidate_published');
	return value as CandidatePublishState;
}

// Set before attempting the Kafka publish. If the process crashes or the
// publish fails before markCandidatePublished runs, this stays '0' and the
// next qualifying ping for the same episode retries -- see
// getCandidatePublishState's use in the caller.
export async function markCandidatePending(redis: Redis, pairKey: string): Promise<void> {
	await redis.hset(`proximity-episode:${pairKey}`, 'candidate_published', '0');
}

// Set only after the Kafka publish is confirmed sent.
export async function markCandidatePublished(redis: Redis, pairKey: string): Promise<void> {
	await redis.hset(`proximity-episode:${pairKey}`, 'candidate_published', '1');
}
