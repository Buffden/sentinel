import { gridDisk } from 'h3-js';
import type { Redis } from 'ioredis';

// Candidate RECALL, not proximity detection: false positives are cheap (the
// caller's exact-distance check discards them), false negatives are not --
// a pair this function never returns is never checked at all.
//
// Same-cell lookup alone is insufficient: two entities can sit on opposite
// sides of an H3 cell boundary, metres apart, in different cells. gridDisk
// returns the origin cell plus all cells within k grid steps, so k >= 1
// also covers immediate neighbors.
//
// Holds no episode state and does no distance calculation -- both belong
// to the caller.

// Candidates are geo-cell:{cell} members within k grid steps of liveGeoCell
// scored (last_seen_ms) at or above minLastSeenMs; stale members are
// excluded the same way the signal-loss scan excludes silent entities.
// Excludes entityId itself.
export async function findProximityCandidates(
	redis: Redis,
	entityId: string,
	liveGeoCell: string,
	k: number,
	minLastSeenMs: number,
): Promise<string[]> {
	const ring = gridDisk(liveGeoCell, k);

	const candidates = new Set<string>();
	for (const cell of ring) {
		const members = await redis.zrangebyscore(`geo-cell:${cell}`, minLastSeenMs, '+inf');
		for (const member of members) {
			if (member !== entityId) candidates.add(member);
		}
	}

	return [...candidates].sort();
}
