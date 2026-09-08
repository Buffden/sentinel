import { gridDisk } from 'h3-js';
import type { Redis } from 'ioredis';

// H3 proximity candidate lookup
//
// Purpose: cheaply narrow "who might possibly be close to this entity" down
// from every live entity to a small candidate set, using the H3 spatial index
// the Position Consumer already maintains in Redis. This is candidate RECALL,
// not proximity detection -- false positives here are expected and cheap
// (the caller's exact-distance check throws them out); false negatives are
// unrecoverable, because a pair this function never returns is never checked
// for exact distance at all.
//
// Same-cell lookup alone is insufficient: two entities can be on opposite
// sides of an H3 cell boundary, physically metres apart, while belonging to
// different cells. gridDisk(cell, k) returns the origin cell plus all cells
// within k grid steps, so a k >= 1 search also covers immediate neighbors.
//
// This function holds no episode state and makes no distance calculation --
// both are the caller's responsibility (Phase 05 CP2 onward).

// Returns candidate entity IDs near `liveGeoCell`, excluding `entityId` itself.
// A candidate qualifies if it is a member of any geo-cell:{cell} sorted set
// within `k` grid steps of `liveGeoCell`, with score (last_seen_ms) at or
// above `minLastSeenMs`. Members below that score are stale live-state and
// are excluded the same way the Alert Evaluator's signal-loss scan excludes
// entities with no recent position.
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
