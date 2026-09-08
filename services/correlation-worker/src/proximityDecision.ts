import type { Redis } from 'ioredis';
import type { Session } from 'neo4j-driver';
import { canonicalPairKey } from './pair.js';
import {
	touchProximityEpisode,
	getCandidatePublishState,
	markCandidatePending,
} from './episode.js';
import { mergeProximityEvent, type ProximityEntity } from './proximityEvent.js';
import { isKnownAssociate } from './knownAssociate.js';

export interface ProximityObservation {
	observedAtMs: number;
	distanceMetres: number;
	lat: number;
	lon: number;
}

export interface ProximityDecision {
	pairKey: string;
	episodeStartMs: number;
	isNewEpisode: boolean;
	isKnownAssociate: boolean;
	shouldPublishCandidate: boolean;
}

// Decides what a qualifying (within-threshold) candidate pair should do:
// record graph evidence always, and publish proximity.candidates at most
// once per unscheduled episode. Does not call Kafka itself -- the caller
// publishes when shouldPublishCandidate is true and must call
// markCandidatePublished afterward to confirm it.
//
// Episode timing (touchProximityEpisode) and graph evidence
// (mergeProximityEvent) are written unconditionally, before the
// known-associate check, so a pair's proximity history is never lost even
// when it turns out to be an expected relationship -- only the *alert* path
// is filtered, not the evidence.
//
// A known associate's episode never gets a candidate_published field at
// all. On a later ping for an existing episode, a missing field is how this
// function recognizes "already established as known" without re-querying
// KNOWN_ASSOCIATE on every confirmation -- only a fresh episode pays that
// cost.
export async function evaluateProximityEncounter(
	redis: Redis,
	session: Session,
	entityA: ProximityEntity,
	entityB: ProximityEntity,
	observation: ProximityObservation,
	gapMs: number,
): Promise<ProximityDecision> {
	const pairKey = canonicalPairKey(entityA.id, entityB.id);

	const episode = await touchProximityEpisode(redis, pairKey, observation.observedAtMs, gapMs);

	await mergeProximityEvent(session, entityA, entityB, {
		episodeStartMs: episode.episodeStartMs,
		lastSeenMs: observation.observedAtMs,
		distanceMetres: observation.distanceMetres,
		lat: observation.lat,
		lon: observation.lon,
	});

	if (episode.isNewEpisode) {
		const knownAssociate = await isKnownAssociate(session, entityA.id, entityB.id);
		if (knownAssociate) {
			return {
				pairKey,
				episodeStartMs: episode.episodeStartMs,
				isNewEpisode: true,
				isKnownAssociate: true,
				shouldPublishCandidate: false,
			};
		}

		await markCandidatePending(redis, pairKey);
		return {
			pairKey,
			episodeStartMs: episode.episodeStartMs,
			isNewEpisode: true,
			isKnownAssociate: false,
			shouldPublishCandidate: true,
		};
	}

	const publishState = await getCandidatePublishState(redis, pairKey);
	return {
		pairKey,
		episodeStartMs: episode.episodeStartMs,
		isNewEpisode: false,
		isKnownAssociate: publishState === null,
		shouldPublishCandidate: publishState === '0',
	};
}
