// Unit tests for observed silence (ADR-022, CP3c). Pure: no Redis or Kafka.
//
// Timestamps are small offsets so each expected value can be checked by hand.
import { describe, expect, it } from 'vitest';
import { buildCoverageSnapshot, observedSilenceMs } from './signalLossCoverage.js';

const NOW = 10_000;

// An initialized authority record whose coverage is closed, so only the
// coverage members passed alongside it count.
const closedAuthority = {
	provider: 'adsbfi',
	epoch: '1',
	authority_since_ms: '0',
	coverage_open_since_ms: '',
	last_active_success_ms: '9000',
	timeline_version: '3',
};

function silence(members: string[], provider: string | undefined, lastSeenMs: number): number {
	const snapshot = buildCoverageSnapshot(closedAuthority, members, NOW);
	return observedSilenceMs(snapshot, provider, lastSeenMs, NOW);
}

describe('observedSilenceMs', () => {
	it('is zero with no coverage', () => {
		expect(silence([], 'adsbfi', 1_000)).toBe(0);
	});

	it('ignores a segment that ended before last_seen_ms', () => {
		expect(silence(['adsbfi|1000|2000|failure'], 'adsbfi', 3_000)).toBe(0);
	});

	it('counts only the part of a straddling segment after last_seen_ms', () => {
		expect(silence(['adsbfi|1000|5000|failure'], 'adsbfi', 3_000)).toBe(2_000);
	});

	it('counts the whole of a segment after last_seen_ms', () => {
		expect(silence(['adsbfi|4000|6000|failure'], 'adsbfi', 3_000)).toBe(2_000);
	});

	it('does not count the gap between two segments', () => {
		const members = ['adsbfi|2000|4000|failure', 'adsbfi|6000|9000|coordinator_shutdown'];
		// 3000..4000 plus 6000..9000; 4000..6000 was not observed.
		expect(silence(members, 'adsbfi', 3_000)).toBe(4_000);
	});

	it("counts only the entity's own provider", () => {
		const members = ['opensky|1000|9000|failure', 'adsbfi|5000|6000|failure'];
		expect(silence(members, 'adsbfi', 1_000)).toBe(1_000);
		expect(silence(members, 'opensky', 1_000)).toBe(8_000);
	});

	it('counts the open span from coverage_open_since_ms to last_active_success_ms', () => {
		const authority = {
			...closedAuthority,
			coverage_open_since_ms: '6000',
			last_active_success_ms: '9000',
		};
		const snapshot = buildCoverageSnapshot(authority, ['adsbfi|2000|4000|failure'], NOW);
		// 3000..4000 closed plus 6000..9000 open. 9000..NOW is not yet proven.
		expect(observedSilenceMs(snapshot, 'adsbfi', 3_000, NOW)).toBe(4_000);
	});

	it("does not give the open span to another provider's entity", () => {
		const authority = {
			...closedAuthority,
			coverage_open_since_ms: '6000',
			last_active_success_ms: '9000',
		};
		const snapshot = buildCoverageSnapshot(authority, [], NOW);
		expect(observedSilenceMs(snapshot, 'opensky', 3_000, NOW)).toBe(0);
	});

	it('ignores an open span that is not a positive interval', () => {
		const authority = {
			...closedAuthority,
			coverage_open_since_ms: '9000',
			last_active_success_ms: '9000',
		};
		const snapshot = buildCoverageSnapshot(authority, [], NOW);
		expect(observedSilenceMs(snapshot, 'adsbfi', 3_000, NOW)).toBe(0);
	});

	it('clamps segment ends at nowMs', () => {
		// A segment ending after nowMs (clock skew between hosts) is cut at nowMs.
		expect(silence(['adsbfi|8000|15000|failure'], 'adsbfi', 3_000)).toBe(2_000);
	});

	it('merges overlapping and touching segments instead of double counting', () => {
		const members = [
			'adsbfi|2000|6000|failure',
			'adsbfi|4000|7000|failure', // overlaps the first
			'adsbfi|7000|8000|coordinator_down', // touches the second
		];
		// One merged interval 2000..8000, of which 3000..8000 follows last_seen.
		expect(silence(members, 'adsbfi', 3_000)).toBe(5_000);
	});

	it('ignores malformed members and counts them', () => {
		const members = [
			'adsbfi|4000|6000|failure',
			'garbage',
			'adsbfi|abc|6000|failure',
			'adsbfi|7000|5000|failure', // end before start
			'|4000|6000|failure', // no provider
			'adsbfi|4000|6000', // no reason
		];
		const snapshot = buildCoverageSnapshot(closedAuthority, members, NOW);
		expect(snapshot.malformedMembers).toBe(5);
		expect(observedSilenceMs(snapshot, 'adsbfi', 3_000, NOW)).toBe(2_000);
	});

	it('is zero for an entity with no provider', () => {
		const members = ['adsbfi|1000|9000|failure'];
		expect(silence(members, undefined, 1_000)).toBe(0);
		expect(silence(members, '', 1_000)).toBe(0);
	});

	it('counts only retained coverage when last_seen_ms is older than all of it', () => {
		const members = ['adsbfi|5000|6000|failure', 'adsbfi|7000|9000|failure'];
		expect(silence(members, 'adsbfi', 0)).toBe(3_000);
	});
});

describe('buildCoverageSnapshot', () => {
	it('treats a missing authority record as uninitialized', () => {
		const snapshot = buildCoverageSnapshot({}, ['adsbfi|1000|9000|failure'], NOW);
		expect(snapshot.initialized).toBe(false);
		expect(observedSilenceMs(snapshot, 'adsbfi', 1_000, NOW)).toBe(0);
	});

	it('treats a heartbeat-only authority record as uninitialized', () => {
		const snapshot = buildCoverageSnapshot({ heartbeat_ms: '9000' }, [], NOW);
		expect(snapshot.initialized).toBe(false);
	});

	it('treats an authority record missing provider or epoch as uninitialized', () => {
		const noProvider = { ...closedAuthority, provider: '' };
		const { epoch: _epoch, ...noEpoch } = closedAuthority;
		expect(buildCoverageSnapshot(noProvider, [], NOW).initialized).toBe(false);
		expect(buildCoverageSnapshot(noEpoch, [], NOW).initialized).toBe(false);
	});

	it('reports the timeline version of an initialized record', () => {
		const snapshot = buildCoverageSnapshot(closedAuthority, [], NOW);
		expect(snapshot.initialized).toBe(true);
		expect(snapshot.timelineVersion).toBe('3');
	});
});
