// Observed silence for signal loss (ADR-022, CP3c).
//
// Wall-clock silence (nowMs - last_seen_ms) cannot tell a dark aircraft from
// an outage of the provider that would have reported it. Observed silence
// counts only the time since last_seen_ms during which that aircraft's own
// provider was proven to be covering: the closed segments in
// {live-provider}:coverage plus the open span in {live-provider}:authority.
//
// The open span ends at last_active_success_ms, not at nowMs. Time after the
// most recent credited cycle is unobserved until a later cycle proves it, so
// a provider that has just failed cannot make its aircraft look dark.
//
// last_seen_ms is source event time and the segments are coordinator
// processing time. ADR-022 accepts that mix: the measured source-to-delivery
// lag is a few seconds against a threshold of minutes, and it can only
// delay an alert, never cause a false one.
//
// Pure: the evaluator reads the timeline once per scan and passes it here.

export interface CoverageInterval {
	startMs: number;
	endMs: number;
}

export interface CoverageSnapshot {
	// false when there is no authority record, or only a heartbeat, or it lacks
	// provider or epoch. No coverage is trusted then, so every silence is 0.
	initialized: boolean;
	timelineVersion: string | null;
	// Per provider: clamped at nowMs, sorted by start, no overlaps.
	intervalsByProvider: Map<string, CoverageInterval[]>;
	malformedMembers: number;
}

const INTEGER_MS = /^\d+$/;

function parseMs(value: string | undefined): number | null {
	if (value === undefined || !INTEGER_MS.test(value)) return null;
	const ms = Number(value);
	return Number.isSafeInteger(ms) ? ms : null;
}

// Member shape written by the coordinator's close script:
// `<provider>|<start_ms>|<end_ms>|<reason>`.
function parseMember(member: string): { provider: string; interval: CoverageInterval } | null {
	const parts = member.split('|');
	if (parts.length !== 4) return null;
	const [provider, start, end, reason] = parts;
	if (!provider || !reason) return null;
	const startMs = parseMs(start);
	const endMs = parseMs(end);
	if (startMs === null || endMs === null || endMs <= startMs) return null;
	return { provider, interval: { startMs, endMs } };
}

// Clamp at nowMs, drop empty intervals, then merge overlapping or touching
// ones. The close script never writes overlaps, but a legacy or hand-edited
// timeline could, and counting the same millisecond twice would make an
// aircraft look darker than it is.
function normalize(intervals: CoverageInterval[], nowMs: number): CoverageInterval[] {
	const clamped = intervals
		.map((i) => ({ startMs: i.startMs, endMs: Math.min(i.endMs, nowMs) }))
		.filter((i) => i.endMs > i.startMs)
		.sort((a, b) => a.startMs - b.startMs);

	const merged: CoverageInterval[] = [];
	for (const interval of clamped) {
		const last = merged[merged.length - 1];
		if (last && interval.startMs <= last.endMs) {
			last.endMs = Math.max(last.endMs, interval.endMs);
		} else {
			merged.push({ ...interval });
		}
	}
	return merged;
}

export function buildCoverageSnapshot(
	authority: Record<string, string>,
	coverageMembers: string[],
	nowMs: number,
): CoverageSnapshot {
	const authorityProvider = authority['provider'];
	const initialized = Boolean(authorityProvider) && Boolean(authority['epoch']);

	const raw = new Map<string, CoverageInterval[]>();
	const add = (provider: string, interval: CoverageInterval) => {
		const list = raw.get(provider) ?? [];
		list.push(interval);
		raw.set(provider, list);
	};

	let malformedMembers = 0;
	for (const member of coverageMembers) {
		const parsed = parseMember(member);
		if (parsed) add(parsed.provider, parsed.interval);
		else malformedMembers++;
	}

	if (initialized && authorityProvider) {
		const openSinceMs = parseMs(authority['coverage_open_since_ms']);
		const lastActiveMs = parseMs(authority['last_active_success_ms']);
		if (openSinceMs !== null && lastActiveMs !== null && lastActiveMs > openSinceMs) {
			add(authorityProvider, { startMs: openSinceMs, endMs: lastActiveMs });
		}
	}

	const intervalsByProvider = new Map<string, CoverageInterval[]>();
	for (const [provider, intervals] of raw) {
		intervalsByProvider.set(provider, normalize(intervals, nowMs));
	}

	return {
		initialized,
		timelineVersion: initialized ? (authority['timeline_version'] ?? null) : null,
		intervalsByProvider,
		malformedMembers,
	};
}

// sum over the provider's intervals of
//   max(0, min(end, nowMs) - max(start, lastSeenMs))
export function observedSilenceMs(
	snapshot: CoverageSnapshot,
	provider: string | undefined,
	lastSeenMs: number,
	nowMs: number,
): number {
	if (!snapshot.initialized || !provider) return 0;
	const intervals = snapshot.intervalsByProvider.get(provider);
	if (!intervals) return 0;

	let total = 0;
	for (const { startMs, endMs } of intervals) {
		total += Math.max(0, Math.min(endMs, nowMs) - Math.max(startMs, lastSeenMs));
	}
	return total;
}
