// adsb.fi feed freshness (ADR-022 section 5).
//
// Coverage claims that adsb.fi was delivering current observations. A valid
// response whose `now` has not moved proves nothing new, so it may be
// published but never extends coverage. adsb.fi's `now` has whole-second
// granularity, so two responses a second apart can legitimately repeat it;
// only a `now` stuck for the frozen-feed window fails the cycle.
//
// The tracker lives in memory for one lease acquisition and is never written
// to Redis. A new lease holder starts empty, so it must see `now` advance
// before it credits any coverage. That costs at most one uncredited cycle
// after each acquisition, which is the conservative direction.

export type FreshnessVerdict =
	// First valid response of this acquisition: the baseline, not credited.
	| 'seeded'
	// `now` is strictly greater than any value seen: credit after publishing.
	| 'fresh'
	// `now` has not advanced, for less than the frozen-feed window: publish,
	// but do not credit.
	| 'unconfirmed'
	// `now` has not advanced for the whole window: the cycle fails.
	| 'frozen';

export class AdsbfiFreshness {
	private highestNowMs: number | null = null;
	// Coordinator processing time at which highestNowMs last advanced.
	private lastAdvanceAtMs = 0;

	constructor(private readonly frozenFeedMs: number) {}

	observe(responseNowMs: number, processingNowMs: number): FreshnessVerdict {
		if (this.highestNowMs === null) {
			this.highestNowMs = responseNowMs;
			this.lastAdvanceAtMs = processingNowMs;
			return 'seeded';
		}
		if (responseNowMs > this.highestNowMs) {
			this.highestNowMs = responseNowMs;
			this.lastAdvanceAtMs = processingNowMs;
			return 'fresh';
		}
		return this.staleForMs(processingNowMs) >= this.frozenFeedMs ? 'frozen' : 'unconfirmed';
	}

	staleForMs(processingNowMs: number): number {
		return processingNowMs - this.lastAdvanceAtMs;
	}

	get highestResponseNowMs(): number | null {
		return this.highestNowMs;
	}
}
