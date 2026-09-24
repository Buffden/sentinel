import { describe, expect, it } from 'vitest';
import { AdsbfiFreshness } from './adsbfiFreshness.js';

const FROZEN_MS = 10_000;
// adsb.fi's `now` has whole-second granularity (observed: ...486000, ...488000).
const NOW = 1790283486000;

describe('AdsbfiFreshness', () => {
	it('seeds on the first response of an acquisition without confirming freshness', () => {
		const f = new AdsbfiFreshness(FROZEN_MS);
		expect(f.observe(NOW, 0)).toBe('seeded');
		expect(f.highestResponseNowMs).toBe(NOW);
	});

	it('confirms freshness only when now is strictly greater than the highest seen', () => {
		const f = new AdsbfiFreshness(FROZEN_MS);
		f.observe(NOW, 0);
		expect(f.observe(NOW + 2_000, 2_000)).toBe('fresh');
		expect(f.observe(NOW + 2_000, 4_000)).toBe('unconfirmed'); // equal is not an advance
		expect(f.observe(NOW + 1_000, 6_000)).toBe('unconfirmed'); // lower is not an advance
		expect(f.highestResponseNowMs).toBe(NOW + 2_000);
	});

	it('resumes fresh when now advances before the frozen-feed window ends', () => {
		const f = new AdsbfiFreshness(FROZEN_MS);
		f.observe(NOW, 0);
		expect(f.observe(NOW, 9_999)).toBe('unconfirmed');
		expect(f.observe(NOW + 1_000, 9_999)).toBe('fresh');
		// The window restarts from that advance.
		expect(f.observe(NOW + 1_000, 19_998)).toBe('unconfirmed');
	});

	it('is frozen once now has not advanced for the whole window, measured on the coordinator clock', () => {
		const f = new AdsbfiFreshness(FROZEN_MS);
		f.observe(NOW, 1_000);
		expect(f.observe(NOW, 10_999)).toBe('unconfirmed');
		expect(f.observe(NOW, 11_000)).toBe('frozen');
		expect(f.staleForMs(11_000)).toBe(10_000);
		// Still frozen until now advances, then fresh again.
		expect(f.observe(NOW, 15_000)).toBe('frozen');
		expect(f.observe(NOW + 1_000, 16_000)).toBe('fresh');
	});

	it('starts empty for a new acquisition', () => {
		const first = new AdsbfiFreshness(FROZEN_MS);
		first.observe(NOW, 0);
		expect(first.observe(NOW + 2_000, 2_000)).toBe('fresh');
		// A new lease holder has seen nothing, so even a newer now only seeds.
		const next = new AdsbfiFreshness(FROZEN_MS);
		expect(next.observe(NOW + 4_000, 4_000)).toBe('seeded');
	});
});
