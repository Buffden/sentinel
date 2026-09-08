import { describe, expect, it } from 'vitest';
import { computeMidpoint } from './midpoint.js';

describe('computeMidpoint', () => {
	it('averages two positions', () => {
		const result = computeMidpoint(37.0, -121.0, 37.002, -121.004);
		expect(result.lat).toBeCloseTo(37.001, 9);
		expect(result.lon).toBeCloseTo(-121.002, 9);
	});

	it('returns the same point when both positions are identical', () => {
		expect(computeMidpoint(37.0, -121.0, 37.0, -121.0)).toEqual({ lat: 37.0, lon: -121.0 });
	});
});
