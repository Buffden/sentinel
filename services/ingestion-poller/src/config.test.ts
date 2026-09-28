import { describe, expect, it } from 'vitest';
import { validateFailbackTiming, validateLeaseTiming } from './config.js';

// The coordinator must notice a lost lease before the key can expire:
// renewal interval + 2 x Redis command timeout < lease TTL (see config.ts).
describe('validateLeaseTiming', () => {
	it('accepts the ADR-022 defaults: 5 s + 2 x 2 s < 15 s', () => {
		expect(() => validateLeaseTiming(15_000, 5_000, 2_000)).not.toThrow();
	});

	it('rejects a timing that only the old interval + timeout check accepted', () => {
		// 5 s + 4 s < 10 s passes the weaker check, but the real worst case is
		// 5 s + 2 x 4 s = 13 s, past the 10 s expiry.
		expect(() => validateLeaseTiming(10_000, 5_000, 4_000)).toThrow(/must be less than/);
	});

	it('rejects the equality boundary', () => {
		expect(() => validateLeaseTiming(15_000, 5_000, 5_000)).toThrow(/must be less than/);
	});

	it('accepts one millisecond below the boundary', () => {
		expect(() => validateLeaseTiming(15_001, 5_000, 5_000)).not.toThrow();
	});
});

// A failed failback must wait longer than OpenSky's active interval, or the
// next attempt can cancel OpenSky's resumed cycle every time (see config.ts).
describe('validateFailbackTiming', () => {
	it('accepts the defaults: 60 s base and 15 min max, both > 25 s OpenSky interval', () => {
		expect(() => validateFailbackTiming(60_000, 900_000, 25_000)).not.toThrow();
	});

	it('rejects a base shorter than the OpenSky interval', () => {
		expect(() => validateFailbackTiming(10_000, 900_000, 25_000)).toThrow(
			/must both be greater than/,
		);
	});

	it('rejects a base equal to the OpenSky interval', () => {
		expect(() => validateFailbackTiming(25_000, 900_000, 25_000)).toThrow(
			/must both be greater than/,
		);
	});

	it('rejects a max below the OpenSky interval, which caps every retry', () => {
		expect(() => validateFailbackTiming(60_000, 10_000, 25_000)).toThrow(
			/must both be greater than/,
		);
	});

	it('rejects a max equal to the OpenSky interval', () => {
		expect(() => validateFailbackTiming(60_000, 25_000, 25_000)).toThrow(
			/must both be greater than/,
		);
	});

	it('accepts base and max one millisecond above the boundary', () => {
		expect(() => validateFailbackTiming(25_001, 25_001, 25_000)).not.toThrow();
	});
});
