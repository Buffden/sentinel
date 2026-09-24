import { describe, expect, it } from 'vitest';
import { validateLeaseTiming } from './config.js';

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
