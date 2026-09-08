import { describe, expect, it } from 'vitest';
import { canonicalPairKey } from './pair.js';

describe('canonicalPairKey', () => {
	it('orders two entity IDs alphabetically', () => {
		expect(canonicalPairKey('a63745', 'aa0001')).toBe('a63745:aa0001');
	});

	it('produces the same key regardless of argument order', () => {
		const ab = canonicalPairKey('a63745', 'aa0001');
		const ba = canonicalPairKey('aa0001', 'a63745');
		expect(ab).toBe(ba);
	});

	it('compares as strings, not numbers', () => {
		// '9' > '10' lexicographically even though 9 < 10 numerically --
		// entity IDs are never parsed as numbers, so this is the expected
		// (if visually surprising) ordering.
		expect(canonicalPairKey('9', '10')).toBe('10:9');
	});

	it('produces different keys for different pairs', () => {
		expect(canonicalPairKey('a63745', 'aa0001')).not.toBe(canonicalPairKey('a63745', 'ab0002'));
	});
});
