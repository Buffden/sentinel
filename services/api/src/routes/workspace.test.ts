// Unit tests for the pure validation/resolution functions behind PUT
// /users/me/workspace. No database, no HTTP -- see workspace.integration.test.ts
// for the full request/response cycle against real Postgres.
import { describe, expect, it } from 'vitest';
import { isSubsetOfAllowed, resolveGeoRegion } from './workspace.js';

describe('resolveGeoRegion', () => {
	it('resolves a known predefined region to the catalog bounds, ignoring any client-sent bounds', () => {
		const region = resolveGeoRegion({
			name: 'France',
			bounds: { min_lat: 0, max_lat: 1, min_lon: 0, max_lon: 1 },
		});

		expect(region).toEqual({
			name: 'France',
			bounds: { min_lat: 41.3, max_lat: 51.1, min_lon: -5.2, max_lon: 9.6 },
		});
	});

	it('rejects a name that is not in the predefined catalog', () => {
		expect(resolveGeoRegion({ name: 'Atlantis' })).toBeNull();
	});

	it('accepts a custom region (name: null) with valid client-supplied bounds', () => {
		const region = resolveGeoRegion({
			name: null,
			bounds: { min_lat: 10, max_lat: 20, min_lon: 10, max_lon: 20 },
		});

		expect(region).toEqual({
			name: null,
			bounds: { min_lat: 10, max_lat: 20, min_lon: 10, max_lon: 20 },
		});
	});

	it('rejects a custom region with an inverted box (min >= max)', () => {
		expect(
			resolveGeoRegion({
				name: null,
				bounds: { min_lat: 20, max_lat: 10, min_lon: 0, max_lon: 1 },
			}),
		).toBeNull();
	});

	it('rejects a custom region with an out-of-range latitude', () => {
		expect(
			resolveGeoRegion({
				name: null,
				bounds: { min_lat: -95, max_lat: 10, min_lon: 0, max_lon: 1 },
			}),
		).toBeNull();
	});

	it('rejects a custom region missing bounds entirely', () => {
		expect(resolveGeoRegion({ name: null })).toBeNull();
	});

	it('rejects a non-object body', () => {
		expect(resolveGeoRegion(null)).toBeNull();
		expect(resolveGeoRegion('France')).toBeNull();
	});
});

describe('isSubsetOfAllowed', () => {
	const ALLOWED = ['aircraft'] as const;

	it('accepts a non-empty subset of the allowed values', () => {
		expect(isSubsetOfAllowed(['aircraft'], ALLOWED)).toBe(true);
	});

	it('rejects an empty array', () => {
		expect(isSubsetOfAllowed([], ALLOWED)).toBe(false);
	});

	it('rejects a value outside the allowed set', () => {
		expect(isSubsetOfAllowed(['aircraft', 'vessel'], ALLOWED)).toBe(false);
	});

	it('rejects a non-array', () => {
		expect(isSubsetOfAllowed('aircraft', ALLOWED)).toBe(false);
	});
});
