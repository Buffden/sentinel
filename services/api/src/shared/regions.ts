// Predefined geographic regions for workspace scope selection (ADR-012).
// A static list, not a database table -- no external geocoder in v1.
// The exact list and precision of bounding boxes is a CP1 implementation
// choice, not an architectural one; add regions here as operators need them.

export interface GeoBounds {
	min_lat: number;
	max_lat: number;
	min_lon: number;
	max_lon: number;
}

export interface PredefinedRegion {
	name: string;
	bounds: GeoBounds;
}

export const PREDEFINED_REGIONS: readonly PredefinedRegion[] = [
	{ name: 'Global', bounds: { min_lat: -90, max_lat: 90, min_lon: -180, max_lon: 180 } },
	{ name: 'France', bounds: { min_lat: 41.3, max_lat: 51.1, min_lon: -5.2, max_lon: 9.6 } },
	{
		name: 'United Kingdom',
		bounds: { min_lat: 49.9, max_lat: 60.9, min_lon: -8.6, max_lon: 1.8 },
	},
	{
		name: 'Western Europe',
		bounds: { min_lat: 36.0, max_lat: 71.0, min_lon: -10.0, max_lon: 30.0 },
	},
	{
		name: 'United States',
		bounds: { min_lat: 24.5, max_lat: 49.4, min_lon: -125.0, max_lon: -66.9 },
	},
];

export function findPredefinedRegion(name: string): PredefinedRegion | undefined {
	return PREDEFINED_REGIONS.find((region) => region.name === name);
}

// One bounds-comparison definition, shared by alertScopeFilter.ts and
// entityScopeFilter.ts -- both need the same inclusive lat/lon rectangle
// check against a workspace scope's geo_region.bounds.
export function withinBounds(pos: { lat: number; lon: number }, bounds: GeoBounds): boolean {
	return (
		pos.lat >= bounds.min_lat &&
		pos.lat <= bounds.max_lat &&
		pos.lon >= bounds.min_lon &&
		pos.lon <= bounds.max_lon
	);
}
