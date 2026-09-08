// Arithmetic mean of the two positions -- a reasonable approximation at
// proximity-detection scale (up to a few km), not a true spherical midpoint.
// The exact geodesic midpoint isn't worth the extra complexity for a value
// that only locates an encounter on the map, not for distance calculation.
export function computeMidpoint(
	latA: number,
	lonA: number,
	latB: number,
	lonB: number,
): { lat: number; lon: number } {
	return { lat: (latA + latB) / 2, lon: (lonA + lonB) / 2 };
}
