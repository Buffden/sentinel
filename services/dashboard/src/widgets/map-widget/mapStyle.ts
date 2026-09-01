// Primary: CARTO Dark Matter — no API key required, high-contrast dark style
// Override with NEXT_PUBLIC_MAP_STYLE for a different provider
export const MAP_STYLE_PRIMARY =
	process.env.NEXT_PUBLIC_MAP_STYLE ??
	'https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json'

// Fallback: OpenFreeMap dark — used if the primary style fails to load
export const MAP_STYLE_FALLBACK = 'https://tiles.openfreemap.org/styles/dark'
