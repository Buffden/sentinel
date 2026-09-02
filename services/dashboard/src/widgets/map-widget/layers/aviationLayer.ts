import { IconLayer } from '@deck.gl/layers'
import type { MapLayerDefinition } from '../types'

export interface AircraftPosition {
	id: string
	lon: number
	lat: number
	// Degrees clockwise from north. Null when unknown — icon renders pointing north.
	courseDeg: number | null
}

// Placeholder — altitude range, airborne/ground toggle, subtype checkboxes added in later checkpoints
export type AviationFilters = Record<string, never>

// Single-icon atlas: the SVG occupies the full 64×64 canvas.
// mask:true lets getColor tint the white silhouette; transparent pixels stay transparent.
const ICON_MAPPING = {
	aircraft: { x: 0, y: 0, width: 64, height: 64, mask: true },
} as const

export const aviationLayer: MapLayerDefinition<AircraftPosition[], AviationFilters> = {
	id: 'aviation-positions',
	label: 'Aircraft',
	enabledByDefault: true,

	createLayer(data) {
		return new IconLayer<AircraftPosition>({
			id: 'aviation-positions',
			data,
			iconAtlas: '/aircraft-icon.svg',
			iconMapping: ICON_MAPPING,
			getIcon: () => 'aircraft',
			getPosition: (d) => [d.lon, d.lat],
			// deck.gl rotates counterclockwise from north; course_deg is clockwise from north.
			// Negating converts: course_deg=90 (east) → getAngle=-90 (clockwise = right).
			getAngle: (d) => -(d.courseDeg ?? 0),
			getSize: 24,
			getColor: [59, 130, 246, 220],
			pickable: true,
		})
	},
}
