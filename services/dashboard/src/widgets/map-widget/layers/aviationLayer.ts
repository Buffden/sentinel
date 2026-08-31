import { ScatterplotLayer } from '@deck.gl/layers'
import type { MapLayerDefinition } from '../types'

export interface AircraftPosition {
	id: string
	lon: number
	lat: number
}

// Placeholder — altitude range, airborne/ground toggle, subtype checkboxes added in later checkpoints
export type AviationFilters = Record<string, never>

export const aviationLayer: MapLayerDefinition<AircraftPosition[], AviationFilters> = {
	id: 'aviation-positions',
	label: 'Aircraft',
	enabledByDefault: true,

	createLayer(data) {
		return new ScatterplotLayer<AircraftPosition>({
			id: 'aviation-positions',
			data,
			getPosition: (d) => [d.lon, d.lat],
			getRadius: 12000,
			radiusUnits: 'meters',
			getFillColor: [59, 130, 246, 220],
			stroked: true,
			getLineColor: [147, 197, 253, 180],
			getLineWidth: 2000,
			lineWidthUnits: 'meters',
			pickable: true,
		})
	},
}
