import { IconLayer } from '@deck.gl/layers'
import type { MapLayerDefinition } from '../types'

export interface AircraftPosition {
	id: string
	lon: number
	lat: number
	// Degrees clockwise from north. Null when unknown — icon renders pointing north.
	courseDeg: number | null
	callsign: string | null
	onGround: boolean | null
}

export type AviationStatusFilter = 'all' | 'airborne' | 'grounded'

export interface AviationFilters {
	// Case-insensitive substring match against callsign. Empty = no filtering.
	// Non-matching aircraft are DIMMED, not removed — the operator can still
	// see where they are; the search highlights what matches.
	callsignSearch: string
	// Airborne/grounded is a hard filter: non-matching aircraft are excluded
	// from the layer's data entirely, unlike the search dim above.
	status: AviationStatusFilter
}

export const DEFAULT_AVIATION_FILTERS: AviationFilters = {
	callsignSearch: '',
	status: 'all',
}

// Exported so MapWidget can compute the same "N of M shown" count the
// panel displays without duplicating the filter rule.
export function matchesStatus(d: AircraftPosition, status: AviationStatusFilter): boolean {
	if (status === 'airborne') return d.onGround !== true
	if (status === 'grounded') return d.onGround === true
	return true
}

function matchesSearch(d: AircraftPosition, search: string): boolean {
	if (search === '') return true
	return (d.callsign ?? '').toLowerCase().includes(search.toLowerCase())
}

// Single-icon atlas: the SVG occupies the full 64×64 canvas.
// mask:true lets getColor tint the white silhouette; transparent pixels stay transparent.
const ICON_MAPPING = {
	aircraft: { x: 0, y: 0, width: 64, height: 64, mask: true },
} as const

const COLOR_MATCHED: [number, number, number, number] = [59, 130, 246, 220]
const COLOR_DIMMED: [number, number, number, number] = [59, 130, 246, 60]

export const aviationLayer: MapLayerDefinition<AircraftPosition[], AviationFilters> = {
	id: 'aviation-positions',
	label: 'Aircraft',
	enabledByDefault: true,

	createLayer(data, filters) {
		// Status is a hard filter — grounded/airborne exclusion happens before
		// the layer ever sees the data, same as any other "not shown at all" filter.
		const visible = data.filter((d) => matchesStatus(d, filters.status))

		return new IconLayer<AircraftPosition>({
			id: 'aviation-positions',
			data: visible,
			iconAtlas: '/aircraft-icon.svg',
			iconMapping: ICON_MAPPING,
			getIcon: () => 'aircraft',
			getPosition: (d) => [d.lon, d.lat],
			// deck.gl rotates counterclockwise from north; course_deg is clockwise from north.
			// Negating converts: course_deg=90 (east) → getAngle=-90 (clockwise = right).
			getAngle: (d) => -(d.courseDeg ?? 0),
			getSize: 24,
			// Callsign search dims rather than removes — a soft filter layered on
			// top of the hard status filter above.
			getColor: (d) => (matchesSearch(d, filters.callsignSearch) ? COLOR_MATCHED : COLOR_DIMMED),
			updateTriggers: {
				getColor: [filters.callsignSearch],
			},
			pickable: true,
		})
	},
}
