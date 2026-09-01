import type { Layer } from '@deck.gl/core'

export interface MapLayerDefinition<TData, TFilters = unknown> {
	id: string
	label: string
	enabledByDefault: boolean
	createLayer(data: TData, filters: TFilters): Layer
}
