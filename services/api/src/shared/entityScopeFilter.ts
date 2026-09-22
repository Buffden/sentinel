import { withinBounds, type GeoBounds } from './regions.js';
import type { LiveEntity } from './liveEntities.js';

export interface EntityScopeFilter {
	bounds: GeoBounds;
	// null means no restriction -- used only by a demo session's ad-hoc bbox
	// filter, same convention as alertScopeFilter.ts's ScopeFilter. An
	// operator's EntityScopeFilter always carries their saved entity_types.
	entity_types: readonly string[] | null;
}

// Unlike matchesScope (alertScopeFilter.ts), a live entity's position comes
// straight from its own Redis hash fields -- no payload extraction by type
// is needed, and there is no alert_types dimension to check for a plain
// entity list.
export function matchesEntityScope(entity: LiveEntity, scope: EntityScopeFilter): boolean {
	if (!withinBounds(entity, scope.bounds)) return false;
	if (
		scope.entity_types &&
		(entity.entity_type === null || !scope.entity_types.includes(entity.entity_type))
	)
		return false;
	return true;
}
