import { pool } from '../db.js';
import { matchesEntityScope } from './entityScopeFilter.js';
import { matchesScope, type AlertForScopeCheck } from './alertScopeFilter.js';
import type { LiveEntity } from './liveEntities.js';
import type { GeoBounds } from './regions.js';

export interface WorkspaceScopeRow {
	geo_region: { bounds: GeoBounds };
	entity_types: string[];
	alert_types: string[];
}

export type EntityAccessResult =
	| { kind: 'no_workspace' }
	| { kind: 'out_of_scope' }
	| { kind: 'in_scope'; scope: WorkspaceScopeRow };

// Resolves whether an operator may see a given entity at all -- shared by
// every by-id entity endpoint (GET /entities/:entity_id, its /history) so
// this decision can't drift between them. Without this check, a direct-by-id
// lookup would let an operator enumerate entity_ids to see data their saved
// workspace scope (ADR-012) was supposed to hide -- CP1's list endpoint
// never has this problem, since it only ever emits ids already in scope.
//
// In scope if the entity's current live state matches, or -- when it has
// gone dark and Redis has nothing -- if it's the primary entity_id (not just
// a counterparty) on at least one alert the saved scope also permits. A dark
// entity with no current state is exactly the case a SIGNAL_LOSS
// investigation needs to still work for.
export async function resolveOperatorEntityAccess(
	userId: string,
	entityId: string,
	liveEntity: LiveEntity | null,
): Promise<EntityAccessResult> {
	const scopeResult = await pool.query<{ scope: WorkspaceScopeRow }>(
		'SELECT scope FROM user_workspaces WHERE user_id = $1',
		[userId],
	);
	if (scopeResult.rows.length === 0) return { kind: 'no_workspace' };

	const scope = scopeResult.rows[0]!.scope;
	const entityScope = { bounds: scope.geo_region.bounds, entity_types: scope.entity_types };

	if (liveEntity) {
		return matchesEntityScope(liveEntity, entityScope)
			? { kind: 'in_scope', scope }
			: { kind: 'out_of_scope' };
	}

	const alertScope = { ...entityScope, alert_types: scope.alert_types };
	const result = await pool.query<AlertForScopeCheck>(
		`SELECT entity_type, alert_type, payload FROM alerts WHERE entity_id = $1 LIMIT 50`,
		[entityId],
	);
	const inScope = result.rows.some((a) => matchesScope(a, alertScope));
	return inScope ? { kind: 'in_scope', scope } : { kind: 'out_of_scope' };
}
