import { Router } from 'express';
import { pool } from '../db.js';
import { asyncHandler } from '../shared/asyncHandler.js';
import { scanLiveEntities } from '../shared/liveEntities.js';
import { matchesEntityScope } from '../shared/entityScopeFilter.js';
import type { GeoBounds } from '../shared/regions.js';

const router = Router();

interface WorkspaceScopeRow {
	geo_region: { bounds: GeoBounds };
	entity_types: string[];
}

// Same bbox shape/order as GET /entities/live and GET /alerts
// (minLat,minLon,maxLat,maxLon). Demo-only fallback, same as GET /alerts.
function parseBbox(raw: string | undefined): GeoBounds | null {
	if (raw === undefined) return null;
	const parts = raw.split(',').map(Number);
	if (parts.length !== 4 || parts.some((n) => !Number.isFinite(n))) return null;
	const [minLat, minLon, maxLat, maxLon] = parts as [number, number, number, number];
	return { min_lat: minLat, max_lat: maxLat, min_lon: minLon, max_lon: maxLon };
}

router.get(
	'/',
	asyncHandler(async (req, res) => {
		// Operator: filtered by their saved workspace. No saved workspace means
		// no entities -- the same fail-closed rule ADR-012 already applies to
		// GET /alerts, applied here for the same reason: an operator's view is
		// scoped by design, not by omission.
		if (res.locals['userRole'] === 'operator') {
			const scopeResult = await pool.query<{ scope: WorkspaceScopeRow }>(
				'SELECT scope FROM user_workspaces WHERE user_id = $1',
				[res.locals['userId'] as string],
			);
			if (scopeResult.rows.length === 0) {
				res.json([]);
				return;
			}
			const scope = scopeResult.rows[0]!.scope;
			const entities = await scanLiveEntities((e) =>
				matchesEntityScope(e, {
					bounds: scope.geo_region.bounds,
					entity_types: scope.entity_types,
				}),
			);
			res.json(entities);
			return;
		}

		// Demo (or any other non-operator caller, including no auth at all):
		// an ad-hoc bbox is the only scope dimension available, same as
		// GET /alerts. No bbox means fully unfiltered, matching
		// GET /entities/live's own demo behavior (it requires bbox itself, but
		// this route's unfiltered fallback is the same "nothing restricts a
		// caller with no scope to draw from" rule GET /alerts already uses).
		const bboxParam = req.query['bbox'] as string | undefined;
		if (bboxParam !== undefined) {
			const bounds = parseBbox(bboxParam);
			if (!bounds) {
				res.status(400).json({ error: 'bbox must be minLat,minLon,maxLat,maxLon' });
				return;
			}
			const entities = await scanLiveEntities((e) =>
				matchesEntityScope(e, { bounds, entity_types: null }),
			);
			res.json(entities);
			return;
		}

		const entities = await scanLiveEntities(() => true);
		res.json(entities);
	}),
);

export { router as entitiesRouter };
