import { Router } from 'express';
import { pool } from '../db.js';
import { config } from '../config.js';
import { asyncHandler } from '../shared/asyncHandler.js';
import { scanLiveEntities, getLiveEntity } from '../shared/liveEntities.js';
import { matchesEntityScope } from '../shared/entityScopeFilter.js';
import { matchesScope, type AlertForScopeCheck } from '../shared/alertScopeFilter.js';
import type { GeoBounds } from '../shared/regions.js';

const router = Router();

interface WorkspaceScopeRow {
	geo_region: { bounds: GeoBounds };
	entity_types: string[];
	alert_types: string[];
}

interface AlertRow extends AlertForScopeCheck {
	alert_id: string;
	entity_id: string;
	counterparty_entity_id: string | null;
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

router.get(
	'/:entity_id',
	asyncHandler(async (req, res) => {
		const entityId = req.params['entity_id'] as string;

		const [liveEntity, alertsResult] = await Promise.all([
			getLiveEntity(entityId),
			pool.query<AlertRow>(
				`SELECT alert_id, entity_id, counterparty_entity_id, entity_type, alert_type, priority,
								status, superseded_by, payload, detected_at, updated_at, acknowledged_at, resolved_at
				 FROM alerts
				 WHERE entity_id = $1 OR counterparty_entity_id = $1
				 ORDER BY detected_at DESC
				 LIMIT $2`,
				[entityId, config.ENTITY_RECENT_ALERTS_MAX],
			),
		]);
		const alerts = alertsResult.rows;

		if (liveEntity === null && alerts.length === 0) {
			res.status(404).json({ error: 'entity not found' });
			return;
		}

		// Operator: same fail-closed rule as GET /entities and GET /alerts,
		// extended to a direct by-id lookup -- otherwise an operator could
		// enumerate entity_ids to see data their saved scope was supposed to
		// hide. A 404 here (not 403) doesn't confirm the entity exists outside
		// their scope.
		if (res.locals['userRole'] === 'operator') {
			const scopeResult = await pool.query<{ scope: WorkspaceScopeRow }>(
				'SELECT scope FROM user_workspaces WHERE user_id = $1',
				[res.locals['userId'] as string],
			);
			if (scopeResult.rows.length === 0) {
				res.status(404).json({ error: 'entity not found' });
				return;
			}
			const scope = scopeResult.rows[0]!.scope;
			const entityScope = { bounds: scope.geo_region.bounds, entity_types: scope.entity_types };
			const alertScope = { ...entityScope, alert_types: scope.alert_types };

			const scopedAlerts = alerts.filter((a) => matchesScope(a, alertScope));

			// In scope if the live state itself matches, or -- when the entity
			// has gone dark and Redis has nothing -- if this entity is the
			// primary (not just counterparty) on at least one in-scope alert.
			// A counterparty-only match doesn't establish the primary entity's
			// own scope membership; it would let an operator confirm entities
			// outside their scope exist just by them being someone else's
			// counterparty.
			const inScope = liveEntity
				? matchesEntityScope(liveEntity, entityScope)
				: scopedAlerts.some((a) => a.entity_id === entityId);

			if (!inScope) {
				res.status(404).json({ error: 'entity not found' });
				return;
			}

			res.json({ entity: liveEntity, alerts: scopedAlerts });
			return;
		}

		// Demo (or any other non-operator caller): unrestricted, same as
		// GET /entities/live's own demo behavior -- there is no sensible bbox
		// for a single-id lookup.
		res.json({ entity: liveEntity, alerts });
	}),
);

export { router as entitiesRouter };
