import { Router } from 'express';
import { pool } from '../db.js';
import { config } from '../config.js';
import { asyncHandler } from '../shared/asyncHandler.js';
import { scanLiveEntities, getLiveEntity } from '../shared/liveEntities.js';
import { matchesEntityScope } from '../shared/entityScopeFilter.js';
import { matchesScope, type AlertForScopeCheck } from '../shared/alertScopeFilter.js';
import { resolveOperatorEntityAccess, fetchWorkspaceScope } from '../shared/entityAccess.js';
import { parseBboxParam } from '../shared/regions.js';

const router = Router();

interface AlertRow extends AlertForScopeCheck {
	alert_id: string;
	entity_id: string;
	counterparty_entity_id: string | null;
}

router.get(
	'/',
	asyncHandler(async (req, res) => {
		// Operator: filtered by their saved workspace. No saved workspace means
		// no entities -- the same fail-closed rule ADR-012 already applies to
		// GET /alerts, applied here for the same reason: an operator's view is
		// scoped by design, not by omission.
		if (res.locals['userRole'] === 'operator') {
			const scope = await fetchWorkspaceScope(res.locals['userId'] as string);
			if (scope === null) {
				res.json([]);
				return;
			}
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
			const bounds = parseBboxParam(bboxParam);
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
	asyncHandler(async (req, res, next) => {
		const entityId = req.params['entity_id'] as string;

		// Structural guard against the /entities/live collision, independent of
		// index.ts's mount order: "live" is never a real entity_id (Position
		// Consumer never writes an entity:live:live hash), so falling through
		// to the next mounted router is always correct here, and it makes this
		// route correct even if a future refactor reorders the mounts -- the
		// one regression test for that (entities.integration.test.ts) still
		// exists too, but shouldn't be the only thing preventing it.
		if (entityId === 'live') {
			next();
			return;
		}

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
		// extended to a direct by-id lookup via the shared access check -- see
		// entityAccess.ts for why this can't just be "was it in the list".
		// A 404 here (not 403) doesn't confirm the entity exists outside scope.
		if (res.locals['userRole'] === 'operator') {
			const access = await resolveOperatorEntityAccess(
				res.locals['userId'] as string,
				entityId,
				liveEntity,
			);
			if (access.kind !== 'in_scope') {
				res.status(404).json({ error: 'entity not found' });
				return;
			}

			const alertScope = {
				bounds: access.scope.geo_region.bounds,
				entity_types: access.scope.entity_types,
				alert_types: access.scope.alert_types,
			};
			const scopedAlerts = alerts.filter((a) => matchesScope(a, alertScope));

			res.json({ entity: liveEntity, alerts: scopedAlerts });
			return;
		}

		// Demo (or any other non-operator caller): unrestricted, same as
		// GET /entities/live's own demo behavior -- there is no sensible bbox
		// for a single-id lookup.
		res.json({ entity: liveEntity, alerts });
	}),
);

interface PositionHistoryRow {
	entity_id: string;
	timestamp_ms: string;
	lat: number;
	lon: number;
	altitude_m: number | null;
	speed_mps: number | null;
	course_deg: number | null;
	heading_deg: number | null;
	on_ground: boolean | null;
	callsign: string | null;
	entity_subtype: string | null;
}

router.get(
	'/:entity_id/history',
	asyncHandler(async (req, res) => {
		const entityId = req.params['entity_id'] as string;

		// Required, not defaulted: an investigation view always has a concrete
		// window in mind (usually an alert's own detected_at range), and there
		// is no sensible width to guess -- same reasoning GET /entities/live
		// already applies to its own required bbox.
		const fromMs = Number(req.query['from_ms']);
		const toMs = Number(req.query['to_ms']);
		if (!Number.isFinite(fromMs) || !Number.isFinite(toMs) || fromMs > toMs) {
			res
				.status(400)
				.json({ error: 'from_ms and to_ms are required and from_ms must be <= to_ms' });
			return;
		}

		const liveEntity = await getLiveEntity(entityId);

		// Same fail-closed by-id access rule as GET /entities/:entity_id -- see
		// entityAccess.ts. History is reachable directly, without ever calling
		// the entity-detail endpoint first, so it needs this check on its own.
		if (res.locals['userRole'] === 'operator') {
			const access = await resolveOperatorEntityAccess(
				res.locals['userId'] as string,
				entityId,
				liveEntity,
			);
			if (access.kind !== 'in_scope') {
				res.status(404).json({ error: 'entity not found' });
				return;
			}
		}

		const result = await pool.query<PositionHistoryRow>(
			`SELECT entity_id, timestamp_ms, lat, lon, altitude_m, speed_mps, course_deg,
							heading_deg, on_ground, callsign, entity_subtype
			 FROM position_history
			 WHERE entity_id = $1 AND observed_at >= to_timestamp($2 / 1000.0) AND observed_at <= to_timestamp($3 / 1000.0)
			 ORDER BY observed_at ASC
			 LIMIT $4`,
			[entityId, fromMs, toMs, config.ENTITY_HISTORY_MAX_POINTS],
		);

		res.json(
			result.rows.map((row) => ({
				...row,
				timestamp_ms: Number(row.timestamp_ms),
			})),
		);
	}),
);

export { router as entitiesRouter };
