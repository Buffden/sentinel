import type { NextFunction, Request, Response } from 'express';
import { Router } from 'express';
import { pool } from '../db.js';
import { redis } from '../redis.js';
import { config } from '../config.js';
import { matchesScope, type AlertForScopeCheck } from '../shared/alertScopeFilter.js';
import type { GeoBounds } from '../shared/regions.js';
import { transitionAlert, type LifecycleTargetStatus } from './alertLifecycle.js';

const router = Router();

// Demo sessions are valid, authenticated requests (requireAuth accepts
// them) that simply have no lifecycle authority -- same rule and reasoning
// as workspace.ts's requireOperatorRole, kept local here since the 403
// message differs per route and this is the only other consumer so far.
function requireOperatorRole(req: Request, res: Response, next: NextFunction): void {
	if (res.locals['userRole'] !== 'operator') {
		res.status(403).json({ error: 'Alert lifecycle changes are not available for this session' });
		return;
	}
	next();
}

interface AlertRow extends AlertForScopeCheck {
	alert_id: string;
	entity_id: string;
	counterparty_entity_id: string | null;
}

interface WorkspaceScopeRow {
	geo_region: { bounds: GeoBounds };
	entity_types: string[];
	alert_types: string[];
}

// Same bbox shape/order as GET /entities/live (minLat,minLon,maxLat,maxLon).
// Unlike that endpoint, bbox is optional here -- it only ever applies to a
// demo session's ad-hoc filter, never to an operator's saved scope.
function parseBbox(raw: string | undefined): GeoBounds | null {
	if (raw === undefined) return null;
	const parts = raw.split(',').map(Number);
	if (parts.length !== 4 || parts.some((n) => !Number.isFinite(n))) return null;
	const [minLat, minLon, maxLat, maxLon] = parts as [number, number, number, number];
	return { min_lat: minLat, max_lat: maxLat, min_lon: minLon, max_lon: maxLon };
}

router.get('/', async (req, res) => {
	const result = await pool.query<AlertRow>(
		`SELECT alert_id, entity_id, counterparty_entity_id, entity_type, alert_type, priority, status,
						superseded_by, payload, detected_at, updated_at, acknowledged_at, resolved_at
		 FROM alerts
		 WHERE status IN ('NEW', 'ACKNOWLEDGED')
		 ORDER BY detected_at DESC`,
	);
	const rows = result.rows;

	// Operator: filtered by their saved workspace. No saved workspace means
	// no alerts -- the same rule ADR-012 already applies to the WebSocket
	// stream, applied here to the REST read for consistency.
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
		res.json(
			rows.filter((row) =>
				matchesScope(row, {
					bounds: scope.geo_region.bounds,
					entity_types: scope.entity_types,
					alert_types: scope.alert_types,
				}),
			),
		);
		return;
	}

	// Demo (or any other non-operator caller, including no auth at all):
	// an ad-hoc bbox is the only scope dimension available, since demo can
	// never save a workspace. No bbox means fully unfiltered -- the
	// transitional fallback until a frontend caller passes the map's
	// current viewport.
	const bboxParam = req.query['bbox'] as string | undefined;
	if (bboxParam !== undefined) {
		const bounds = parseBbox(bboxParam);
		if (!bounds) {
			res.status(400).json({ error: 'bbox must be minLat,minLon,maxLat,maxLon' });
			return;
		}
		res.json(
			rows.filter((row) => matchesScope(row, { bounds, entity_types: null, alert_types: null })),
		);
		return;
	}

	res.json(rows);
});

router.patch('/:alert_id', requireOperatorRole, async (req, res) => {
	const alertId = req.params['alert_id'] as string;
	const target = (req.body as { status?: unknown }).status;
	if (target !== 'ACKNOWLEDGED' && target !== 'RESOLVED') {
		res.status(400).json({ error: 'status must be ACKNOWLEDGED or RESOLVED' });
		return;
	}

	const outcome = await transitionAlert(
		alertId,
		target as LifecycleTargetStatus,
		res.locals['userId'] as string,
	);

	switch (outcome.kind) {
		case 'not_found':
			res.status(404).json({ error: 'alert not found' });
			return;
		case 'invalid_transition':
			res.status(409).json(outcome.alert);
			return;
		case 'applied':
		case 'idempotent':
			// Always publish, even on an idempotent no-op write: this is the
			// same "republish canonical state on every delivery" discipline
			// compositeSupersession.ts already uses, so a client retrying a
			// PATCH after a lost publish still converges.
			await redis.publish(config.ALERT_EVENTS_CHANNEL, JSON.stringify(outcome.alert));
			res.json(outcome.alert);
			return;
	}
});

export { router as alertsRouter };
