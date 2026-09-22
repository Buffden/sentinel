import type { NextFunction, Request, Response } from 'express';
import { Router } from 'express';
import { pool } from '../db.js';
import { redis } from '../redis.js';
import { config } from '../config.js';
import { matchesScope, type AlertForScopeCheck } from '../shared/alertScopeFilter.js';
import { parseBboxParam } from '../shared/regions.js';
import { fetchWorkspaceScope } from '../shared/entityAccess.js';
import { asyncHandler } from '../shared/asyncHandler.js';
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

const KNOWN_STATUSES = ['NEW', 'ACKNOWLEDGED', 'RESOLVED', 'SUPERSEDED'];

// Comma-separated status list, e.g. "RESOLVED,SUPERSEDED" for investigation
// pulling closed history. Returns null (caller keeps today's NEW/ACKNOWLEDGED
// default) when the param is absent, and an error string when present but
// invalid -- never a silently-empty result for a typo'd status.
function parseStatusFilter(
	raw: string | undefined,
): { statuses: string[] } | { error: string } | null {
	if (raw === undefined) return null;
	const statuses = raw.split(',').map((s) => s.trim());
	const invalid = statuses.find((s) => !KNOWN_STATUSES.includes(s));
	if (invalid !== undefined) {
		return { error: `unknown status "${invalid}"; must be one of ${KNOWN_STATUSES.join(', ')}` };
	}
	return { statuses };
}

router.get(
	'/',
	asyncHandler(async (req, res) => {
		const statusParam = req.query['status'] as string | undefined;
		const parsedStatus = parseStatusFilter(statusParam);
		if (parsedStatus !== null && 'error' in parsedStatus) {
			res.status(400).json({ error: parsedStatus.error });
			return;
		}
		// Default unchanged from before this checkpoint: the dashboard's
		// AlertWidget depends on exactly this restriction when no status param
		// is supplied, and must see no behavior change.
		const statuses = parsedStatus?.statuses ?? ['NEW', 'ACKNOWLEDGED'];

		// Optional investigation filter: alerts where this entity is primary or
		// counterparty, same join GET /entities/:entity_id already uses for its
		// own alerts array. Absent means unfiltered by entity, same as today.
		const entityIdParam = (req.query['entity_id'] as string | undefined) ?? null;

		const result = await pool.query<AlertRow>(
			`SELECT alert_id, entity_id, counterparty_entity_id, entity_type, alert_type, priority, status,
							superseded_by, payload, detected_at, updated_at, acknowledged_at, resolved_at
			 FROM alerts
			 WHERE status = ANY($1)
				 AND ($2::text IS NULL OR entity_id = $2 OR counterparty_entity_id = $2)
			 ORDER BY detected_at DESC`,
			[statuses, entityIdParam],
		);
		const rows = result.rows;

		// Operator: filtered by their saved workspace. No saved workspace means
		// no alerts -- the same rule ADR-012 already applies to the WebSocket
		// stream, applied here to the REST read for consistency.
		if (res.locals['userRole'] === 'operator') {
			const scope = await fetchWorkspaceScope(res.locals['userId'] as string);
			if (scope === null) {
				res.json([]);
				return;
			}
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
			const bounds = parseBboxParam(bboxParam);
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
	}),
);

router.get(
	'/:alert_id',
	asyncHandler(async (req, res) => {
		const alertId = req.params['alert_id'] as string;
		const result = await pool.query<AlertRow>(
			`SELECT alert_id, entity_id, counterparty_entity_id, entity_type, alert_type, priority, status,
							superseded_by, payload, detected_at, updated_at, acknowledged_at, resolved_at
			 FROM alerts
			 WHERE alert_id = $1`,
			[alertId],
		);
		const row = result.rows[0];
		if (!row) {
			res.status(404).json({ error: 'alert not found' });
			return;
		}

		// Same fail-closed by-scope rule as GET /alerts, extended to a direct
		// by-id lookup for the same reason every other by-id endpoint this
		// phase needed it: otherwise an operator could enumerate alert_ids to
		// read evidence their saved scope was supposed to hide. Unlike the
		// entity by-id endpoints, there's no "dark" fallback to consider here --
		// an alert row always carries its own entity_type/alert_type/payload,
		// nothing else to check.
		if (res.locals['userRole'] === 'operator') {
			const scope = await fetchWorkspaceScope(res.locals['userId'] as string);
			if (scope === null) {
				res.status(404).json({ error: 'alert not found' });
				return;
			}
			const inScope = matchesScope(row, {
				bounds: scope.geo_region.bounds,
				entity_types: scope.entity_types,
				alert_types: scope.alert_types,
			});
			if (!inScope) {
				res.status(404).json({ error: 'alert not found' });
				return;
			}
		}

		// Demo (or any other non-operator caller): unrestricted, same as every
		// other by-id endpoint's demo path this phase -- no bbox-equivalent
		// scope dimension applies to a single-id lookup.
		res.json(row);
	}),
);

router.patch(
	'/:alert_id',
	requireOperatorRole,
	asyncHandler(async (req, res) => {
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
	}),
);

export { router as alertsRouter };
