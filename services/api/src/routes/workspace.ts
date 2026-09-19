import type { NextFunction, Request, Response } from 'express';
import { Router } from 'express';
import { pool } from '../db.js';
import { findPredefinedRegion, PREDEFINED_REGIONS, type GeoBounds } from '../shared/regions.js';
import { asyncHandler } from '../shared/asyncHandler.js';

const router = Router();

// v1 tracks aircraft only. Extending this list before a second entity type
// actually exists would be speculative infrastructure (CLAUDE.md).
const ALLOWED_ENTITY_TYPES = ['aircraft'] as const;

// ROUTE_DEVIATION is a valid alert type in the data model but does not yet
// flow through the system (Phase 04 is deferred at CP1) -- listed here so
// a saved scope naming it is not rejected once it does.
const ALLOWED_ALERT_TYPES = [
	'SIGNAL_LOSS',
	'ROUTE_DEVIATION',
	'UNSCHEDULED_PROXIMITY',
	'COMPOSITE',
] as const;

interface GeoRegionSelection {
	name: string | null;
	bounds: GeoBounds;
}

interface WorkspaceScope {
	geo_region: GeoRegionSelection;
	entity_types: string[];
	alert_types: string[];
}

// Route gating, not requireAuth's job: a demo session is a valid, authenticated
// request (requireAuth accepts it) that simply has no workspace to read or
// write. Demo sessions carry a non-UUID user_id and no `users` row, so letting
// one reach the query would fail the user_workspaces foreign key instead of
// returning a clean, intentional 403.
function requireOperatorRole(req: Request, res: Response, next: NextFunction): void {
	if (res.locals['userRole'] !== 'operator') {
		res.status(403).json({ error: 'Workspace scope is not available for this session' });
		return;
	}
	next();
}

function isValidBounds(raw: unknown): raw is GeoBounds {
	if (typeof raw !== 'object' || raw === null) return false;
	const b = raw as Record<string, unknown>;
	const { min_lat, max_lat, min_lon, max_lon } = b;
	if (
		typeof min_lat !== 'number' ||
		typeof max_lat !== 'number' ||
		typeof min_lon !== 'number' ||
		typeof max_lon !== 'number' ||
		![min_lat, max_lat, min_lon, max_lon].every(Number.isFinite)
	) {
		return false;
	}
	if (min_lat < -90 || max_lat > 90 || min_lon < -180 || max_lon > 180) return false;
	return min_lat < max_lat && min_lon < max_lon;
}

// A named selection always resolves to the catalog's own bounds -- a client
// cannot submit "France" with a bounding box that isn't actually France.
// Only a custom (name: null) selection supplies its own bounds.
export function resolveGeoRegion(raw: unknown): GeoRegionSelection | null {
	if (typeof raw !== 'object' || raw === null) return null;
	const r = raw as Record<string, unknown>;
	const name = r['name'];

	if (name === null) {
		const bounds = r['bounds'];
		return isValidBounds(bounds) ? { name: null, bounds } : null;
	}
	if (typeof name !== 'string') return null;

	const predefined = findPredefinedRegion(name);
	return predefined ? { name: predefined.name, bounds: predefined.bounds } : null;
}

export function isSubsetOfAllowed<T extends string>(
	values: unknown,
	allowed: readonly T[],
): values is T[] {
	return (
		Array.isArray(values) &&
		values.length > 0 &&
		values.every((v) => typeof v === 'string' && (allowed as readonly string[]).includes(v))
	);
}

// Static catalog, not user-specific and highly cacheable -- GET is the
// deliberate exception to the POST-for-new-reads convention (see
// docs/DATA_MODEL.md's API contracts section).
router.get('/regions', (_req, res) => {
	res.json(PREDEFINED_REGIONS);
});

router.post(
	'/',
	requireOperatorRole,
	asyncHandler(async (_req, res) => {
		const userId = res.locals['userId'] as string;
		const result = await pool.query<{ scope: WorkspaceScope }>(
			`SELECT scope FROM user_workspaces WHERE user_id = $1`,
			[userId],
		);
		if (result.rows.length === 0) {
			res.status(404).json({ error: 'no_workspace' });
			return;
		}
		res.json(result.rows[0]!.scope);
	}),
);

router.put(
	'/',
	requireOperatorRole,
	asyncHandler(async (req, res) => {
		const body = req.body as {
			geo_region?: unknown;
			entity_types?: unknown;
			alert_types?: unknown;
		};

		const geoRegion = resolveGeoRegion(body.geo_region);
		if (!geoRegion) {
			res.status(400).json({ error: 'Invalid geo_region' });
			return;
		}
		if (!isSubsetOfAllowed(body.entity_types, ALLOWED_ENTITY_TYPES)) {
			res.status(400).json({ error: 'Invalid entity_types' });
			return;
		}
		if (!isSubsetOfAllowed(body.alert_types, ALLOWED_ALERT_TYPES)) {
			res.status(400).json({ error: 'Invalid alert_types' });
			return;
		}

		const scope: WorkspaceScope = {
			geo_region: geoRegion,
			entity_types: body.entity_types,
			alert_types: body.alert_types,
		};
		const userId = res.locals['userId'] as string;

		await pool.query(
			`INSERT INTO user_workspaces (user_id, scope, updated_at)
			 VALUES ($1, $2, now())
			 ON CONFLICT (user_id) DO UPDATE SET scope = EXCLUDED.scope, updated_at = EXCLUDED.updated_at`,
			[userId, JSON.stringify(scope)],
		);

		res.json(scope);
	}),
);

export { router as workspaceRouter };
