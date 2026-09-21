import { withinBounds, type GeoBounds } from './regions.js';

export interface AlertForScopeCheck {
	entity_type: string;
	alert_type: string;
	payload: unknown;
}

export interface ScopeFilter {
	bounds: GeoBounds;
	// null means no restriction on this dimension -- used only by a demo
	// session's ad-hoc bbox filter, which has no saved entity/alert-type
	// list to draw from. An operator's ScopeFilter always carries concrete
	// arrays from their saved workspace; null must never appear there.
	entity_types: readonly string[] | null;
	alert_types: readonly string[] | null;
}

interface Position {
	lat: number;
	lon: number;
}

function numberField(obj: Record<string, unknown>, key: string): number | null {
	const v = obj[key];
	return typeof v === 'number' && Number.isFinite(v) ? v : null;
}

// Position is always read from the alert's own payload (immutable at
// detection time), never current Redis state -- see ADR-012. The field
// path differs by alert_type -- corrected against the real payload builders
// (services/alert-evaluator/src/evaluator.ts and composite.ts), not the
// (stale) paths ADR-012 originally named. ROUTE_DEVIATION's payload shape
// is undecided (Phase 04 is deferred), so it deliberately returns null
// rather than guessing -- matchesScope treats null as excluded, never
// included by a fallback.
export function extractAlertPosition(alert: AlertForScopeCheck): Position | null {
	const payload = (alert.payload ?? {}) as Record<string, unknown>;

	switch (alert.alert_type) {
		case 'SIGNAL_LOSS': {
			const lat = numberField(payload, 'last_known_lat');
			const lon = numberField(payload, 'last_known_lon');
			return lat !== null && lon !== null ? { lat, lon } : null;
		}
		case 'UNSCHEDULED_PROXIMITY': {
			const lat = numberField(payload, 'lat');
			const lon = numberField(payload, 'lon');
			return lat !== null && lon !== null ? { lat, lon } : null;
		}
		case 'COMPOSITE': {
			const proximity = payload['proximity'];
			if (typeof proximity !== 'object' || proximity === null) return null;
			const p = proximity as Record<string, unknown>;
			const lat = numberField(p, 'lat');
			const lon = numberField(p, 'lon');
			return lat !== null && lon !== null ? { lat, lon } : null;
		}
		default:
			return null;
	}
}

// One definition, reused by GET /alerts (CP2) and the future WebSocket
// alert-events fan-out (CP3) -- see the alert-scope-filtering concept doc
// for why this must not become two separately-maintained implementations.
export function matchesScope(alert: AlertForScopeCheck, scope: ScopeFilter): boolean {
	const pos = extractAlertPosition(alert);
	if (!pos) return false;
	if (!withinBounds(pos, scope.bounds)) return false;
	if (scope.entity_types && !scope.entity_types.includes(alert.entity_type)) return false;
	if (scope.alert_types && !scope.alert_types.includes(alert.alert_type)) return false;
	return true;
}
