import { describe, expect, it } from 'vitest';
import { extractAlertPosition, matchesScope, type AlertForScopeCheck } from './alertScopeFilter.js';

const FRANCE_BOUNDS = { min_lat: 41.3, max_lat: 51.1, min_lon: -5.2, max_lon: 9.6 };

function alert(overrides: Partial<AlertForScopeCheck>): AlertForScopeCheck {
	return {
		entity_type: 'aircraft',
		alert_type: 'SIGNAL_LOSS',
		payload: {},
		...overrides,
	};
}

describe('extractAlertPosition', () => {
	it('reads SIGNAL_LOSS position from last_known_lat/last_known_lon', () => {
		const pos = extractAlertPosition(
			alert({ alert_type: 'SIGNAL_LOSS', payload: { last_known_lat: 45, last_known_lon: 2 } }),
		);
		expect(pos).toEqual({ lat: 45, lon: 2 });
	});

	it('does not read SIGNAL_LOSS position from a flat lat/lon (the stale ADR-012 field name)', () => {
		expect(
			extractAlertPosition(alert({ alert_type: 'SIGNAL_LOSS', payload: { lat: 45, lon: 2 } })),
		).toBeNull();
	});

	it('reads UNSCHEDULED_PROXIMITY position from a flat lat/lon', () => {
		const pos = extractAlertPosition(
			alert({ alert_type: 'UNSCHEDULED_PROXIMITY', payload: { lat: 45, lon: 2 } }),
		);
		expect(pos).toEqual({ lat: 45, lon: 2 });
	});

	it('reads COMPOSITE position from nested payload.proximity.lat/lon', () => {
		const pos = extractAlertPosition(
			alert({ alert_type: 'COMPOSITE', payload: { proximity: { lat: 45, lon: 2 } } }),
		);
		expect(pos).toEqual({ lat: 45, lon: 2 });
	});

	it('does not read COMPOSITE position from a top-level lat/lon (the stale ADR-012 field name)', () => {
		expect(
			extractAlertPosition(alert({ alert_type: 'COMPOSITE', payload: { lat: 45, lon: 2 } })),
		).toBeNull();
	});

	it('returns null for ROUTE_DEVIATION (payload shape undecided)', () => {
		expect(
			extractAlertPosition(alert({ alert_type: 'ROUTE_DEVIATION', payload: { lat: 45, lon: 2 } })),
		).toBeNull();
	});

	it('returns null when the expected fields are missing or non-numeric', () => {
		expect(
			extractAlertPosition(
				alert({ alert_type: 'SIGNAL_LOSS', payload: { last_known_lat: 'not-a-number' } }),
			),
		).toBeNull();
	});
});

describe('matchesScope', () => {
	it('matches an alert inside bounds with matching entity and alert type', () => {
		const a = alert({
			entity_type: 'aircraft',
			alert_type: 'SIGNAL_LOSS',
			payload: { last_known_lat: 45, last_known_lon: 2 },
		});
		expect(
			matchesScope(a, {
				bounds: FRANCE_BOUNDS,
				entity_types: ['aircraft'],
				alert_types: ['SIGNAL_LOSS'],
			}),
		).toBe(true);
	});

	it('excludes an alert outside the geographic bounds', () => {
		const a = alert({ payload: { last_known_lat: 40.7, last_known_lon: -74.0 } }); // NYC, outside France
		expect(
			matchesScope(a, {
				bounds: FRANCE_BOUNDS,
				entity_types: ['aircraft'],
				alert_types: ['SIGNAL_LOSS'],
			}),
		).toBe(false);
	});

	it('excludes an alert whose type is not in alert_types', () => {
		const a = alert({
			alert_type: 'UNSCHEDULED_PROXIMITY',
			payload: { lat: 45, lon: 2 },
		});
		expect(
			matchesScope(a, {
				bounds: FRANCE_BOUNDS,
				entity_types: ['aircraft'],
				alert_types: ['SIGNAL_LOSS'],
			}),
		).toBe(false);
	});

	it('excludes an alert whose entity_type is not in entity_types', () => {
		const a = alert({ entity_type: 'vessel', payload: { last_known_lat: 45, last_known_lon: 2 } });
		expect(
			matchesScope(a, {
				bounds: FRANCE_BOUNDS,
				entity_types: ['aircraft'],
				alert_types: ['SIGNAL_LOSS'],
			}),
		).toBe(false);
	});

	it('excludes an alert with no extractable position, even with permissive bounds', () => {
		const a = alert({ alert_type: 'ROUTE_DEVIATION', payload: {} });
		expect(
			matchesScope(a, {
				bounds: { min_lat: -90, max_lat: 90, min_lon: -180, max_lon: 180 },
				entity_types: null,
				alert_types: null,
			}),
		).toBe(false);
	});

	it('treats null entity_types/alert_types as no restriction (the demo bbox case)', () => {
		const a = alert({
			entity_type: 'aircraft',
			alert_type: 'COMPOSITE',
			payload: { proximity: { lat: 45, lon: 2 } },
		});
		expect(matchesScope(a, { bounds: FRANCE_BOUNDS, entity_types: null, alert_types: null })).toBe(
			true,
		);
	});
});
