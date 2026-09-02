import { describe, expect, it } from 'vitest';
import { normalizeAdsbRaw } from './normalize.js';

function raw(overrides: Record<string, unknown> = {}): string {
	return JSON.stringify({
		icao24: 'abc123',
		lat: 51.5,
		lon: -0.1,
		time_position: 1_700_000_000,
		...overrides,
	});
}

describe('normalizeAdsbRaw', () => {
	it('rejects unparseable JSON as parse_error', () => {
		const result = normalizeAdsbRaw('{not json');
		expect(result).toMatchObject({ ok: false, kind: 'parse_error' });
	});

	it('rejects a JSON array as parse_error, not a valid record', () => {
		const result = normalizeAdsbRaw('[1,2,3]');
		expect(result).toMatchObject({ ok: false, kind: 'parse_error' });
	});

	it('routes missing icao24 to missing_entity_id, not no_position', () => {
		const result = normalizeAdsbRaw(raw({ icao24: undefined }));
		expect(result).toMatchObject({ ok: false, kind: 'missing_entity_id' });
	});

	it('routes empty-string icao24 to missing_entity_id', () => {
		const result = normalizeAdsbRaw(raw({ icao24: '' }));
		expect(result).toMatchObject({ ok: false, kind: 'missing_entity_id' });
	});

	it('treats a null lat as no_position, not a DLQ candidate', () => {
		const result = normalizeAdsbRaw(raw({ lat: null }));
		expect(result).toMatchObject({ ok: false, kind: 'no_position', entity_id: 'abc123' });
	});

	it('treats a missing time_position as no_position', () => {
		const result = normalizeAdsbRaw(raw({ time_position: undefined }));
		expect(result).toMatchObject({ ok: false, kind: 'no_position', entity_id: 'abc123' });
	});

	it('treats a wrong-typed lat as parse_error, not no_position', () => {
		const result = normalizeAdsbRaw(raw({ lat: 'not-a-number' }));
		expect(result).toMatchObject({ ok: false, kind: 'parse_error' });
	});

	it('converts time_position from seconds to canonical milliseconds', () => {
		const result = normalizeAdsbRaw(raw({ time_position: 1_700_000_000 }));
		expect(result.ok).toBe(true);
		if (result.ok) expect(result.position.timestamp_ms).toBe(1_700_000_000_000);
	});

	it('prefers geo_altitude over baro_altitude when both are present', () => {
		const result = normalizeAdsbRaw(raw({ geo_altitude: 1000, baro_altitude: 900 }));
		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.position.altitude_m).toBe(1000);
			expect(result.position.geo_altitude_m).toBe(1000);
			expect(result.position.baro_altitude_m).toBe(900);
		}
	});

	it('falls back to baro_altitude when geo_altitude is absent', () => {
		const result = normalizeAdsbRaw(raw({ baro_altitude: 900 }));
		expect(result.ok).toBe(true);
		if (result.ok) expect(result.position.altitude_m).toBe(900);
	});

	it('converts last_contact from seconds to canonical milliseconds', () => {
		const result = normalizeAdsbRaw(raw({ last_contact: 1_700_000_500 }));
		expect(result.ok).toBe(true);
		if (result.ok) expect(result.position.last_contact_ms).toBe(1_700_000_500_000);
	});

	it('trims OpenSky callsign padding', () => {
		const result = normalizeAdsbRaw(raw({ callsign: 'BAW123  ' }));
		expect(result.ok).toBe(true);
		if (result.ok) expect(result.position.callsign).toBe('BAW123');
	});

	it('normalizes a blank callsign to null rather than an empty string', () => {
		const result = normalizeAdsbRaw(raw({ callsign: '   ' }));
		expect(result.ok).toBe(true);
		if (result.ok) expect(result.position.callsign).toBeNull();
	});

	it.each([
		[8, 'rotorcraft'],
		[10, 'lighter_than_air'],
		[14, 'uav'],
		[3, 'fixed_wing'],
		[7, 'fixed_wing'],
		[1, 'unknown'],
		[99, 'unknown'],
	])('maps OpenSky category %i to entity_subtype %s', (category: number, expected: string) => {
		const result = normalizeAdsbRaw(raw({ category }));
		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.position.entity_subtype).toBe(expected);
			expect(result.position.provider_category).toBe(String(category));
		}
	});

	it('leaves entity_subtype and provider_category null when category is absent', () => {
		const result = normalizeAdsbRaw(raw());
		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.position.entity_subtype).toBeNull();
			expect(result.position.provider_category).toBeNull();
		}
	});

	it('produces an aircraft position with all AIS-specific fields null', () => {
		const result = normalizeAdsbRaw(raw());
		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.position.entity_type).toBe('aircraft');
			expect(result.position.source).toBe('adsb');
			expect(result.position.navigation_status).toBeNull();
			expect(result.position.rate_of_turn).toBeNull();
			expect(result.position.destination).toBeNull();
		}
	});
});
