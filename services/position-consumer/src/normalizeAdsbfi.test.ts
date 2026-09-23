import { describe, expect, it } from 'vitest';
import { adsbfiEventTimeMs, normalizeAdsbfiRecord, normalizeByProvider } from './normalize.js';

// A real aircraft object from an adsb.fi /api/v3 response captured on
// 2026-09-23 (EIN960 over SF Bay). That response's top-level `now` was
// 1790127082000, epoch milliseconds, kept by the poller as response_now_ms.
const RESPONSE_NOW_MS = 1790127082000;
const ein960 = {
	hex: '4caa40',
	type: 'adsb_icao',
	flight: 'EIN960  ',
	r: 'EI-EIM',
	t: 'A333',
	alt_baro: 4150,
	alt_geom: 4175,
	gs: 257.8,
	track: 331.25,
	baro_rate: 2496,
	squawk: '3211',
	category: 'A5',
	lat: 37.688416,
	lon: -122.520174,
	seen_pos: 0.4,
	spi: 0,
	seen: 0.3,
	response_now_ms: RESPONSE_NOW_MS,
	fetched_at_ms: 1790127082300,
};

describe('adsbfiEventTimeMs', () => {
	it('derives source time from the response time minus seconds-before-now', () => {
		expect(adsbfiEventTimeMs(RESPONSE_NOW_MS, 0.4)).toBe(1790127081600);
	});

	it('rounds to whole milliseconds so replays produce the same identity', () => {
		expect(adsbfiEventTimeMs(RESPONSE_NOW_MS, 1.2345)).toBe(1790127080766);
	});
});

describe('normalizeAdsbfiRecord', () => {
	it('maps the real EIN960 record with a deterministic source timestamp', () => {
		const result = normalizeAdsbfiRecord(ein960);
		expect(result.ok).toBe(true);
		if (!result.ok) return;
		expect(result.position).toMatchObject({
			entity_id: '4caa40',
			entity_type: 'aircraft',
			timestamp_ms: 1790127081600,
			lat: 37.688416,
			lon: -122.520174,
			course_deg: 331.25,
			source: 'adsb',
			provider: 'adsbfi',
			last_contact_ms: 1790127081700,
			squawk: '3211',
			spi: false,
			position_source: 0,
			callsign: 'EIN960',
			entity_subtype: 'fixed_wing',
			provider_category: 'A5',
		});
		expect(result.position.speed_mps).toBeCloseTo(257.8 * 0.514444, 6);
		expect(result.position.vertical_rate_mps).toBeCloseTo(2496 * 0.00508, 6);
	});

	it('never uses processing time: fetched_at_ms does not change the timestamp', () => {
		const a = normalizeAdsbfiRecord(ein960);
		const b = normalizeAdsbfiRecord({ ...ein960, fetched_at_ms: 9_999_999_999_999 });
		expect(a.ok && b.ok && a.position.timestamp_ms === b.position.timestamp_ms).toBe(true);
	});

	it('lowercases the ICAO address used as entity_id', () => {
		const result = normalizeAdsbfiRecord({ ...ein960, hex: '4CAA40' });
		expect(result.ok && result.position.entity_id).toBe('4caa40');
	});

	it('rejects a non-ICAO (~) track address instead of admitting it as an entity', () => {
		expect(normalizeAdsbfiRecord({ ...ein960, hex: '~2b3cf6' })).toMatchObject({
			ok: false,
			kind: 'missing_entity_id',
		});
	});

	it('treats a missing position as no_position, not a DLQ case', () => {
		expect(normalizeAdsbfiRecord({ ...ein960, lat: undefined, lon: undefined })).toEqual({
			ok: false,
			kind: 'no_position',
			entity_id: '4caa40',
		});
	});

	it('rejects a position that cannot be timed because response_now_ms is missing', () => {
		expect(normalizeAdsbfiRecord({ ...ein960, response_now_ms: undefined })).toMatchObject({
			ok: false,
			kind: 'parse_error',
		});
	});

	it('maps A7 to rotorcraft and keeps the provider category verbatim', () => {
		const result = normalizeAdsbfiRecord({ ...ein960, category: 'A7' });
		expect(
			result.ok && [result.position.entity_subtype, result.position.provider_category],
		).toEqual(['rotorcraft', 'A7']);
	});

	it('converts alt_baro and alt_geom from feet to metres', () => {
		const result = normalizeAdsbfiRecord(ein960);

		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.position.baro_altitude_m).toBeCloseTo(4150 * 0.3048);
			expect(result.position.geo_altitude_m).toBeCloseTo(4175 * 0.3048);
			expect(result.position.on_ground).toBe(false);
		}
	});

	it('prefers geo altitude over baro altitude for altitude_m, as the OpenSky mapping does', () => {
		const withGeo = normalizeAdsbfiRecord(ein960);

		expect(withGeo.ok).toBe(true);
		if (withGeo.ok) {
			expect(withGeo.position.altitude_m).toBeCloseTo(4175 * 0.3048);
		}

		const withoutGeo = normalizeAdsbfiRecord({ ...ein960, alt_geom: null });

		expect(withoutGeo.ok).toBe(true);
		if (withoutGeo.ok) {
			expect(withoutGeo.position.altitude_m).toBeCloseTo(4150 * 0.3048);
		}
	});

	it('treats alt_baro "ground" as on_ground true with no barometric altitude', () => {
		const result = normalizeAdsbfiRecord({ ...ein960, alt_baro: 'ground', alt_geom: 125 });

		expect(result.ok).toBe(true);
		if (result.ok) {
			expect(result.position.baro_altitude_m).toBeNull();
			expect(result.position.geo_altitude_m).toBeCloseTo(125 * 0.3048);
			expect(result.position.altitude_m).toBeCloseTo(125 * 0.3048);
			expect(result.position.on_ground).toBe(true);
		}
	});
});

describe('normalizeByProvider', () => {
	it('dispatches adsbfi payloads to the adsb.fi mapping', () => {
		expect(normalizeByProvider('adsbfi', ein960)).toMatchObject({
			ok: true,
			position: { provider: 'adsbfi' },
		});
	});

	it('dispatches opensky payloads to the OpenSky mapping', () => {
		const opensky = { icao24: 'abc123', lat: 37.6, lon: -122.3, time_position: 1_700_000_000 };
		expect(normalizeByProvider('opensky', opensky)).toMatchObject({
			ok: true,
			position: { provider: 'opensky', timestamp_ms: 1_700_000_000_000 },
		});
	});
});
