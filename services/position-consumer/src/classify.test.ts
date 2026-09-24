import { describe, expect, it } from 'vitest';
import { classifyAdsbRaw } from './classify.js';

// Shape of a record written by Sentinel's OpenSky poller before ADR-021.
const legacyOpenSky = {
	icao24: 'abc123',
	callsign: 'TEST1   ',
	origin_country: 'United States',
	time_position: 1_700_000_000,
	last_contact: 1_700_000_001,
	lon: -122.3,
	lat: 37.6,
	on_ground: false,
	position_source: 0,
	fetched_at_ms: 1_700_000_002_000,
};

describe('classifyAdsbRaw', () => {
	it('accepts an envelope with a known provider and returns only the payload', () => {
		const payload = { hex: '4caa40', lat: 37.6 };
		const c = classifyAdsbRaw(JSON.stringify({ provider: 'adsbfi', payload }));
		expect(c).toEqual({ ok: true, provider: 'adsbfi', payload, legacy: false });
	});

	it('accepts an OpenSky envelope', () => {
		const c = classifyAdsbRaw(JSON.stringify({ provider: 'opensky', payload: legacyOpenSky }));
		expect(c).toMatchObject({ ok: true, provider: 'opensky', legacy: false });
	});

	it('rejects an envelope naming a provider that has not been decided', () => {
		const c = classifyAdsbRaw(JSON.stringify({ provider: 'flightradar24', payload: {} }));
		expect(c).toMatchObject({ ok: false, kind: 'unknown_provider' });
	});

	it.each([
		['payload missing', { provider: 'adsbfi' }],
		['provider missing', { payload: { hex: 'abc' } }],
		['payload is an array', { provider: 'adsbfi', payload: [1] }],
		['payload is null', { provider: 'adsbfi', payload: null }],
		['provider is not a string', { provider: 7, payload: {} }],
		['extra envelope key', { provider: 'adsbfi', payload: {}, extra: true }],
	])('rejects a malformed envelope: %s', (_label, record) => {
		const c = classifyAdsbRaw(JSON.stringify(record));
		expect(c).toMatchObject({ ok: false, kind: 'invalid_envelope' });
	});

	it('classifies a legacy bare OpenSky poller record as opensky', () => {
		const c = classifyAdsbRaw(JSON.stringify(legacyOpenSky));
		expect(c).toEqual({ ok: true, provider: 'opensky', payload: legacyOpenSky, legacy: true });
	});

	it('keeps a legacy record with null position fields on the legacy path, for the normalizer to decide', () => {
		const c = classifyAdsbRaw(
			JSON.stringify({ ...legacyOpenSky, lat: null, lon: null, time_position: null }),
		);
		expect(c).toMatchObject({ ok: true, provider: 'opensky', legacy: true });
	});

	it('does not default a bare adsb.fi record to OpenSky', () => {
		const c = classifyAdsbRaw(
			JSON.stringify({ hex: '4caa40', lat: 37.6, lon: -122.5, seen_pos: 0.4 }),
		);
		expect(c).toMatchObject({ ok: false, kind: 'unidentified_provider' });
	});

	it.each([
		['fetched_at_ms missing', { fetched_at_ms: undefined }],
		['icao24 empty', { icao24: '' }],
		['time_position key absent', { time_position: undefined }],
	])('does not treat a near-miss bare record as legacy OpenSky: %s', (_label, overrides) => {
		const c = classifyAdsbRaw(JSON.stringify({ ...legacyOpenSky, ...overrides }));
		expect(c).toMatchObject({ ok: false, kind: 'unidentified_provider' });
	});

	it('reports non-JSON as parse_error', () => {
		expect(classifyAdsbRaw('{not json')).toMatchObject({ ok: false, kind: 'parse_error' });
	});

	it('reports a JSON array as parse_error, as before classification existed', () => {
		expect(classifyAdsbRaw('[1,2,3]')).toMatchObject({ ok: false, kind: 'parse_error' });
	});
});
