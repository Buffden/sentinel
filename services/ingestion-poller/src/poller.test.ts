import { describe, expect, it } from 'vitest';
import { mapStateVector } from './poller.js';

// One real OpenSky state vector, positional per the API contract documented
// in poller.ts. Index 12 (sensors) intentionally included to prove it maps
// even though nothing downstream currently reads it.
const FULL_STATE_VECTOR: unknown[] = [
	'400ac2', // 0 icao24
	'BAW123  ', // 1 callsign (provider padding preserved verbatim)
	'United Kingdom', // 2 origin_country
	1_700_000_000, // 3 time_position
	1_700_000_005, // 4 last_contact
	-0.1, // 5 lon
	51.5, // 6 lat
	10000, // 7 baro_altitude
	false, // 8 on_ground
	230.5, // 9 velocity
	270, // 10 true_track
	-1.5, // 11 vertical_rate
	[1, 2, 3], // 12 sensors
	10200, // 13 geo_altitude
	'7500', // 14 squawk
	false, // 15 spi
	0, // 16 position_source
	3, // 17 category (fixed_wing range)
];

describe('mapStateVector', () => {
	it('maps every positional index to its named field, verbatim', () => {
		const fetchedAtMs = 1_700_000_010_000;
		const event = mapStateVector(FULL_STATE_VECTOR, fetchedAtMs);

		expect(event).toEqual({
			icao24: '400ac2',
			callsign: 'BAW123  ',
			origin_country: 'United Kingdom',
			time_position: 1_700_000_000,
			last_contact: 1_700_000_005,
			lon: -0.1,
			lat: 51.5,
			baro_altitude: 10000,
			on_ground: false,
			velocity: 230.5,
			true_track: 270,
			vertical_rate: -1.5,
			sensors: [1, 2, 3],
			geo_altitude: 10200,
			squawk: '7500',
			spi: false,
			position_source: 0,
			category: 3,
			fetched_at_ms: fetchedAtMs,
		});
	});

	it("does not trim or coerce callsign — that is the Position Consumer's job", () => {
		const event = mapStateVector(FULL_STATE_VECTOR, 0);
		expect(event.callsign).toBe('BAW123  ');
	});

	it('defaults sensors to null when the field is absent (no extended sensor list)', () => {
		const withoutSensors = [...FULL_STATE_VECTOR];
		withoutSensors[12] = null;
		const event = mapStateVector(withoutSensors, 0);
		expect(event.sensors).toBeNull();
	});

	it('defaults category to null when the poller URL omitted extended=1', () => {
		const withoutCategory = [...FULL_STATE_VECTOR];
		withoutCategory[17] = undefined;
		const event = mapStateVector(withoutCategory, 0);
		expect(event.category).toBeNull();
	});

	it('defaults callsign to null when the state vector has no callsign', () => {
		const withoutCallsign = [...FULL_STATE_VECTOR];
		withoutCallsign[1] = null;
		const event = mapStateVector(withoutCallsign, 0);
		expect(event.callsign).toBeNull();
	});

	it('stamps fetched_at_ms as processing time, independent of time_position', () => {
		const event = mapStateVector(FULL_STATE_VECTOR, 1_700_000_999_000);
		expect(event.fetched_at_ms).toBe(1_700_000_999_000);
		expect(event.fetched_at_ms).not.toBe(event.time_position);
	});
});
