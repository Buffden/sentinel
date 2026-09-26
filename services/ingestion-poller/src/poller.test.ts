import { describe, expect, it } from 'vitest';
import {
	fetchOpenskyCycle,
	mapStateVector,
	parseCreditsRemaining,
	parseRetryAfterSeconds,
	validateOpenskyBody,
} from './poller.js';

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

// ---- Header parsing ------------------------------------------------------------

describe('parseRetryAfterSeconds', () => {
	it('reads the whole-number value OpenSky sent on the observed 429', () => {
		expect(parseRetryAfterSeconds('31952')).toBe(31_952);
		expect(parseRetryAfterSeconds(' 31939 ')).toBe(31_939);
		expect(parseRetryAfterSeconds('0')).toBe(0);
	});

	it('treats a missing or unusable header as absent, never as a guessed value', () => {
		expect(parseRetryAfterSeconds(null)).toBeNull();
		expect(parseRetryAfterSeconds('')).toBeNull();
		expect(parseRetryAfterSeconds('-5')).toBeNull();
		expect(parseRetryAfterSeconds('12.5')).toBeNull();
		expect(parseRetryAfterSeconds('soon')).toBeNull();
		expect(parseRetryAfterSeconds('99999999999999999999')).toBeNull();
	});
});

describe('parseCreditsRemaining', () => {
	it('reads the balance, including zero on the last allowed call', () => {
		expect(parseCreditsRemaining('399')).toBe(399);
		expect(parseCreditsRemaining('0')).toBe(0);
	});

	it('is null when absent, as on a 429', () => {
		expect(parseCreditsRemaining(null)).toBeNull();
	});
});

// ---- OpenSky adapter ---------------------------------------------

describe('validateOpenskyBody (ADR-022 section 3)', () => {
	it('accepts a numeric epoch-seconds time with states as an array or null', () => {
		// Shape observed live on 2026-09-25: time in seconds, 150 states.
		expect(validateOpenskyBody({ time: 1790365527, states: [] })).toBeNull();
		expect(validateOpenskyBody({ time: 1790365527, states: null })).toBeNull();
	});

	it('rejects a non-numeric time, other states values, and non-objects', () => {
		expect(validateOpenskyBody({ time: '1790365527', states: [] })).toBe(
			'time is not a finite number',
		);
		expect(validateOpenskyBody({ states: [] })).toBe('time is not a finite number');
		expect(validateOpenskyBody({ time: Number.NaN, states: [] })).toBe(
			'time is not a finite number',
		);
		expect(validateOpenskyBody({ time: 1790365527, states: {} })).toBe(
			'states is neither an array nor null',
		);
		expect(validateOpenskyBody({ time: 1790365527 })).toBe('states is neither an array nor null');
		expect(validateOpenskyBody([])).toBe('response is not a JSON object');
	});
});

describe('fetchOpenskyCycle: fetch, validate and map, never publish', () => {
	const anonymous = async () => null;
	const respond =
		(status: number, body: unknown, headers: Record<string, string> = {}) =>
		async () =>
			new Response(typeof body === 'string' ? body : JSON.stringify(body), { status, headers });

	it('a valid response is ok, with the credits header when present', async () => {
		const withCredits = respond(
			200,
			{ time: 1790365527, states: null },
			{ 'x-rate-limit-remaining': '399' },
		);
		expect(await fetchOpenskyCycle(anonymous, withCredits as typeof fetch)).toEqual({
			kind: 'ok',
			messages: [],
			responseTime: 1790365527,
			creditsRemaining: 399,
		});
		const without = respond(200, { time: 1790365527, states: [] });
		expect(await fetchOpenskyCycle(anonymous, without as typeof fetch)).toEqual({
			kind: 'ok',
			messages: [],
			responseTime: 1790365527,
			creditsRemaining: null,
		});
	});

	it('maps each state vector into the opensky envelope, keyed by ICAO24', async () => {
		const vector = [
			'ac34bf',
			'SWA1932 ',
			'United States',
			1790371084,
			1790371085,
			-122.3,
			37.6,
			1500,
			false,
			120,
			90,
			0,
			null,
			1600,
			'1234',
			false,
			0,
			3,
		];
		const result = await fetchOpenskyCycle(
			anonymous,
			respond(200, { time: 1790371086, states: [vector] }) as typeof fetch,
		);
		expect(result.kind).toBe('ok');
		if (result.kind !== 'ok') return;
		expect(result.messages).toHaveLength(1);
		expect(result.messages[0]!.key).toBe('ac34bf');
		const envelope = JSON.parse(result.messages[0]!.value) as {
			provider: string;
			payload: { icao24: string };
		};
		expect(envelope.provider).toBe('opensky');
		expect(envelope.payload.icao24).toBe('ac34bf');
	});

	it('a 429 carries its retry time, or null when the header is missing', async () => {
		// The retry value captured in CP2's real 429.
		const withRetry = respond(429, '', { 'x-rate-limit-retry-after-seconds': '31952' });
		expect(await fetchOpenskyCycle(anonymous, withRetry as typeof fetch)).toEqual({
			kind: 'rate_limited',
			retryAfterSeconds: 31952,
		});
		expect(await fetchOpenskyCycle(anonymous, respond(429, '') as typeof fetch)).toEqual({
			kind: 'rate_limited',
			retryAfterSeconds: null,
		});
	});

	it('classifies status, validation, network and token failures', async () => {
		const check = (fetchFn: unknown, getToken = anonymous) =>
			fetchOpenskyCycle(getToken, fetchFn as typeof fetch);
		expect(await check(respond(503, ''))).toEqual({ kind: 'failed', error: 'http_503' });
		expect(await check(respond(200, 'not json'))).toEqual({
			kind: 'failed',
			error: 'validation: response is not JSON',
		});
		expect(await check(respond(200, { time: 'x', states: [] }))).toEqual({
			kind: 'failed',
			error: 'validation: time is not a finite number',
		});
		const dns = async () => {
			throw Object.assign(new TypeError('fetch failed'), { cause: { code: 'ENOTFOUND' } });
		};
		expect(await check(dns)).toEqual({ kind: 'failed', error: 'network:ENOTFOUND' });
		const tokenFails = async () => {
			throw new Error('OpenSky token request failed: HTTP 401');
		};
		let fetched = false;
		const neverCalled = async () => {
			fetched = true;
			return new Response('{}');
		};
		expect(await check(neverCalled, tokenFails)).toEqual({
			kind: 'failed',
			error: 'auth: OpenSky token request failed: HTTP 401',
		});
		expect(fetched).toBe(false);
	});
});
