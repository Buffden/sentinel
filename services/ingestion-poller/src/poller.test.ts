import { describe, expect, it } from 'vitest';
import {
	type CycleOutcome,
	NOT_PAUSED,
	type PollPlanOptions,
	type RateLimitState,
	creditsPerCall,
	fallbackBackoffMs,
	mapStateVector,
	parseCreditsRemaining,
	parseRetryAfterSeconds,
	planNextPoll,
	projectDailyBudget,
	rateLimitLogLine,
	fetchOpenskyCycle,
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

// ---- Credit budget -----------------------------------------------------------

const SF_BAY_BOX = { lamin: 36.9, lomin: -122.8, lamax: 38.1, lomax: -121.5 };
const OLD_EUROPE_BOX = { lamin: 49, lomin: -8, lamax: 61, lomax: 10 };

describe('creditsPerCall', () => {
	it('follows the ADR-020 area bands, inclusive at each upper edge', () => {
		expect(creditsPerCall(1.56)).toBe(1);
		expect(creditsPerCall(25)).toBe(1);
		expect(creditsPerCall(25.01)).toBe(2);
		expect(creditsPerCall(100)).toBe(2);
		expect(creditsPerCall(216)).toBe(3);
		expect(creditsPerCall(400)).toBe(3);
		expect(creditsPerCall(401)).toBe(4);
	});
});

describe('projectDailyBudget', () => {
	it('keeps the default SF Bay box at 25 s inside the logged-in budget', () => {
		const p = projectDailyBudget(SF_BAY_BOX, 25_000, true);
		expect(p.area_square_degrees).toBe(1.56);
		expect(p.credits_per_call).toBe(1);
		expect(p.calls_per_day).toBe(3_456);
		expect(p.projected_daily_credits).toBe(3_456);
		expect(p.daily_budget).toBe(4_000);
		expect(p.within_budget).toBe(true);
		expect(p.min_sustainable_interval_ms).toBe(21_600);
	});

	it('flags the same settings as over budget when anonymous', () => {
		const p = projectDailyBudget(SF_BAY_BOX, 25_000, false);
		expect(p.daily_budget).toBe(400);
		expect(p.within_budget).toBe(false);
		// 400 calls a day: one every 216 s.
		expect(p.min_sustainable_interval_ms).toBe(216_000);
	});

	it('reproduces the old default overspend: 3 credits every 10 s', () => {
		const p = projectDailyBudget(OLD_EUROPE_BOX, 10_000, true);
		expect(p.credits_per_call).toBe(3);
		expect(p.projected_daily_credits).toBe(25_920);
		expect(p.within_budget).toBe(false);
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

// ---- Fallback backoff ------------------------------------------------------------

describe('fallbackBackoffMs', () => {
	const BASE = 60_000;
	const MAX = 900_000;
	const lowest = () => 0;
	// Just under 1, the highest value Math.random can return.
	const highest = () => 0.999999;

	it('waits exactly the 60 s floor on the first fallback', () => {
		expect(fallbackBackoffMs(1, BASE, MAX, lowest)).toBe(60_000);
		expect(fallbackBackoffMs(1, BASE, MAX, highest)).toBe(60_000);
	});

	it('doubles the ceiling each time, never below the floor', () => {
		expect(fallbackBackoffMs(2, BASE, MAX, lowest)).toBe(60_000);
		expect(fallbackBackoffMs(2, BASE, MAX, highest)).toBe(119_999);
		expect(fallbackBackoffMs(3, BASE, MAX, highest)).toBe(239_999);
		expect(fallbackBackoffMs(4, BASE, MAX, highest)).toBe(479_999);
	});

	it('stops growing at the 15 min cap', () => {
		expect(fallbackBackoffMs(5, BASE, MAX, highest)).toBe(899_999);
		expect(fallbackBackoffMs(50, BASE, MAX, highest)).toBe(899_999);
		expect(fallbackBackoffMs(50, BASE, MAX, lowest)).toBe(60_000);
	});
});

// ---- Pause and resume ------------------------------------------------------------

describe('planNextPoll', () => {
	const OPTS: PollPlanOptions = {
		intervalMs: 25_000,
		backoffBaseMs: 60_000,
		backoffMaxMs: 900_000,
	};
	const T0 = Date.parse('2026-09-23T20:43:04Z');
	const ok: CycleOutcome = { kind: 'ok' };
	const failed: CycleOutcome = { kind: 'failed' };
	const observed429: CycleOutcome = {
		kind: 'rate_limited',
		retryAfterSeconds: 31_952,
		retryAfterHeader: '31952',
	};
	const headerless429: CycleOutcome = {
		kind: 'rate_limited',
		retryAfterSeconds: null,
		retryAfterHeader: null,
	};
	const highest = () => 0.999999;

	it('polls at the normal interval while healthy', () => {
		const plan = planNextPoll(NOT_PAUSED, ok, T0, OPTS);
		expect(plan.delayMs).toBe(25_000);
		expect(plan.event).toBeNull();
		expect(plan.state).toEqual(NOT_PAUSED);
	});

	it('pauses for exactly the retry time on the observed 429', () => {
		const plan = planNextPoll(NOT_PAUSED, observed429, T0, OPTS);
		expect(plan.event).toBe('paused');
		expect(plan.delaySource).toBe('retry_header');
		expect(plan.delayMs).toBe(31_952_000);
		expect(plan.state.pausedSinceMs).toBe(T0);
		expect(plan.state.rateLimitedResponses).toBe(1);
		// A valid header never advances the fallback count.
		expect(plan.state.consecutiveFallbacks).toBe(0);
	});

	it('never retries sooner than the normal interval, even for a 0 s retry', () => {
		const zero: CycleOutcome = {
			kind: 'rate_limited',
			retryAfterSeconds: 0,
			retryAfterHeader: '0',
		};
		expect(planNextPoll(NOT_PAUSED, zero, T0, OPTS).delayMs).toBe(25_000);
	});

	it('keeps a nonsensical retry time within what setTimeout can wait', () => {
		const huge: CycleOutcome = {
			kind: 'rate_limited',
			retryAfterSeconds: 10_000_000,
			retryAfterHeader: '10000000',
		};
		expect(planNextPoll(NOT_PAUSED, huge, T0, OPTS).delayMs).toBe(2_147_483_647);
	});

	it('logs a resume once, with how long the pause lasted, and resets', () => {
		const paused = planNextPoll(NOT_PAUSED, observed429, T0, OPTS);
		const resumeAt = T0 + 31_952_000;
		const resumed = planNextPoll(paused.state, ok, resumeAt, OPTS);
		expect(resumed.event).toBe('resumed');
		expect(resumed.pausedForMs).toBe(31_952_000);
		expect(resumed.delayMs).toBe(25_000);
		expect(resumed.state).toEqual(NOT_PAUSED);

		// The next success is ordinary: no second resume.
		expect(planNextPoll(resumed.state, ok, resumeAt + 25_000, OPTS).event).toBeNull();
	});

	it('marks a second 429 during a pause as an extension, not a new pause', () => {
		const first = planNextPoll(NOT_PAUSED, observed429, T0, OPTS);
		const second = planNextPoll(first.state, observed429, T0 + 31_952_000, OPTS);
		expect(second.event).toBe('pause_extended');
		expect(second.state.pausedSinceMs).toBe(T0);
		expect(second.state.rateLimitedResponses).toBe(2);
	});

	it('backs off 60 s, then up to 120 s, 240 s... while headers stay missing', () => {
		let state: RateLimitState = NOT_PAUSED;
		const delays: number[] = [];
		for (let i = 0; i < 6; i++) {
			const plan = planNextPoll(state, headerless429, T0, OPTS, highest);
			expect(plan.delaySource).toBe('fallback_backoff');
			delays.push(plan.delayMs);
			state = plan.state;
		}
		expect(delays).toEqual([60_000, 119_999, 239_999, 479_999, 899_999, 899_999]);
		expect(state.consecutiveFallbacks).toBe(6);
	});

	it('resets the fallback progression after the first success', () => {
		let state: RateLimitState = NOT_PAUSED;
		for (let i = 0; i < 4; i++) state = planNextPoll(state, headerless429, T0, OPTS, highest).state;
		state = planNextPoll(state, ok, T0, OPTS).state;
		const next = planNextPoll(state, headerless429, T0, OPTS, highest);
		expect(next.event).toBe('paused');
		expect(next.delayMs).toBe(60_000);
		expect(next.state.consecutiveFallbacks).toBe(1);
	});

	it('does not end a pause or reset backoff on a network error or 5xx', () => {
		let state: RateLimitState = NOT_PAUSED;
		for (let i = 0; i < 3; i++) state = planNextPoll(state, headerless429, T0, OPTS, highest).state;
		const plan = planNextPoll(state, failed, T0, OPTS);
		expect(plan.event).toBeNull();
		expect(plan.delayMs).toBe(25_000);
		expect(plan.state).toEqual(state);
	});
});

describe('rateLimitLogLine', () => {
	const OPTS: PollPlanOptions = {
		intervalMs: 25_000,
		backoffBaseMs: 60_000,
		backoffMaxMs: 900_000,
	};
	const T0 = Date.parse('2026-09-23T20:43:04Z');
	const observed429: CycleOutcome = {
		kind: 'rate_limited',
		retryAfterSeconds: 31_952,
		retryAfterHeader: '31952',
	};

	it('states why the poller paused and when it will try again', () => {
		const plan = planNextPoll(NOT_PAUSED, observed429, T0, OPTS);
		const line = rateLimitLogLine(plan, observed429, T0);
		expect(line).toEqual({
			level: 'warn',
			message: 'opensky rate limited, pausing requests',
			fields: {
				http_status: 429,
				delay_source: 'retry_header',
				retry_after_header: '31952',
				delay_ms: 31_952_000,
				// The refill moment computed from the real run.
				resume_at: '2026-09-24T05:35:36.000Z',
				rate_limited_responses: 1,
			},
		});
	});

	it('includes the fallback count and raw header when the header was unusable', () => {
		const bad: CycleOutcome = {
			kind: 'rate_limited',
			retryAfterSeconds: null,
			retryAfterHeader: 'soon',
		};
		const plan = planNextPoll(NOT_PAUSED, bad, T0, OPTS, () => 0);
		const line = rateLimitLogLine(plan, bad, T0);
		expect(line?.fields).toMatchObject({
			delay_source: 'fallback_backoff',
			retry_after_header: 'soon',
			delay_ms: 60_000,
			consecutive_fallbacks: 1,
		});
	});

	it('logs the resume at info with the pause length', () => {
		const paused = planNextPoll(NOT_PAUSED, observed429, T0, OPTS);
		const ok: CycleOutcome = { kind: 'ok' };
		const resumed = planNextPoll(paused.state, ok, T0 + 31_952_000, OPTS);
		expect(rateLimitLogLine(resumed, ok, T0 + 31_952_000)).toEqual({
			level: 'info',
			message: 'opensky resumed after rate limit',
			fields: { paused_for_ms: 31_952_000 },
		});
	});

	it('logs nothing for an ordinary cycle', () => {
		const ok: CycleOutcome = { kind: 'ok' };
		expect(rateLimitLogLine(planNextPoll(NOT_PAUSED, ok, T0, OPTS), ok, T0)).toBeNull();
	});
});

// ---- OpenSky health check (CP3d) ---------------------------------------------

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
