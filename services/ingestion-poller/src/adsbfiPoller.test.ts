import { afterEach, describe, expect, it, vi } from 'vitest';
import {
	fetchAdsbfiCycle,
	fetchAdsbfiResponse,
	nextDelayMs,
	splitAdsbfiResponse,
	toResponseNowMs,
} from './adsbfiPoller.js';

const BOX = { lamin: 36.9, lomin: -122.8, lamax: 38.1, lomax: -121.5 };

// `now` taken from a real adsb.fi /api/v3 response captured on 2026-09-23.
const REAL_NOW = 1790127082000;

describe('toResponseNowMs', () => {
	it('accepts the epoch-millisecond `now` of a real adsb.fi response unchanged', () => {
		expect(toResponseNowMs(REAL_NOW)).toBe(REAL_NOW);
	});

	it.each([
		['epoch seconds', 1790127082],
		['a string', '1790127082000'],
		['missing', undefined],
		['not finite', Number.NaN],
	])('rejects a `now` that is not epoch milliseconds: %s', (_label, now) => {
		expect(() => toResponseNowMs(now)).toThrow(/not epoch milliseconds/);
	});
});

describe('splitAdsbfiResponse', () => {
	const inBox = { hex: '4CAA40', lat: 37.69, lon: -122.52, seen_pos: 0.4 };
	const outsideBox = { hex: 'a1b2c3', lat: 38.5, lon: -122.0, seen_pos: 1 };
	const nonIcao = { hex: '~2b3cf6', lat: 37.6, lon: -122.3, seen_pos: 1 };
	const noPosition = { hex: 'abcdef', seen: 3 };

	const result = splitAdsbfiResponse(
		{ now: REAL_NOW, ac: [inBox, outsideBox, nonIcao, noPosition] },
		BOX,
		1790127082300,
	);

	it('publishes only ICAO aircraft inside the box, keyed by lowercase ICAO24', () => {
		expect(result.messages.map((m) => m.key)).toEqual(['4caa40']);
	});

	it('counts every skipped aircraft by reason', () => {
		expect(result).toMatchObject({
			total: 4,
			skippedNonIcao: 1,
			skippedOutsideBox: 1,
			skippedNoPosition: 1,
		});
	});

	it('wraps each aircraft in the adsbfi envelope with the response time and fetch time', () => {
		expect(JSON.parse(result.messages[0]!.value)).toEqual({
			provider: 'adsbfi',
			payload: { ...inBox, response_now_ms: REAL_NOW, fetched_at_ms: 1790127082300 },
		});
	});

	it('rejects the whole response when `now` is unusable, rather than publishing untimed records', () => {
		expect(() => splitAdsbfiResponse({ now: 1790127082, ac: [inBox] }, BOX, 0)).toThrow();
	});
});

describe('nextDelayMs', () => {
	const INTERVAL = 2_000;
	const BASE = 2_000;
	const MAX = 60_000;

	it('uses the normal interval when nothing has failed', () => {
		expect(nextDelayMs(0, INTERVAL, BASE, MAX, () => 0.5)).toBe(INTERVAL);
	});

	it('grows the jitter ceiling exponentially with consecutive failures', () => {
		// random() just under 1 approaches the ceiling: base * 2^(n-1)
		expect(nextDelayMs(3, INTERVAL, BASE, MAX, () => 0.999)).toBe(Math.floor(0.999 * 8_000));
	});

	it('caps the ceiling at the configured maximum', () => {
		expect(nextDelayMs(20, INTERVAL, BASE, MAX, () => 0.999)).toBe(Math.floor(0.999 * MAX));
	});

	it('never retries sooner than the normal poll interval, however low the jitter', () => {
		for (const failures of [1, 2, 5, 20]) {
			expect(nextDelayMs(failures, INTERVAL, BASE, MAX, () => 0)).toBe(INTERVAL);
		}
	});
});

// ---- Failure classes for provider health (CP3d) --------------------------------

describe('fetchAdsbfiResponse: last_error classes', () => {
	const quiet = () => {};
	afterEach(() => {
		vi.unstubAllGlobals();
	});
	const stub = (impl: () => Promise<Response>) => vi.stubGlobal('fetch', vi.fn(impl));

	it('returns the split response on success, including zero aircraft', async () => {
		stub(async () => new Response(JSON.stringify({ now: 1790365486000, ac: [] })));
		const result = await fetchAdsbfiResponse(quiet);
		expect('error' in result).toBe(false);
	});

	it('classifies the failure shapes observed live', async () => {
		stub(async () => new Response('', { status: 400 })); // the bad-path response
		expect(await fetchAdsbfiResponse(quiet)).toEqual({ error: 'http_400' });
		stub(async () => new Response('', { status: 429 }));
		expect(await fetchAdsbfiResponse(quiet)).toEqual({ error: 'rate_limited' });
		stub(async () => {
			throw Object.assign(new Error('The operation was aborted due to timeout'), {
				name: 'TimeoutError',
			});
		});
		expect(await fetchAdsbfiResponse(quiet)).toEqual({ error: 'timeout' });
		stub(async () => new Response(JSON.stringify({ ac: [] })));
		expect(await fetchAdsbfiResponse(quiet)).toEqual({
			error: 'validation: adsb.fi response "now" is not epoch milliseconds: undefined',
		});
	});

	it('keeps the legacy wrapper returning null on failure', async () => {
		stub(async () => new Response('', { status: 503 }));
		expect(await fetchAdsbfiCycle(quiet)).toBeNull();
	});
});
