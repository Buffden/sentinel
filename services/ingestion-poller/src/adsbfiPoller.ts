// adsb.fi adapter for the ingestion coordinator.
//
// One request to adsb.fi's public circle endpoint: fetch, validate, filter to
// the monitored box, and map ICAO-addressed aircraft into adsb.raw envelopes.
// It never publishes and never schedules; the coordinator owns authority,
// cadence, backoff and Kafka delivery.
//
// The response's top-level `now` is preserved because adsb.fi's seen/seen_pos
// values are relative to it. Non-ICAO track addresses (prefixed with "~") are
// outside Sentinel's canonical aircraft identity and are skipped here.

import { config } from './config.js';
import { adsbRawEnvelope } from './envelope.js';
import { classifyRequestError } from './providerHealth.js';

const USER_AGENT = 'sentinel-ingestion-poller/0.1 (portfolio project)';

const ADSBFI_URL =
	`https://opendata.adsb.fi/api/v3/lat/${config.ADSBFI_CENTER_LAT}` +
	`/lon/${config.ADSBFI_CENTER_LON}/dist/${config.ADSBFI_RADIUS_NM}`;

// ---- Pure helpers (unit-tested) ---------------------------------------------

// Plausible range for an epoch-milliseconds timestamp: 2017-07 to 2100-01.
// A seconds value (about 1.8e9) or a garbage value falls outside it.
const MIN_EPOCH_MS = 1_500_000_000_000;
const MAX_EPOCH_MS = 4_102_444_800_000;

// adsb.fi's top-level `now` is epoch milliseconds (ADS-B Exchange v2 format;
// observed as 1790127082000 in a real response). The unit is asserted here
// rather than assumed downstream: anything that is not a plausible epoch-ms
// number fails the whole response instead of producing wrong event times.
export function toResponseNowMs(now: unknown): number {
	if (
		typeof now !== 'number' ||
		!Number.isFinite(now) ||
		now < MIN_EPOCH_MS ||
		now > MAX_EPOCH_MS
	) {
		throw new Error(`adsb.fi response "now" is not epoch milliseconds: ${JSON.stringify(now)}`);
	}
	return now;
}

export interface Box {
	lamin: number;
	lomin: number;
	lamax: number;
	lomax: number;
}

export interface SplitResult {
	messages: { key: string; value: string }[];
	// The response's top-level `now`, already validated as epoch milliseconds.
	// The ingestion coordinator uses it to confirm the feed is advancing.
	responseNowMs: number;
	total: number;
	skippedNonIcao: number;
	skippedNoPosition: number;
	skippedOutsideBox: number;
}

// Turns one adsb.fi response body into enveloped adsb.raw messages.
export function splitAdsbfiResponse(body: unknown, box: Box, fetchedAtMs: number): SplitResult {
	if (typeof body !== 'object' || body === null) {
		throw new Error('adsb.fi response is not a JSON object');
	}
	const response = body as Record<string, unknown>;
	const responseNowMs = toResponseNowMs(response['now']);
	const aircraft = Array.isArray(response['ac']) ? response['ac'] : [];

	const result: SplitResult = {
		messages: [],
		responseNowMs,
		total: aircraft.length,
		skippedNonIcao: 0,
		skippedNoPosition: 0,
		skippedOutsideBox: 0,
	};

	for (const item of aircraft) {
		if (typeof item !== 'object' || item === null) continue;
		const ac = item as Record<string, unknown>;
		const hex = ac['hex'];
		if (typeof hex !== 'string' || hex === '') continue;
		if (hex.startsWith('~')) {
			result.skippedNonIcao++;
			continue;
		}
		const lat = ac['lat'];
		const lon = ac['lon'];
		if (typeof lat !== 'number' || typeof lon !== 'number') {
			result.skippedNoPosition++;
			continue;
		}
		if (lat < box.lamin || lat > box.lamax || lon < box.lomin || lon > box.lomax) {
			result.skippedOutsideBox++;
			continue;
		}
		result.messages.push({
			key: hex.toLowerCase(),
			value: adsbRawEnvelope('adsbfi', {
				...ac,
				response_now_ms: responseNowMs,
				fetched_at_ms: fetchedAtMs,
			}),
		});
	}
	return result;
}

// ---- Logging ---------------------------------------------------------------

export type Log = (
	level: 'info' | 'warn' | 'error',
	message: string,
	extra?: Record<string, unknown>,
) => void;

// ---- Poll cycle ------------------------------------------------------------

const BOX: Box = {
	lamin: config.ADSBFI_BOX_LAMIN,
	lomin: config.ADSBFI_BOX_LOMIN,
	lamax: config.ADSBFI_BOX_LAMAX,
	lomax: config.ADSBFI_BOX_LOMAX,
};

// Fetch and split one adsb.fi response, without publishing. A failure is
// returned as its last_error class (providerHealth.ts), already logged:
// `timeout`, `network:<code>`, `rate_limited`, `http_<status>` or
// `validation: <message>`. Split out from publishing so the ingestion
// coordinator can record provider health and check it still holds its lease
// between the two steps.
export interface AdsbfiFetchFailure {
	error: string;
}

export async function fetchAdsbfiResponse(logFn: Log): Promise<SplitResult | AdsbfiFetchFailure> {
	const fetchedAtMs = Date.now();
	let response: Response;
	try {
		response = await fetch(ADSBFI_URL, {
			headers: { Accept: 'application/json', 'User-Agent': USER_AGENT },
			signal: AbortSignal.timeout(config.ADSBFI_FETCH_TIMEOUT_MS),
		});
	} catch (err) {
		logFn('warn', 'adsb.fi request failed', {
			error: err instanceof Error ? err.message : String(err),
		});
		return { error: classifyRequestError(err) };
	}

	if (response.status === 429) {
		logFn('warn', 'adsb.fi rate limited (429, no retry time given)');
		return { error: 'rate_limited' };
	}
	if (!response.ok) {
		logFn('warn', 'adsb.fi returned an error status', { http_status: response.status });
		return { error: `http_${response.status}` };
	}

	try {
		return splitAdsbfiResponse(await response.json(), BOX, fetchedAtMs);
	} catch (err) {
		const message = err instanceof Error ? err.message : String(err);
		logFn('error', 'adsb.fi response rejected', { error: message });
		return { error: `validation: ${message}` };
	}
}
