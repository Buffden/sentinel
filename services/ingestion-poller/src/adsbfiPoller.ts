// adsb.fi ingestion poller: Sentinel's regional primary live source (ADR-020).
//
// Polls adsb.fi's public circle endpoint on a fixed cadence, keeps aircraft
// inside the monitored box, and publishes one adsb.raw message per aircraft in
// the { provider: 'adsbfi', payload } envelope (ADR-021).
//
// Only one live provider is authoritative at a time. This poller and the
// OpenSky poller are not run together; automatic switching is Phase 10 CP3.
//
// Responsibility boundary: like the OpenSky poller, this may split a provider
// response into per-aircraft records and filter by area. Field mapping, units,
// validation and DLQ handling belong to the Position Consumer.
//
// Payload context kept from the response (ADR-021):
//   response_now_ms  the response's top-level `now`. adsb.fi's seen_pos and
//                    seen are seconds before it, so without it a split-out
//                    record cannot be given a deterministic event time.
//   fetched_at_ms    when this poll ran; processing time, never event time.
//
// Identity: only ICAO-addressed aircraft are published, keyed by lowercase
// ICAO24. Non-ICAO track addresses (starting with "~") are outside the CP1
// canonical identity model: skipped here and counted in the poll log, never
// published or sent to the DLQ.
//
// Rate limiting: adsb.fi allows one request per second and sends no
// rate-limit headers; a 429 carries no retry time. Failed cycles back off with
// bounded exponential backoff and full jitter, never faster than the normal
// poll interval.

import { fileURLToPath } from 'node:url';
import { Kafka, Partitioners } from 'kafkajs';
import { config } from './config.js';
import { adsbRawEnvelope } from './envelope.js';

const TOPIC = 'adsb.raw';

// adsb.fi's Cloudflare front end rejects default client user agents (observed
// as HTTP 403, error 1010, in the provider experiment).
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

// Delay before the next request. With no failures it is the normal interval.
// After n consecutive failures it is a random value in [0, min(max, base * 2^(n-1))]
// (full jitter), floored at the normal interval so a retry is never sooner
// than normal polling.
export function nextDelayMs(
	consecutiveFailures: number,
	intervalMs: number,
	baseMs: number,
	maxMs: number,
	random: () => number = Math.random,
): number {
	if (consecutiveFailures <= 0) return intervalMs;
	const ceiling = Math.min(maxMs, baseMs * 2 ** (consecutiveFailures - 1));
	return Math.max(intervalMs, Math.floor(random() * ceiling));
}

// ---- Kafka setup -----------------------------------------------------------

const kafka = new Kafka({
	clientId: 'ingestion-poller-adsbfi',
	brokers: config.KAFKA_BROKERS,
	logLevel: 0,
});

const producer = kafka.producer({
	// Same partitioner as the OpenSky poller, so a given ICAO24 key maps to
	// the same partition whichever provider published it.
	createPartitioner: Partitioners.LegacyPartitioner,
});

// ---- Logging ---------------------------------------------------------------

export type Log = (
	level: 'info' | 'warn' | 'error',
	message: string,
	extra?: Record<string, unknown>,
) => void;

function log(
	level: 'info' | 'warn' | 'error',
	message: string,
	extra?: Record<string, unknown>,
): void {
	process.stdout.write(
		JSON.stringify({
			timestamp: new Date().toISOString(),
			level,
			service: 'ingestion-poller',
			provider: 'adsbfi',
			message,
			...extra,
		}) + '\n',
	);
}

// ---- Poll cycle ------------------------------------------------------------

const BOX: Box = {
	lamin: config.ADSBFI_BOX_LAMIN,
	lomin: config.ADSBFI_BOX_LOMIN,
	lamax: config.ADSBFI_BOX_LAMAX,
	lomax: config.ADSBFI_BOX_LOMAX,
};

// Fetch and split one adsb.fi response, without publishing. Returns null when
// the request or response failed (already logged). Split out from publishing
// so the ingestion coordinator can check it still holds its lease between the
// two steps.
export async function fetchAdsbfiCycle(logFn: Log): Promise<SplitResult | null> {
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
		return null;
	}

	if (response.status === 429) {
		logFn('warn', 'adsb.fi rate limited (429, no retry time given)');
		return null;
	}
	if (!response.ok) {
		logFn('warn', 'adsb.fi returned an error status', { http_status: response.status });
		return null;
	}

	try {
		return splitAdsbfiResponse(await response.json(), BOX, fetchedAtMs);
	} catch (err) {
		logFn('error', 'adsb.fi response rejected', {
			error: err instanceof Error ? err.message : String(err),
		});
		return null;
	}
}

export function pollCycleSummary(split: SplitResult, firstOffset: string): Record<string, unknown> {
	return {
		aircraft_in_response: split.total,
		published: split.messages.length,
		skipped_non_icao: split.skippedNonIcao,
		skipped_no_position: split.skippedNoPosition,
		skipped_outside_box: split.skippedOutsideBox,
		topic: TOPIC,
		first_offset: firstOffset,
	};
}

// Returns true when the cycle succeeded, false when it should count as a failure.
async function pollOnce(): Promise<boolean> {
	const split = await fetchAdsbfiCycle(log);
	if (split === null) return false;

	let firstOffset = 'none';
	if (split.messages.length > 0) {
		const results = await producer.send({ topic: TOPIC, messages: split.messages });
		firstOffset = results[0]?.baseOffset ?? 'unknown';
	}

	log('info', 'poll cycle complete', pollCycleSummary(split, firstOffset));
	return true;
}

// ---- Poll loop -------------------------------------------------------------

let pollTimeout: ReturnType<typeof setTimeout> | null = null;
let stopping = false;
let consecutiveFailures = 0;

function scheduleNextPoll(): void {
	if (stopping) return;
	const delay = nextDelayMs(
		consecutiveFailures,
		config.ADSBFI_POLL_INTERVAL_MS,
		config.ADSBFI_BACKOFF_BASE_MS,
		config.ADSBFI_BACKOFF_MAX_MS,
	);
	if (consecutiveFailures > 0) {
		log('warn', 'backing off before next request', {
			consecutive_failures: consecutiveFailures,
			delay_ms: delay,
		});
	}
	pollTimeout = setTimeout(() => {
		void pollOnce()
			.then((ok) => {
				if (ok && consecutiveFailures > 0) {
					log('info', 'adsb.fi recovered', { after_failures: consecutiveFailures });
				}
				consecutiveFailures = ok ? 0 : consecutiveFailures + 1;
			})
			.catch((err: unknown) => {
				// Kafka publish errors land here; count them as a failed cycle.
				consecutiveFailures++;
				log('error', 'poll cycle error', {
					error: err instanceof Error ? err.message : String(err),
				});
			})
			.finally(() => scheduleNextPoll());
	}, delay);
}

// ---- Entry point -----------------------------------------------------------

async function run(): Promise<void> {
	await producer.connect();
	log('info', 'poller starting', {
		url: ADSBFI_URL,
		box: BOX,
		poll_interval_ms: config.ADSBFI_POLL_INTERVAL_MS,
		backoff_base_ms: config.ADSBFI_BACKOFF_BASE_MS,
		backoff_max_ms: config.ADSBFI_BACKOFF_MAX_MS,
	});
	scheduleNextPoll();
}

async function shutdown(signal: string): Promise<void> {
	stopping = true;
	if (pollTimeout !== null) clearTimeout(pollTimeout);
	log('info', 'shutdown initiated', { signal });
	await producer.disconnect();
	log('info', 'producer disconnected');
	process.exit(0);
}

// Only run when executed directly (`npm run poll:adsbfi`), not when imported by tests.
if (process.argv[1] === fileURLToPath(import.meta.url)) {
	process.on('SIGINT', () => {
		shutdown('SIGINT').catch(() => process.exit(1));
	});
	process.on('SIGTERM', () => {
		shutdown('SIGTERM').catch(() => process.exit(1));
	});
	run().catch((err: unknown) => {
		log('error', 'poller failed', { error: err instanceof Error ? err.message : String(err) });
		process.exit(1);
	});
}
