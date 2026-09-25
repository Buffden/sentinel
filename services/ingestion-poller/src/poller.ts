// OpenSky ingestion poller.
//
// Polls the OpenSky Network REST API on a fixed interval and publishes one
// adsb.raw Kafka message per state vector, keyed by icao24, wrapped in the
// { provider: 'opensky', payload } envelope (ADR-021).
//
// Responsibility boundary (ARCHITECTURE.md):
//   The poller may unwrap the provider response envelope and split it into
//   per-entity records. Field coercion, canonical naming, validation,
//   persistence, and DLQ handling belong to the Position Consumer.
//
// Field names follow OpenSky's own naming so the raw Kafka log faithfully
// represents the source. The Position Consumer maps them to the canonical
// schema.
//
// Message key: icao24 (ICAO 24-bit aircraft address).
//   This becomes entity_id in the canonical schema. Keying by icao24 ensures
//   that when adsb.raw is scaled to multiple partitions, all events for the
//   same aircraft land in the same partition and are consumed in arrival order.
//
// Rate limiting (ADR-020):
//   OpenSky limits access by a daily credit budget, not a request rate: 400
//   credits a day anonymous, 4,000 logged in. Each /states/all call costs 1 to
//   4 credits by box area. The default SF Bay box costs 1 credit, and the 25 s
//   default interval spends 3,456 credits a day. Startup logs this projection
//   and warns when it exceeds the budget.
//
//   Successful responses carry X-Rate-Limit-Remaining. When the budget is
//   spent OpenSky answers 429 with X-Rate-Limit-Retry-After-Seconds and no
//   remaining balance (observed 2026-09-23: balance 0, then a 429 with a retry
//   of 31,952 s, about 8.9 hours). The poller then makes no requests until the
//   retry time passes. A 429 without a usable retry header falls back to
//   bounded exponential backoff with jitter. The pause and the resume are each
//   logged once.
//
//   While paused, no OpenSky positions are published, so every OpenSky
//   aircraft falls silent. Telling that apart from aircraft going dark is
//   provider health (Phase 10 CP3), not this poller's job.
//
// Authentication:
//   OPENSKY_CLIENT_ID/OPENSKY_CLIENT_SECRET (optional) enable OAuth2
//   client-credentials auth, which raises the daily budget from 400 to 4,000
//   credits. Without them, requests are unauthenticated. `npm run poll` loads
//   them from this service's .env when that file exists.

import { fileURLToPath } from 'node:url';
import { Kafka, Partitioners } from 'kafkajs';
import { config } from './config.js';
import { adsbRawEnvelope } from './envelope.js';
import { classifyRequestError } from './providerHealth.js';

// extended=1 instructs OpenSky to include the category field (index 17 in the
// state vector). Without it, entity_subtype and provider_category are always
// null in the canonical schema. Must appear before the bounding-box params.
const OPENSKY_URL =
	`https://opensky-network.org/api/states/all` +
	`?extended=1` +
	`&lamin=${config.OPENSKY_LAMIN}&lomin=${config.OPENSKY_LOMIN}` +
	`&lamax=${config.OPENSKY_LAMAX}&lomax=${config.OPENSKY_LOMAX}`;

// ---- Types -----------------------------------------------------------------

// Provider-fidelity shape of one adsb.raw Kafka message.
// Field names follow OpenSky's own naming so the raw log faithfully represents
// the source. No trimming, coercion, or dropping of fields — that is the
// Position Consumer's job. The only addition is fetched_at_ms, an operational
// timestamp that captures when this poll cycle ran (processing time, not source
// event time). It documents the lag between OpenSky's update interval and Kafka
// delivery and is useful for operational monitoring.
export interface AdsbRawEvent {
	icao24: string;
	callsign: string | null; // preserved verbatim; Position Consumer normalises
	origin_country: string;
	time_position: number | null; // Unix seconds; source event time for this position
	last_contact: number; // Unix seconds; last transponder message received
	lon: number | null;
	lat: number | null;
	baro_altitude: number | null; // metres above mean sea level (barometric)
	on_ground: boolean;
	velocity: number | null; // m/s ground speed
	true_track: number | null; // degrees clockwise from north
	vertical_rate: number | null; // m/s; positive = climbing
	sensors: number[] | null; // receiver IDs that contributed to this state vector
	geo_altitude: number | null; // metres; GNSS altitude; may differ from baro
	squawk: string | null;
	spi: boolean;
	position_source: number; // 0=ADS-B, 1=ASTERIX, 2=MLAT, 3=FLARM
	category: number | null; // ADS-B emitter category; index 17; only with extended=1
	fetched_at_ms: number; // processing time of this poll cycle; NOT source event time
}

// ---- Credit budget (pure helpers, unit-tested) -------------------------------

// OpenSky's daily /states budgets (ADR-020). An active feeder account gets
// 8,000, but the poller cannot tell a feeder apart, so logged in means 4,000.
export const OPENSKY_DAILY_CREDITS_ANONYMOUS = 400;
export const OPENSKY_DAILY_CREDITS_AUTHENTICATED = 4_000;

const MS_PER_DAY = 86_400_000;

export interface Box {
	lamin: number;
	lomin: number;
	lamax: number;
	lomax: number;
}

export function boxAreaSquareDegrees(box: Box): number {
	return Math.abs(box.lamax - box.lamin) * Math.abs(box.lomax - box.lomin);
}

// Credits one /states/all call costs for a box of this area (ADR-020 table).
export function creditsPerCall(areaSquareDegrees: number): number {
	if (areaSquareDegrees <= 25) return 1;
	if (areaSquareDegrees <= 100) return 2;
	if (areaSquareDegrees <= 400) return 3;
	return 4;
}

export interface BudgetProjection {
	area_square_degrees: number;
	credits_per_call: number;
	calls_per_day: number;
	projected_daily_credits: number;
	daily_budget: number;
	within_budget: boolean;
	// The shortest interval this box can sustain all day on this budget.
	min_sustainable_interval_ms: number;
}

export function projectDailyBudget(
	box: Box,
	intervalMs: number,
	authenticated: boolean,
): BudgetProjection {
	const area = boxAreaSquareDegrees(box);
	const cost = creditsPerCall(area);
	const callsPerDay = Math.ceil(MS_PER_DAY / intervalMs);
	const budget = authenticated
		? OPENSKY_DAILY_CREDITS_AUTHENTICATED
		: OPENSKY_DAILY_CREDITS_ANONYMOUS;
	const projected = callsPerDay * cost;
	return {
		area_square_degrees: Math.round(area * 100) / 100,
		credits_per_call: cost,
		calls_per_day: callsPerDay,
		projected_daily_credits: projected,
		daily_budget: budget,
		within_budget: projected <= budget,
		min_sustainable_interval_ms: Math.ceil((MS_PER_DAY * cost) / budget),
	};
}

// ---- Rate-limit handling (pure helpers, unit-tested) --------------------------

// Both OpenSky rate-limit headers are whole numbers. Anything else (missing,
// empty, negative, fractional, text) is treated as absent rather than guessed.
function parseWholeNumberHeader(value: string | null): number | null {
	if (value === null) return null;
	const trimmed = value.trim();
	if (!/^\d+$/.test(trimmed)) return null;
	const n = Number(trimmed);
	return Number.isSafeInteger(n) ? n : null;
}

// X-Rate-Limit-Remaining: credits left. Sent on successful responses, absent
// on a 429 (observed 2026-09-23), so null on a 429 is normal.
export function parseCreditsRemaining(value: string | null): number | null {
	return parseWholeNumberHeader(value);
}

// X-Rate-Limit-Retry-After-Seconds: seconds until the budget refills, sent
// with a 429. Null means the fallback backoff applies.
export function parseRetryAfterSeconds(value: string | null): number | null {
	return parseWholeNumberHeader(value);
}

// Delay for the nth consecutive 429 without a usable retry header (n >= 1):
// a random value between the base and min(max, base * 2^(n-1)).
// With the defaults: n=1 60 s, n=2 60-120 s, n=3 60-240 s, n=4 60-480 s,
// n>=5 60-900 s. The base is a floor, not full jitter from zero, because each
// retry spends a request against a budget that is probably still empty.
export function fallbackBackoffMs(
	consecutiveFallbacks: number,
	baseMs: number,
	maxMs: number,
	random: () => number = Math.random,
): number {
	const n = Math.max(1, consecutiveFallbacks);
	const ceiling = Math.max(baseMs, Math.min(maxMs, baseMs * 2 ** (n - 1)));
	return baseMs + Math.floor(random() * (ceiling - baseMs));
}

// setTimeout fires almost immediately for delays above 2^31 - 1 ms (about 24.8
// days) instead of waiting. OpenSky's retry times are under a day, so this only
// guards against a nonsensical header turning a pause into a request flood.
const MAX_TIMER_DELAY_MS = 2_147_483_647;

export type CycleOutcome =
	| { kind: 'ok' }
	| { kind: 'rate_limited'; retryAfterSeconds: number | null; retryAfterHeader: string | null }
	| { kind: 'failed' };

export interface RateLimitState {
	// When the current pause began (processing time); null when not paused.
	pausedSinceMs: number | null;
	// 429s received since the pause began.
	rateLimitedResponses: number;
	// 429s without a usable retry header since the last success.
	consecutiveFallbacks: number;
}

export const NOT_PAUSED: RateLimitState = {
	pausedSinceMs: null,
	rateLimitedResponses: 0,
	consecutiveFallbacks: 0,
};

export interface PollPlanOptions {
	intervalMs: number;
	backoffBaseMs: number;
	backoffMaxMs: number;
}

export interface PollPlan {
	state: RateLimitState;
	delayMs: number;
	event: 'paused' | 'pause_extended' | 'resumed' | null;
	// Where delayMs came from when rate limited.
	delaySource: 'retry_header' | 'fallback_backoff' | null;
	// Set on 'resumed': how long the pause lasted.
	pausedForMs: number | null;
}

// Decides the delay before the next request from the cycle that just ended.
// Only a successful response ends a pause and resets the fallback count: a
// network error or 5xx while paused says nothing about the budget.
export function planNextPoll(
	state: RateLimitState,
	outcome: CycleOutcome,
	nowMs: number,
	opts: PollPlanOptions,
	random: () => number = Math.random,
): PollPlan {
	if (outcome.kind === 'ok') {
		const wasPaused = state.pausedSinceMs !== null;
		return {
			state: NOT_PAUSED,
			delayMs: opts.intervalMs,
			event: wasPaused ? 'resumed' : null,
			delaySource: null,
			pausedForMs: wasPaused ? nowMs - (state.pausedSinceMs as number) : null,
		};
	}

	if (outcome.kind === 'failed') {
		return {
			state,
			delayMs: opts.intervalMs,
			event: null,
			delaySource: null,
			pausedForMs: null,
		};
	}

	const event = state.pausedSinceMs === null ? 'paused' : 'pause_extended';
	const pausedSinceMs = state.pausedSinceMs ?? nowMs;
	const rateLimitedResponses = state.rateLimitedResponses + 1;

	if (outcome.retryAfterSeconds !== null) {
		// The provider's retry time replaces the fallback backoff, but the wait
		// is the longer of the retry time and the normal interval, so a retry
		// of 0 still waits one interval. Capped only by the timer limit above.
		const delayMs = Math.min(
			MAX_TIMER_DELAY_MS,
			Math.max(opts.intervalMs, outcome.retryAfterSeconds * 1000),
		);
		return {
			state: { ...state, pausedSinceMs, rateLimitedResponses },
			delayMs,
			event,
			delaySource: 'retry_header',
			pausedForMs: null,
		};
	}

	const consecutiveFallbacks = state.consecutiveFallbacks + 1;
	return {
		state: { pausedSinceMs, rateLimitedResponses, consecutiveFallbacks },
		delayMs: fallbackBackoffMs(consecutiveFallbacks, opts.backoffBaseMs, opts.backoffMaxMs, random),
		event,
		delaySource: 'fallback_backoff',
		pausedForMs: null,
	};
}

export interface RateLimitLogLine {
	level: 'info' | 'warn';
	message: string;
	fields: Record<string, unknown>;
}

// The log line for a pause, an extended pause or a resume; null otherwise.
// resume_at is when the next request is due, so an operator reading an
// 8-hour pause knows it is intentional and when it ends.
export function rateLimitLogLine(
	plan: PollPlan,
	outcome: CycleOutcome,
	nowMs: number,
): RateLimitLogLine | null {
	if (plan.event === 'resumed') {
		return {
			level: 'info',
			message: 'opensky resumed after rate limit',
			fields: { paused_for_ms: plan.pausedForMs },
		};
	}
	if (plan.event === null || outcome.kind !== 'rate_limited') return null;

	return {
		level: 'warn',
		message:
			plan.event === 'paused'
				? 'opensky rate limited, pausing requests'
				: 'opensky still rate limited, pause extended',
		fields: {
			http_status: 429,
			delay_source: plan.delaySource,
			retry_after_header: outcome.retryAfterHeader,
			delay_ms: plan.delayMs,
			resume_at: new Date(nowMs + plan.delayMs).toISOString(),
			rate_limited_responses: plan.state.rateLimitedResponses,
			...(plan.delaySource === 'fallback_backoff'
				? { consecutive_fallbacks: plan.state.consecutiveFallbacks }
				: {}),
		},
	};
}

// ---- Kafka setup -----------------------------------------------------------

const kafka = new Kafka({
	clientId: 'ingestion-poller',
	brokers: config.KAFKA_BROKERS,
	logLevel: 0,
});

const producer = kafka.producer({
	// LegacyPartitioner preserves kafkajs v1 hash behaviour. Required in v2 to
	// suppress the deprecation warning. When adsb.raw is scaled to multiple
	// partitions, this partitioner hashes the icao24 key with murmur2 to route
	// all events for the same aircraft to the same partition.
	createPartitioner: Partitioners.LegacyPartitioner,
});

// ---- Logging ---------------------------------------------------------------

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
			message,
			...extra,
		}) + '\n',
	);
}

// ---- OpenSky auth ------------------------------------------------------------

// OpenSky's OAuth2 token endpoint (Keycloak, client-credentials grant).
const OPENSKY_TOKEN_URL =
	'https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token';

interface CachedToken {
	accessToken: string;
	expiresAtMs: number;
}

let cachedToken: CachedToken | null = null;

// Returns a bearer token when OPENSKY_CLIENT_ID/SECRET are configured, null
// otherwise (falls back to unauthenticated requests). Caches the token until
// shortly before its own expiry so most poll cycles reuse it instead of
// re-authenticating on every request.
async function getAccessToken(): Promise<string | null> {
	if (!config.OPENSKY_CLIENT_ID || !config.OPENSKY_CLIENT_SECRET) return null;

	const now = Date.now();
	if (cachedToken && cachedToken.expiresAtMs > now) return cachedToken.accessToken;

	const body = new URLSearchParams({
		grant_type: 'client_credentials',
		client_id: config.OPENSKY_CLIENT_ID,
		client_secret: config.OPENSKY_CLIENT_SECRET,
	});

	const response = await fetch(OPENSKY_TOKEN_URL, {
		method: 'POST',
		headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
		body,
		signal: AbortSignal.timeout(config.FETCH_TIMEOUT_MS),
	});

	if (!response.ok) {
		throw new Error(`OpenSky token request failed: HTTP ${response.status}`);
	}

	const data = (await response.json()) as { access_token: string; expires_in: number };

	// Refresh 60s before actual expiry so a cached token is never used right up
	// against the moment it stops being valid mid-request.
	cachedToken = {
		accessToken: data.access_token,
		expiresAtMs: now + (data.expires_in - 60) * 1000,
	};
	return cachedToken.accessToken;
}

// ---- OpenSky fetch ---------------------------------------------------------

// OpenSky returns state vectors as positional arrays. Index positions are fixed
// by the API contract and documented here to make the mapping auditable.
// Index 12 is the sensors array (receiver IDs) — not needed downstream, skipped.
export function mapStateVector(state: unknown[], fetchedAtMs: number): AdsbRawEvent {
	return {
		icao24: state[0] as string,
		// callsign preserved verbatim — no trimming or mutation here.
		// Position Consumer is responsible for normalisation.
		callsign: typeof state[1] === 'string' ? state[1] : null,
		origin_country: state[2] as string,
		time_position: state[3] as number | null,
		last_contact: state[4] as number,
		lon: state[5] as number | null,
		lat: state[6] as number | null,
		baro_altitude: state[7] as number | null,
		on_ground: state[8] as boolean,
		velocity: state[9] as number | null,
		true_track: state[10] as number | null,
		vertical_rate: state[11] as number | null,
		// [12] sensors: receiver IDs; preserved for provider fidelity
		sensors: Array.isArray(state[12]) ? (state[12] as number[]) : null,
		geo_altitude: state[13] as number | null,
		squawk: state[14] as string | null,
		spi: state[15] as boolean,
		position_source: state[16] as number,
		// [17] category — ADS-B emitter category; present only when extended=1 was in URL
		category: typeof state[17] === 'number' ? state[17] : null,
		fetched_at_ms: fetchedAtMs,
	};
}

type FetchResult =
	| { kind: 'ok'; events: AdsbRawEvent[]; creditsRemaining: number | null }
	| { kind: 'rate_limited'; retryAfterSeconds: number | null; retryAfterHeader: string | null };

async function fetchStateVectors(): Promise<FetchResult> {
	const fetchedAtMs = Date.now();

	const token = await getAccessToken();
	const headers: Record<string, string> = { Accept: 'application/json' };
	if (token) headers['Authorization'] = `Bearer ${token}`;

	const response = await fetch(OPENSKY_URL, {
		headers,
		signal: AbortSignal.timeout(config.FETCH_TIMEOUT_MS),
	});

	// A 429 is not an ordinary failure: the budget is spent and retrying
	// before the refill cannot succeed. It carries no remaining balance.
	if (response.status === 429) {
		const retryAfterHeader = response.headers.get('x-rate-limit-retry-after-seconds');
		return {
			kind: 'rate_limited',
			retryAfterSeconds: parseRetryAfterSeconds(retryAfterHeader),
			retryAfterHeader,
		};
	}

	if (!response.ok) {
		throw new Error(`OpenSky returned HTTP ${response.status}`);
	}

	const creditsRemaining = parseCreditsRemaining(response.headers.get('x-rate-limit-remaining'));

	const body = (await response.json()) as {
		time: number;
		states: unknown[] | null;
	};

	if (!Array.isArray(body.states) || body.states.length === 0) {
		return { kind: 'ok', events: [], creditsRemaining };
	}

	return {
		kind: 'ok',
		events: (body.states as unknown[][]).map((s) => mapStateVector(s, fetchedAtMs)),
		creditsRemaining,
	};
}

// ---- OpenSky health check (ADR-022, CP3d) ------------------------------------

// A standby check for the ingestion coordinator: fetch and validate only,
// never publish. Its outcome is provider health evidence and nothing else.

export type OpenskyCheckResult =
	| { kind: 'ok'; creditsRemaining: number | null }
	| { kind: 'failed'; error: string }
	| { kind: 'rate_limited'; retryAfterSeconds: number | null };

// ADR-022 section 3: `time` is a finite number (epoch seconds, not ms: the
// adsb.fi millisecond check does not apply), and `states` is an array or null.
// Returns the problem, or null when the body is valid.
export function validateOpenskyBody(body: unknown): string | null {
	if (typeof body !== 'object' || body === null || Array.isArray(body)) {
		return 'response is not a JSON object';
	}
	const { time, states } = body as { time?: unknown; states?: unknown };
	if (typeof time !== 'number' || !Number.isFinite(time)) return 'time is not a finite number';
	if (states !== null && !Array.isArray(states)) return 'states is neither an array nor null';
	return null;
}

// A failure to get or refresh the access token makes OpenSky unusable from
// here, so it is a health failure too, classed `auth: ...`. Error messages
// never contain the client secret.
export async function checkOpenskyHealth(
	getToken: () => Promise<string | null> = getAccessToken,
	fetchFn: typeof fetch = fetch,
): Promise<OpenskyCheckResult> {
	let token: string | null;
	try {
		token = await getToken();
	} catch (err) {
		const cls = classifyRequestError(err);
		return { kind: 'failed', error: `auth: ${cls.startsWith('error: ') ? cls.slice(7) : cls}` };
	}

	const headers: Record<string, string> = { Accept: 'application/json' };
	if (token) headers['Authorization'] = `Bearer ${token}`;
	let response: Response;
	try {
		response = await fetchFn(OPENSKY_URL, {
			headers,
			signal: AbortSignal.timeout(config.FETCH_TIMEOUT_MS),
		});
	} catch (err) {
		return { kind: 'failed', error: classifyRequestError(err) };
	}

	if (response.status === 429) {
		return {
			kind: 'rate_limited',
			retryAfterSeconds: parseRetryAfterSeconds(
				response.headers.get('x-rate-limit-retry-after-seconds'),
			),
		};
	}
	if (!response.ok) return { kind: 'failed', error: `http_${response.status}` };

	let body: unknown;
	try {
		body = await response.json();
	} catch {
		return { kind: 'failed', error: 'validation: response is not JSON' };
	}
	const problem = validateOpenskyBody(body);
	if (problem !== null) return { kind: 'failed', error: `validation: ${problem}` };
	return {
		kind: 'ok',
		creditsRemaining: parseCreditsRemaining(response.headers.get('x-rate-limit-remaining')),
	};
}

// ---- Poll cycle ------------------------------------------------------------

async function pollOnce(): Promise<CycleOutcome> {
	let result: FetchResult;

	try {
		result = await fetchStateVectors();
	} catch (err) {
		// Log and skip this cycle. A transient OpenSky outage should not crash
		// the poller; retry after the normal interval.
		log('warn', 'opensky fetch failed, skipping cycle', {
			error: err instanceof Error ? err.message : String(err),
		});
		return { kind: 'failed' };
	}

	if (result.kind === 'rate_limited') return result;

	const events = result.events;

	if (events.length === 0) {
		log('info', 'opensky returned no state vectors', {
			credits_remaining: result.creditsRemaining,
		});
		return { kind: 'ok' };
	}

	const messages = events.map((event) => ({
		key: event.icao24,
		value: adsbRawEnvelope('opensky', event),
	}));

	// When POLLER_BATCH_MAX_MESSAGES is 0 (no cap), batchSize covers the full array
	// and exactly one producer.send() is issued — identical to previous behavior.
	// A positive value chunks the array, useful for larger polling regions.
	const batchSize =
		config.POLLER_BATCH_MAX_MESSAGES === 0 ? messages.length : config.POLLER_BATCH_MAX_MESSAGES;

	let firstOffset = 'unknown';
	for (let i = 0; i < messages.length; i += batchSize) {
		const chunk = messages.slice(i, i + batchSize);
		const results = await producer.send({ topic: config.TOPIC, messages: chunk });
		if (i === 0) firstOffset = results[0]?.baseOffset ?? 'unknown';
	}

	// Log the base offset of the first batch so you can verify with:
	//   docker exec sentinel-redpanda rpk topic consume adsb.raw --offset <N> --num 1
	log('info', 'poll cycle complete', {
		state_vectors: events.length,
		// null when OpenSky sent no X-Rate-Limit-Remaining header.
		credits_remaining: result.creditsRemaining,
		topic: config.TOPIC,
		first_offset: firstOffset,
	});
	return { kind: 'ok' };
}

// ---- Poll loop -------------------------------------------------------------

let pollTimeout: ReturnType<typeof setTimeout> | null = null;
let stopping = false;
let rateLimitState: RateLimitState = NOT_PAUSED;

function scheduleNextPoll(delayMs: number): void {
	if (stopping) return;
	pollTimeout = setTimeout(() => {
		void pollOnce()
			.catch((err: unknown): CycleOutcome => {
				// Kafka publish errors land here: a failed cycle, retried at the
				// normal interval.
				log('error', 'poll cycle error', {
					error: err instanceof Error ? err.message : String(err),
				});
				return { kind: 'failed' };
			})
			.then((outcome) => {
				const nowMs = Date.now();
				const plan = planNextPoll(rateLimitState, outcome, nowMs, {
					intervalMs: config.POLL_INTERVAL_MS,
					backoffBaseMs: config.OPENSKY_BACKOFF_BASE_MS,
					backoffMaxMs: config.OPENSKY_BACKOFF_MAX_MS,
				});
				const line = rateLimitLogLine(plan, outcome, nowMs);
				if (line) log(line.level, line.message, line.fields);
				rateLimitState = plan.state;
				scheduleNextPoll(plan.delayMs);
			});
	}, delayMs);
}

// ---- Entry point -----------------------------------------------------------

async function run(): Promise<void> {
	log('info', 'producer connecting', { brokers: config.KAFKA_BROKERS });
	await producer.connect();
	log('info', 'producer connected');

	const authenticated = Boolean(config.OPENSKY_CLIENT_ID && config.OPENSKY_CLIENT_SECRET);
	const budget = projectDailyBudget(
		{
			lamin: config.OPENSKY_LAMIN,
			lomin: config.OPENSKY_LOMIN,
			lamax: config.OPENSKY_LAMAX,
			lomax: config.OPENSKY_LOMAX,
		},
		config.POLL_INTERVAL_MS,
		authenticated,
	);

	log('info', 'poller starting', {
		url: OPENSKY_URL,
		poll_interval_ms: config.POLL_INTERVAL_MS,
		fetch_timeout_ms: config.FETCH_TIMEOUT_MS,
		batch_max_messages:
			config.POLLER_BATCH_MAX_MESSAGES === 0 ? 'unlimited' : config.POLLER_BATCH_MAX_MESSAGES,
		authenticated,
		backoff_base_ms: config.OPENSKY_BACKOFF_BASE_MS,
		backoff_max_ms: config.OPENSKY_BACKOFF_MAX_MS,
		...budget,
	});

	// Warn, not refuse: a short over-budget run (a test, a demo) is legitimate,
	// and the 429 pause keeps even a long one from hammering OpenSky.
	if (!budget.within_budget) {
		log('warn', 'projected daily credits exceed the budget, expect a 429 pause', {
			projected_daily_credits: budget.projected_daily_credits,
			daily_budget: budget.daily_budget,
			min_sustainable_interval_ms: budget.min_sustainable_interval_ms,
		});
	}

	// Run the first poll immediately so you see output without waiting a full
	// interval. Later delays come from planNextPoll.
	scheduleNextPoll(0);
}

async function shutdown(signal: string): Promise<void> {
	stopping = true;
	if (pollTimeout !== null) clearTimeout(pollTimeout);
	log('info', 'shutdown initiated', { signal });
	await producer.disconnect();
	log('info', 'producer disconnected');
	process.exit(0);
}

// Only run the service when this file is executed directly (`npm run poll`),
// not when imported — e.g. by a unit test importing mapStateVector below.
if (process.argv[1] === fileURLToPath(import.meta.url)) {
	process.on('SIGINT', () => {
		shutdown('SIGINT').catch(() => process.exit(1));
	});
	process.on('SIGTERM', () => {
		shutdown('SIGTERM').catch(() => process.exit(1));
	});

	run().catch((err: unknown) => {
		log('error', 'poller failed', {
			error: {
				name: err instanceof Error ? err.name : 'UnknownError',
				message: err instanceof Error ? err.message : String(err),
			},
		});
		process.exit(1);
	});
}
