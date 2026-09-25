// OpenSky adapter for the ingestion coordinator.
//
// One request to the OpenSky Network REST API: authenticate if configured,
// fetch, validate and map each state vector into an adsb.raw message keyed by
// icao24 and wrapped in the { provider: 'opensky', payload } envelope
// (ADR-021). It never publishes and never schedules: the coordinator decides
// what each request is for and owns cadence, backoff and publishing.
//
// Field names follow OpenSky's own naming so the raw Kafka log faithfully
// represents the source. The Position Consumer maps them to the canonical
// schema. Keying by icao24 keeps one aircraft's records on one partition.
//
// OpenSky limits access by a daily credit budget, not a request rate: 400
// credits a day anonymous, 4,000 with OAuth2 client credentials
// (OPENSKY_CLIENT_ID/SECRET). The SF Bay box costs 1 credit per request.
// Successful responses carry X-Rate-Limit-Remaining. A spent budget answers
// 429 with X-Rate-Limit-Retry-After-Seconds and no remaining balance
// (observed 2026-09-23: a retry of 31,952 s).

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

// ---- Rate-limit headers ------------------------------------------------------

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

// ---- OpenSky request for the ingestion coordinator (ADR-022) ----------------

// One OpenSky request: fetch, validate and map, never publish. The
// coordinator decides what the request is for: a standby check (health
// only), a candidate delivery while authority is none, or an active cycle
// while OpenSky is authoritative. Every one is health evidence, so there is
// never a second request for the same purpose.

export type OpenskyFetchResult =
	| {
			kind: 'ok';
			// Enveloped `adsb.raw` messages, keyed by ICAO24; empty for states: null.
			messages: { key: string; value: string }[];
			// The response `time`, epoch seconds.
			responseTime: number;
			creditsRemaining: number | null;
	  }
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
export async function fetchOpenskyCycle(
	getToken: () => Promise<string | null> = getAccessToken,
	fetchFn: typeof fetch = fetch,
): Promise<OpenskyFetchResult> {
	const fetchedAtMs = Date.now();
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
	const { time, states } = body as { time: number; states: unknown[] | null };
	const messages = (states ?? []).map((state) => {
		const event = mapStateVector(state as unknown[], fetchedAtMs);
		return { key: event.icao24, value: adsbRawEnvelope('opensky', event) };
	});
	return {
		kind: 'ok',
		messages,
		responseTime: time,
		creditsRemaining: parseCreditsRemaining(response.headers.get('x-rate-limit-remaining')),
	};
}

// Whether OpenSky requests are authenticated. Anonymous access has a 400
// credit daily budget, which a 25 s active cadence spends in about 2.8 h.
export const openskyAuthenticated = (): boolean =>
	Boolean(config.OPENSKY_CLIENT_ID && config.OPENSKY_CLIENT_SECRET);
