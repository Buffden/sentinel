// Position Consumer — normalization and validation.
//
// Converts a raw adsb.raw payload string into a canonical NormalizedPosition,
// or returns a typed rejection that tells the caller what to do next:
//
//   parse_error / missing_entity_id  → DLQ (added in CP4)
//   no_position                      → skip with warn (valid source record,
//                                      no current GPS fix — not malformed)
//   ok                               → persist, update live state, publish
//
// H3 geo-cell computation is intentionally absent here.
// history_geo_cell and live_geo_cell are added to the position in CP7 when
// the Redis geo-cell sorted-set write is implemented. Keeping them out of CP3
// keeps this module free of spatial-index concerns.
//
// This module owns no I/O. It is a pure function on a string so it can be
// exercised directly in a Node.js REPL or unit test without any infrastructure.

// ---- navigation_status enum ------------------------------------------------
//
// Normalized string enum. The raw AIS NAVSTAT integer is NOT stored here —
// it belongs in raw_events.payload only.
// null means "not applicable" (used for all ADS-B records).
export type NavigationStatus =
	| 'under_way_engine'
	| 'anchored'
	| 'not_under_command'
	| 'restricted'
	| 'constrained_by_draught'
	| 'moored'
	| 'aground'
	| 'fishing'
	| 'under_way_sailing'
	| 'sart_active'
	| 'unknown';

// ---- NormalizedPosition ----------------------------------------------------
//
// The canonical form that flows into position_history, Redis, and
// position.normalized. No raw provider payload embedded — that is written to
// raw_events independently from the original Kafka message string.
//
// H3 geo-cells (history_geo_cell, live_geo_cell) are added in CP7.
//
// Universal movement core: fields present for every source type.
// Decision-support fields: nullable; null meaning depends on entity_type.
//   e.g. altitude_m is always null for vessels; navigation_status always null
//   for aircraft.
export interface NormalizedPosition {
	// --- Universal movement core ---
	entity_id: string; // icao24 for ADS-B; MMSI for AIS
	entity_type: 'aircraft' | 'vessel' | 'satellite' | 'ground_vehicle' | 'unknown';
	timestamp_ms: number; // source event time; NEVER processing time
	lat: number;
	lon: number;
	speed_mps: number | null; // velocity (ADS-B) or SOG * 0.514444 (AIS knots→m/s)
	course_deg: number | null; // true_track (ADS-B) or COG (AIS)
	heading_deg: number | null; // HEADING (AIS only; ADS-B does not separate from course)
	source: 'adsb' | 'ais' | 'satellite' | 'synthetic';
	provider: string | null; // 'opensky' | 'aishub' | etc.

	// --- Altitude (all null for vessels) ---
	// altitude_m: preferred composite (geo_altitude ?? baro_altitude)
	altitude_m: number | null;
	baro_altitude_m: number | null; // barometric; raw from provider
	geo_altitude_m: number | null; // GNSS; raw from provider

	// --- Movement quality ---
	vertical_rate_mps: number | null; // m/s; positive = climbing; null for vessels
	on_ground: boolean | null; // surface indicator; null for vessels

	// --- ADS-B specific (null for AIS/other) ---
	last_contact_ms: number | null; // last_contact * 1000; critical for signal-loss
	squawk: string | null; // 4-digit transponder code (7500/7600/7700)
	spi: boolean | null; // special position identification
	position_source: number | null; // 0=ADS-B 1=ASTERIX 2=MLAT 3=FLARM

	// --- AIS specific (null for ADS-B) ---
	navigation_status: NavigationStatus | null; // normalized from NAVSTAT integer
	rate_of_turn: number | null; // ROT; unusual manoeuvre detection
	position_accuracy: boolean | null; // PAC; high/low accuracy flag
	destination: string | null; // DEST; route/deviation rules
	eta: string | null; // ETA; route/deviation rules
	draught_m: number | null; // vessel draught in metres

	// --- Entity classification ---
	callsign: string | null;
	// entity_subtype: broad normalized class, source-neutral.
	//   aircraft: 'fixed_wing' | 'rotorcraft' | 'uav' | 'lighter_than_air' | 'unknown'
	//   vessel:   'cargo' | 'tanker' | 'passenger' | 'tug' | 'sailing' | 'fishing' | 'unknown'
	entity_subtype: string | null;
	// provider_category: original provider classification, verbatim as string.
	//   ADS-B: OpenSky category integer as string (requires extended=1 URL param).
	//   AIS:   ship type code as string.
	provider_category: string | null;
}

// Typed rejection variants so the consumer can branch without string-matching.
export type NormalizeResult =
	// Valid position record, ready for persistence and publication.
	| { ok: true; position: NormalizedPosition }
	// Valid source record but entity has no current GPS fix (on the ground,
	// or transponder reporting without position data). Not a DLQ candidate.
	| { ok: false; kind: 'no_position'; entity_id: string }
	// Unprocessable: JSON parse failure or missing entity identity.
	// Route to adsb.dlq (implemented in CP4).
	| { ok: false; kind: 'parse_error' | 'missing_entity_id'; detail: string };

// ---- ADS-B entity_subtype mapping ------------------------------------------
//
// OpenSky category codes (only present when poller URL includes extended=1).
// Source: https://opensky-network.org/apidoc/rest.html
//
//  0: No information
//  1: No ADS-B emitter category information
//  2: Light (< 15500 lbs)
//  3: Small (15500–75000 lbs)
//  4: Large (75000–300000 lbs)
//  5: High Vortex Large (e.g. B-757)
//  6: Heavy (> 300000 lbs)
//  7: High Performance (> 5g acceleration and 400 kts)
//  8: Rotorcraft
//  9: Glider / sailplane
// 10: Lighter-than-air
// 11: Parachutist / skydiver
// 12: Ultralight / hang-glider / paraglider
// 13: Reserved
// 14: Unmanned Aerial Vehicle
// 15: Space / trans-atmospheric vehicle
// 16: Surface vehicle — emergency
// 17: Surface vehicle — service
// 18: Point obstacle (includes tethered balloons)
// 19: Cluster obstacle
// 20: Line obstacle
function adsbCategoryToSubtype(category: number | null): string | null {
	if (category == null) return null;
	if (category === 8) return 'rotorcraft';
	if (category === 10) return 'lighter_than_air';
	if (category === 14) return 'uav';
	// 2–7: all are fixed-wing aircraft of varying size/performance
	if (category >= 2 && category <= 7) return 'fixed_wing';
	// 0, 1, 9, 11–13, 15–20: insufficient information to classify
	return 'unknown';
}

// ---- Normalization ---------------------------------------------------------

export function normalizeAdsbRaw(rawValue: string): NormalizeResult {
	// Step 1: JSON parse.
	// A non-parseable value is definitively malformed — DLQ in CP4.
	let parsed: unknown;
	try {
		parsed = JSON.parse(rawValue);
	} catch (err) {
		return {
			ok: false,
			kind: 'parse_error',
			detail: err instanceof Error ? err.message : String(err),
		};
	}

	if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) {
		return { ok: false, kind: 'parse_error', detail: 'payload is not a JSON object' };
	}

	return normalizeOpenSkyRecord(parsed as Record<string, unknown>);
}

// OpenSky mapping on an already-parsed record. Reached either from a legacy
// bare record or from an envelope's payload (ADR-021); the rules are the same
// in both cases, which is what keeps legacy replay behaviour unchanged.
export function normalizeOpenSkyRecord(r: Record<string, unknown>): NormalizeResult {
	// Step 2: Entity identity.
	// icao24 is the stable entity identifier. Without it, persistence and the
	// Redis live key cannot be written. DLQ in CP4.
	if (typeof r['icao24'] !== 'string' || r['icao24'] === '') {
		return {
			ok: false,
			kind: 'missing_entity_id',
			detail: 'icao24 field missing or empty',
		};
	}

	const entity_id = r['icao24'] as string;

	// Step 3: Position fields.
	// null lat, lon, or time_position is valid in OpenSky — the aircraft is
	// reporting without a position lock. Skip with warn; do NOT DLQ.
	// This distinction is deliberate: the record is not broken, just not
	// actionable for position-history or Redis state.
	if (r['lat'] == null || r['lon'] == null || r['time_position'] == null) {
		return { ok: false, kind: 'no_position', entity_id };
	}

	// If the fields are present but the wrong type, the source is corrupted.
	if (
		typeof r['lat'] !== 'number' ||
		typeof r['lon'] !== 'number' ||
		typeof r['time_position'] !== 'number'
	) {
		return {
			ok: false,
			kind: 'parse_error',
			detail: 'lat, lon, or time_position present but wrong type',
		};
	}

	const lat = r['lat'] as number;
	const lon = r['lon'] as number;
	const time_position = r['time_position'] as number;

	// Step 4: Altitude.
	// Prefer GNSS geo_altitude (directly measured); fall back to baro_altitude.
	// Both preserved so downstream can reason about which measurement was used.
	const geo_altitude_m = typeof r['geo_altitude'] === 'number' ? r['geo_altitude'] : null;
	const baro_altitude_m = typeof r['baro_altitude'] === 'number' ? r['baro_altitude'] : null;
	const altitude_m = geo_altitude_m ?? baro_altitude_m;

	// Step 5: Optional movement fields.
	const speed_mps = typeof r['velocity'] === 'number' ? r['velocity'] : null;
	const course_deg = typeof r['true_track'] === 'number' ? r['true_track'] : null;
	const vertical_rate_mps = typeof r['vertical_rate'] === 'number' ? r['vertical_rate'] : null;
	const on_ground = typeof r['on_ground'] === 'boolean' ? r['on_ground'] : null;
	// last_contact is Unix seconds in OpenSky; canonical uses milliseconds throughout.
	const last_contact_ms = typeof r['last_contact'] === 'number' ? r['last_contact'] * 1000 : null;

	// Step 6: ADS-B specific fields.
	const squawk = typeof r['squawk'] === 'string' && r['squawk'] !== '' ? r['squawk'] : null;
	const spi = typeof r['spi'] === 'boolean' ? r['spi'] : null;
	const position_source = typeof r['position_source'] === 'number' ? r['position_source'] : null;
	// callsign: trim provider whitespace padding before storing in the canonical
	// record. OpenSky pads callsigns to a fixed width with trailing spaces.
	// The raw value is preserved verbatim in raw_events.payload.
	const callsign =
		typeof r['callsign'] === 'string' && r['callsign'].trim() !== '' ? r['callsign'].trim() : null;

	// Step 7: Entity classification.
	// category is only present when the poller adds extended=1 to the OpenSky URL.
	// Without it, entity_subtype and provider_category are null — acceptable
	// until the poller fix is confirmed to reach real records.
	const category = typeof r['category'] === 'number' ? r['category'] : null;
	const entity_subtype = adsbCategoryToSubtype(category);
	const provider_category = category !== null ? String(category) : null;

	return {
		ok: true,
		position: {
			entity_id,
			entity_type: 'aircraft',
			// Source event time in milliseconds.
			// NEVER use fetched_at_ms here — that is processing time and would
			// break replay safety and deterministic idempotency identities.
			timestamp_ms: time_position * 1000,
			lat,
			lon,
			speed_mps,
			course_deg,
			heading_deg: null, // ADS-B does not report heading separately from course
			source: 'adsb',
			provider: 'opensky',

			altitude_m,
			baro_altitude_m,
			geo_altitude_m,

			vertical_rate_mps,
			on_ground,

			last_contact_ms,
			squawk,
			spi,
			position_source,

			// AIS-specific fields — all null for ADS-B
			navigation_status: null,
			rate_of_turn: null,
			position_accuracy: null,
			destination: null,
			eta: null,
			draught_m: null,

			callsign,
			entity_subtype,
			provider_category,
		},
	};
}

// ---- adsb.fi (ADR-020 regional primary, ADR-021 envelope) ------------------
//
// adsb.fi serves the ADS-B Exchange v2 format. A payload is one aircraft object
// split out of a response, plus the response-level context the poller keeps:
//   response_now_ms  the response's top-level `now`, epoch milliseconds
//   fetched_at_ms    processing time of the poll (never used for event time)
// seen_pos and seen are seconds BEFORE `now`, so event times can only be
// derived deterministically from response_now_ms, never from Date.now().

const KNOTS_TO_MPS = 0.514444;
const FEET_PER_MINUTE_TO_MPS = 0.00508;
const FEET_TO_METRES = 0.3048;

// The single place that turns adsb.fi's relative "seconds before now" into an
// absolute source event time. Rounded to whole milliseconds so the result is
// stable across replays and safe as part of (entity_id, observed_at).
export function adsbfiEventTimeMs(responseNowMs: number, secondsBeforeNow: number): number {
	return Math.round(responseNowMs - secondsBeforeNow * 1000);
}

// ADS-B emitter category as adsb.fi reports it ("A1".."C7"). Mirrors the
// OpenSky integer mapping above: same classes, different encoding.
function adsbfiCategoryToSubtype(category: string | null): string | null {
	if (category == null) return null;
	if (category === 'A7') return 'rotorcraft';
	if (category === 'B2') return 'lighter_than_air';
	if (category === 'B6') return 'uav';
	if (/^A[1-6]$/.test(category)) return 'fixed_wing';
	return 'unknown';
}

// adsb.fi `type` names where the position came from; the canonical field
// uses OpenSky's codes (0=ADS-B 1=ASTERIX 2=MLAT 3=FLARM). Sources with no
// equivalent code (TIS-B, mode_s, other) stay null rather than guessed.
function adsbfiTypeToPositionSource(type: unknown): number | null {
	if (typeof type !== 'string') return null;
	if (type.startsWith('adsb_') || type.startsWith('adsr_')) return 0;
	if (type === 'mlat') return 2;
	return null;
}

// adsb.fi reports alt_baro and alt_geom in FEET, and alt_baro is the string
// "ground" (not a number) when the aircraft is on the ground. Canonical fields
// are metres, with altitude_m preferring geo over baro like the OpenSky mapping.
// A "ground" report still keeps a usable alt_geom as altitude_m.
export function mapAdsbfiAltitudeAndGround(r: Record<string, unknown>): {
	baro_altitude_m: number | null;
	geo_altitude_m: number | null;
	altitude_m: number | null;
	on_ground: boolean | null;
} {
	const altBaro = r['alt_baro'];
	const altGeom = r['alt_geom'];

	const baro_altitude_m = typeof altBaro === 'number' ? altBaro * FEET_TO_METRES : null;
	const geo_altitude_m = typeof altGeom === 'number' ? altGeom * FEET_TO_METRES : null;
	const altitude_m = geo_altitude_m ?? baro_altitude_m;

	// A numeric alt_baro means readsb did not flag the aircraft as grounded, so
	// it maps to false. Anything else (absent, unexpected) stays unknown.
	const on_ground = altBaro === 'ground' ? true : typeof altBaro === 'number' ? false : null;

	return { baro_altitude_m, geo_altitude_m, altitude_m, on_ground };
}

export function normalizeAdsbfiRecord(r: Record<string, unknown>): NormalizeResult {
	const hex = r['hex'];
	if (typeof hex !== 'string' || hex === '') {
		return { ok: false, kind: 'missing_entity_id', detail: 'hex field missing or empty' };
	}
	// The poller skips non-ICAO tracks (ADR-021). Rejecting here as well keeps
	// a hand-published or future-producer record from entering the ICAO-based
	// canonical identity model.
	if (hex.startsWith('~')) {
		return {
			ok: false,
			kind: 'missing_entity_id',
			detail: `non-ICAO address "${hex}" is not supported`,
		};
	}
	const entity_id = hex.toLowerCase();

	if (r['lat'] == null || r['lon'] == null) {
		return { ok: false, kind: 'no_position', entity_id };
	}
	if (typeof r['lat'] !== 'number' || typeof r['lon'] !== 'number') {
		return { ok: false, kind: 'parse_error', detail: 'lat or lon present but wrong type' };
	}
	const responseNowMs = r['response_now_ms'];
	const seenPos = r['seen_pos'];
	if (typeof responseNowMs !== 'number' || typeof seenPos !== 'number') {
		return {
			ok: false,
			kind: 'parse_error',
			detail: 'response_now_ms or seen_pos missing; position time cannot be derived',
		};
	}

	const seen = r['seen'];
	const category = typeof r['category'] === 'string' ? r['category'] : null;
	const flight = r['flight'];
	const verticalRateFpm =
		typeof r['baro_rate'] === 'number'
			? r['baro_rate']
			: typeof r['geom_rate'] === 'number'
				? r['geom_rate']
				: null;

	return {
		ok: true,
		position: {
			entity_id,
			entity_type: 'aircraft',
			timestamp_ms: adsbfiEventTimeMs(responseNowMs, seenPos),
			lat: r['lat'],
			lon: r['lon'],
			speed_mps: typeof r['gs'] === 'number' ? r['gs'] * KNOTS_TO_MPS : null,
			course_deg: typeof r['track'] === 'number' ? r['track'] : null,
			heading_deg: null, // ADS-B course, as in the OpenSky mapping
			source: 'adsb',
			provider: 'adsbfi',

			...mapAdsbfiAltitudeAndGround(r),

			vertical_rate_mps: verticalRateFpm !== null ? verticalRateFpm * FEET_PER_MINUTE_TO_MPS : null,

			last_contact_ms: typeof seen === 'number' ? adsbfiEventTimeMs(responseNowMs, seen) : null,
			squawk: typeof r['squawk'] === 'string' && r['squawk'] !== '' ? r['squawk'] : null,
			spi: typeof r['spi'] === 'number' ? r['spi'] === 1 : null,
			position_source: adsbfiTypeToPositionSource(r['type']),

			navigation_status: null,
			rate_of_turn: null,
			position_accuracy: null,
			destination: null,
			eta: null,
			draught_m: null,

			callsign: typeof flight === 'string' && flight.trim() !== '' ? flight.trim() : null,
			entity_subtype: adsbfiCategoryToSubtype(category),
			provider_category: category,
		},
	};
}

// ---- Provider dispatch (ADR-021) --------------------------------------------

export function normalizeByProvider(
	provider: 'opensky' | 'adsbfi',
	payload: Record<string, unknown>,
): NormalizeResult {
	switch (provider) {
		case 'opensky':
			return normalizeOpenSkyRecord(payload);
		case 'adsbfi':
			return normalizeAdsbfiRecord(payload);
	}
}
