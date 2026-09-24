// Position Consumer — adsb.raw record classification (ADR-021).
//
// Decides which provider produced an adsb.raw record BEFORE any provider
// mapping runs, so the archive can record the provider even when that
// provider's normalization later rejects the record.
//
//   enveloped   { provider, payload }         → that provider's mapping
//   legacy      bare OpenSky poller record    → OpenSky mapping (replay only)
//   rejected    unknown_provider | invalid_envelope | unidentified_provider
//               | parse_error                 → raw_events (provider null) + DLQ
//
// There is deliberately no default provider. A record that cannot be
// identified is never normalized as OpenSky.
//
// Pure: no I/O, so every branch is unit-testable without infrastructure.

export const KNOWN_ADSB_PROVIDERS = ['opensky', 'adsbfi'] as const;
export type AdsbProvider = (typeof KNOWN_ADSB_PROVIDERS)[number];

export type RejectionKind =
	'parse_error' | 'invalid_envelope' | 'unknown_provider' | 'unidentified_provider';

export type Classification =
	| {
			ok: true;
			provider: AdsbProvider;
			// Only what the provider sent: the envelope's payload, or the legacy
			// record itself. This is what raw_events.payload stores.
			payload: Record<string, unknown>;
			legacy: boolean;
	  }
	| { ok: false; kind: RejectionKind; detail: string };

function isPlainObject(v: unknown): v is Record<string, unknown> {
	return typeof v === 'object' && v !== null && !Array.isArray(v);
}

function isKnownProvider(v: string): v is AdsbProvider {
	return (KNOWN_ADSB_PROVIDERS as readonly string[]).includes(v);
}

// Identifies records written by Sentinel's own OpenSky poller before the
// envelope existed. Checks shape only, never field validity: the OpenSky
// normalizer still decides no_position, wrong types and every other outcome,
// so legacy replay behaves exactly as it did before classification existed.
function isLegacyOpenSkyRecord(r: Record<string, unknown>): boolean {
	return (
		typeof r['icao24'] === 'string' &&
		r['icao24'] !== '' &&
		typeof r['fetched_at_ms'] === 'number' &&
		'lat' in r &&
		'lon' in r &&
		'time_position' in r
	);
}

export function classifyAdsbRaw(rawValue: string): Classification {
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

	if (!isPlainObject(parsed)) {
		return { ok: false, kind: 'parse_error', detail: 'payload is not a JSON object' };
	}

	// Either envelope key marks the record as an envelope attempt; a malformed
	// one is rejected rather than falling through to the legacy check.
	if ('provider' in parsed || 'payload' in parsed) {
		const keys = Object.keys(parsed);
		if (keys.length !== 2 || !('provider' in parsed) || !('payload' in parsed)) {
			return {
				ok: false,
				kind: 'invalid_envelope',
				detail: `envelope must have exactly provider and payload, got: ${keys.join(', ')}`,
			};
		}
		const provider = parsed['provider'];
		const payload = parsed['payload'];
		if (typeof provider !== 'string') {
			return { ok: false, kind: 'invalid_envelope', detail: 'provider is not a string' };
		}
		if (!isPlainObject(payload)) {
			return { ok: false, kind: 'invalid_envelope', detail: 'payload is not a JSON object' };
		}
		if (!isKnownProvider(provider)) {
			return {
				ok: false,
				kind: 'unknown_provider',
				detail: `provider "${provider}" is not accepted`,
			};
		}
		return { ok: true, provider, payload, legacy: false };
	}

	if (isLegacyOpenSkyRecord(parsed)) {
		return { ok: true, provider: 'opensky', payload: parsed, legacy: true };
	}

	return {
		ok: false,
		kind: 'unidentified_provider',
		detail: 'no envelope and not the legacy OpenSky record shape',
	};
}
