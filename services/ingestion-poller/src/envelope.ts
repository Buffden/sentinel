// adsb.raw transport envelope (ADR-021).
//
// Every producer wraps the provider's record as { provider, payload } so the
// Position Consumer knows which provider sent a record before parsing it.
// The payload is the provider's own record plus the documented context the
// producer adds (OpenSky: fetched_at_ms; adsb.fi: response_now_ms, fetched_at_ms).
export type AdsbProvider = 'opensky' | 'adsbfi';

export function adsbRawEnvelope(provider: AdsbProvider, payload: object): string {
	return JSON.stringify({ provider, payload });
}
