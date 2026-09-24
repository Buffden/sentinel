import { describe, expect, it } from 'vitest';
import { adsbRawEnvelope } from './envelope.js';

describe('adsbRawEnvelope', () => {
	it('wraps a provider record as exactly { provider, payload }', () => {
		const payload = { icao24: 'abc123', fetched_at_ms: 1 };
		expect(JSON.parse(adsbRawEnvelope('opensky', payload))).toEqual({
			provider: 'opensky',
			payload,
		});
	});

	it('leaves the provider record itself unmodified', () => {
		const payload = { hex: '4caa40', seen_pos: 0.4, response_now_ms: 1790127082000 };
		expect(JSON.parse(adsbRawEnvelope('adsbfi', payload)).payload).toEqual(payload);
	});
});
