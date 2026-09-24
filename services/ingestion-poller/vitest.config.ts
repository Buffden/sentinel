import { defineConfig } from 'vitest/config';

// No enforced threshold: the pure logic is tested (field mapping, credit
// budget, header parsing, backoff, pause and resume decisions), but
// fetchStateVectors/pollOnce/the poll loop need a live OpenSky HTTP call or a
// real fetch mock, so coverage is reporting only.
export default defineConfig({
	test: {
		coverage: {
			provider: 'v8',
			reporter: ['text', 'html'],
		},
	},
});
