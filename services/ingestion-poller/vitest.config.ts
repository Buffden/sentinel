import { defineConfig } from 'vitest/config';

// No enforced threshold: only mapStateVector (pure field-mapping logic) is
// tested. fetchStateVectors/pollOnce/the poll loop need a live OpenSky HTTP
// call or a real fetch mock, neither in scope for this pass. Overall ~17%,
// reporting only — a threshold would be meaningless until those get tests.
export default defineConfig({
	test: {
		coverage: {
			provider: 'v8',
			reporter: ['text', 'html'],
		},
	},
});
