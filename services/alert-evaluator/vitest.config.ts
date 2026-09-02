import { defineConfig } from 'vitest/config';

// No enforced threshold: evaluator.ts's main()/runLeaderSession() loop
// (leader-election orchestration, process signal handling) is untested —
// runScan()'s actual alert logic is covered, leader.ts is covered, but the
// wrapper loop needs a live multi-instance scenario, out of scope here.
// Overall ~61%, reporting only.
export default defineConfig({
	test: {
		coverage: {
			provider: 'v8',
			reporter: ['text', 'html'],
		},
	},
});
