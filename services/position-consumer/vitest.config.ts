import { defineConfig } from 'vitest/config';

// No enforced threshold: consumer.ts's run()/handleMessage() Kafka consumer
// loop is deliberately untested (needs a live broker to exercise end-to-end,
// out of scope for this pass — see consumer.integration.test.ts's own scope
// note). That leaves overall coverage around 46%, which isn't a meaningful
// number to gate on until the loop itself gets a real test. Reporting only.
export default defineConfig({
	test: {
		coverage: {
			provider: 'v8',
			reporter: ['text', 'html'],
		},
	},
});
