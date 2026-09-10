import { defineConfig } from 'vitest/config';

// config.ts requires JWT_SECRET (and other env vars) at import time.
// Vitest's `env` option sets these before any test file — and therefore
// before config.ts — is evaluated, so tests never need real secrets.
//
// PG_URL/REDIS_URL: config.ts's own defaults (port 5432, password "sentinel")
// don't match this repo's actual docker-compose setup (port 5433, password
// "sentinel-dev" — see .env). That mismatch predates this file and is
// unrelated to testing; overridden here so alertSink.integration.test.ts
// reliably reaches the real local services regardless.
export default defineConfig({
	test: {
		// Integration files share one real Redis instance and its alert-events/
		// position-updates channels. Vitest's default file parallelism lets
		// wsServer's own tests (asserting "no message arrives in this window")
		// observe an unrelated publish from alertSink's tests running
		// concurrently in another worker, a cross-file interference, not a
		// logic bug in either file. Files still run in isolated module
		// registries; only the across-file concurrency is removed.
		fileParallelism: false,
		env: {
			JWT_SECRET: 'test-secret-not-for-production',
			PG_URL: 'postgresql://sentinel:sentinel-dev@localhost:5433/sentinel',
			REDIS_URL: 'redis://localhost:6379',
		},
		// No enforced threshold: coverage here ranges from ~18% (alertSink.ts's
		// Kafka consumer loop, deliberately out of scope — see its test file)
		// to ~98% (routes). A single global number would be either toothless
		// or immediately failing depending on which file it's measured against.
		// Reporting only, for visibility — a threshold is a decision to make
		// once there's an agreed per-file baseline, not a number to guess at.
		coverage: {
			provider: 'v8',
			reporter: ['text', 'html'],
		},
	},
});
