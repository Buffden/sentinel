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
		env: {
			JWT_SECRET: 'test-secret-not-for-production',
			PG_URL: 'postgresql://sentinel:sentinel-dev@localhost:5433/sentinel',
			REDIS_URL: 'redis://localhost:6379',
		},
	},
});
