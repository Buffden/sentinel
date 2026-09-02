import { defineConfig } from 'vitest/config';

// config.ts requires JWT_SECRET (and other env vars) at import time.
// Vitest's `env` option sets these before any test file — and therefore
// before config.ts — is evaluated, so tests never need real secrets.
export default defineConfig({
	test: {
		env: {
			JWT_SECRET: 'test-secret-not-for-production',
		},
	},
});
