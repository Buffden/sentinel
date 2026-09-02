import { defineConfig } from 'vitest/config'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const dirname = path.dirname(fileURLToPath(import.meta.url))

// Mirrors tsconfig.json's "@/*" -> "./src/*" path alias. Vitest (Vite's
// module resolution) doesn't read tsconfig paths on its own.
export default defineConfig({
	resolve: {
		alias: {
			'@': path.resolve(dirname, './src'),
		},
	},
})
