// The bounded health runner, with stand-in checks: no Postgres, Redis or
// Neo4j is touched, so this suite writes nothing shared.
import { describe, expect, it } from 'vitest';
import { checkHealth, NotConnectedError } from './health.js';

const TIMEOUT_MS = 50;
const hangs = () => new Promise<void>(() => {});

describe('checkHealth', () => {
	it('is healthy only when every check succeeds', async () => {
		const report = await checkHealth({ a: async () => {}, b: async () => {} }, TIMEOUT_MS);
		expect(report).toEqual({ healthy: true, checks: { a: { ok: true }, b: { ok: true } } });
	});

	it('answers within the bound when a check hangs, and names that dependency', async () => {
		const started = Date.now();
		const report = await checkHealth({ stuck: hangs, fine: async () => {} }, TIMEOUT_MS);
		expect(Date.now() - started).toBeLessThan(TIMEOUT_MS * 10);
		expect(report.healthy).toBe(false);
		expect(report.checks).toEqual({ stuck: { ok: false, reason: 'timeout' }, fine: { ok: true } });
	});

	it('distinguishes a client known to be disconnected from a failed call', async () => {
		const report = await checkHealth(
			{
				offline: async () => {
					throw new NotConnectedError('client status reconnecting');
				},
				refused: async () => {
					throw new Error('connect ECONNREFUSED');
				},
			},
			TIMEOUT_MS,
		);
		expect(report.checks).toEqual({
			offline: { ok: false, reason: 'not_connected' },
			refused: { ok: false, reason: 'error' },
		});
	});

	it('keeps at most one probe per dependency running while it hangs', async () => {
		let started = 0;
		let release = () => {};
		const hangsUntilReleased = () => {
			started++;
			return new Promise<void>((resolve) => (release = resolve));
		};
		const first = await checkHealth({ paused: hangsUntilReleased }, TIMEOUT_MS);
		expect(first.checks['paused']).toEqual({ ok: false, reason: 'timeout' });

		// Later requests during the same hang reuse the round's timeout result
		// at once, rather than each waiting out a timer of its own.
		for (let i = 0; i < 5; i++) {
			const startedAt = Date.now();
			const report = await checkHealth({ paused: hangsUntilReleased }, TIMEOUT_MS);
			expect(Date.now() - startedAt).toBeLessThan(TIMEOUT_MS / 2);
			expect(report.checks['paused']).toEqual({ ok: false, reason: 'timeout' });
		}
		expect(started).toBe(1);

		// Once the stuck probe settles, the next request starts a fresh one.
		release();
		await new Promise((r) => setTimeout(r, 0));
		expect(
			(await checkHealth({ paused: hangsUntilReleased }, TIMEOUT_MS)).checks['paused'],
		).toEqual({
			ok: false,
			reason: 'timeout',
		});
		expect(started).toBe(2);
	});

	it('handles a probe that fails after its timeout, then starts a fresh round', async () => {
		let started = 0;
		const failsLate = () => {
			started++;
			return new Promise<void>((_, reject) =>
				setTimeout(() => reject(new Error('late failure')), TIMEOUT_MS * 2),
			);
		};
		const report = await checkHealth({ late: failsLate }, TIMEOUT_MS);
		expect(report.checks['late']).toEqual({ ok: false, reason: 'timeout' });
		// Let the probe reject. An unhandled rejection here fails the run.
		await new Promise((r) => setTimeout(r, TIMEOUT_MS * 3));
		await checkHealth({ late: failsLate }, TIMEOUT_MS);
		expect(started).toBe(2);
	});

	it('reports healthy again once the dependency recovers', async () => {
		let up = false;
		const check = async () => {
			if (!up) throw new Error('down');
		};
		expect((await checkHealth({ recovering: check }, TIMEOUT_MS)).healthy).toBe(false);
		up = true;
		expect((await checkHealth({ recovering: check }, TIMEOUT_MS)).healthy).toBe(true);
	});
});
