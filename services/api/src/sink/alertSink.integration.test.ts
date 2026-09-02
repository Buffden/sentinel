// Integration test for alert persistence idempotency in alertSink.ts.
// Runs against a REAL TimescaleDB and Redis (docker-compose), not mocks —
// alerts.alert_id is a real Postgres primary key, and this proves a
// redelivered or re-detected alert for the same episode collides on it
// instead of creating a duplicate row, which a mocked pool couldn't prove.
//
// Requires: `make up && make migrate` (locally) or the CI service containers.
import { randomUUID } from 'node:crypto';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';
import { pool } from '../db.js';
import { redis } from '../redis.js';
import { config } from '../config.js';
import { persistAlert, type AlertMessage } from './alertSink.js';

function buildAlert(overrides: Partial<AlertMessage> = {}): AlertMessage {
	return {
		alert_id: `test-alert-${randomUUID()}`,
		entity_id: 'test-entity',
		entity_type: 'aircraft',
		alert_type: 'SIGNAL_LOSS',
		priority: 'STANDARD',
		status: 'NEW',
		detected_at_ms: 1_700_000_000_000,
		payload: { dark_since_ms: 1_699_999_000_000 },
		...overrides,
	};
}

describe('persistAlert — idempotent by alert_id (integration)', () => {
	beforeAll(async () => {
		await pool.query('SELECT 1'); // fail fast with a clear error if Postgres is unreachable
		await redis.ping(); // fail fast if Redis is unreachable
	});

	afterAll(async () => {
		await pool.end();
		await redis.quit();
	});

	it('replaying the same alert_id never produces a second row, and the first write wins', async () => {
		const first = buildAlert({ status: 'NEW', priority: 'STANDARD' });
		const raw1 = JSON.stringify(first);
		await persistAlert(first, raw1);

		try {
			// Simulated redelivery of the same episode's alert: same alert_id,
			// deliberately different mutable fields so it's unambiguous whether
			// this landed a second write or a real overwrite.
			const replay = buildAlert({
				alert_id: first.alert_id,
				status: 'ACKNOWLEDGED',
				priority: 'CRITICAL',
			});
			await persistAlert(replay, JSON.stringify(replay));

			const { rows } = await pool.query('SELECT status, priority FROM alerts WHERE alert_id = $1', [
				first.alert_id,
			]);
			expect(rows).toHaveLength(1);
			expect(rows[0].status).toBe('NEW');
			expect(rows[0].priority).toBe('STANDARD');
		} finally {
			await pool.query('DELETE FROM alerts WHERE alert_id = $1', [first.alert_id]);
		}
	});

	it('a different alert_id is a distinct row', async () => {
		const a = buildAlert();
		const b = buildAlert();

		try {
			await persistAlert(a, JSON.stringify(a));
			await persistAlert(b, JSON.stringify(b));

			const { rows } = await pool.query(
				'SELECT alert_id FROM alerts WHERE alert_id = ANY($1) ORDER BY alert_id',
				[[a.alert_id, b.alert_id]],
			);
			expect(rows.map((r: { alert_id: string }) => r.alert_id).sort()).toEqual(
				[a.alert_id, b.alert_id].sort(),
			);
		} finally {
			await pool.query('DELETE FROM alerts WHERE alert_id = ANY($1)', [[a.alert_id, b.alert_id]]);
		}
	});

	it('publishes the raw payload verbatim to alert-events for WebSocket fan-out', async () => {
		const alert = buildAlert();
		const raw = JSON.stringify(alert);

		const received = new Promise<string>((resolve) => {
			const sub = redis.duplicate();
			sub.subscribe(config.ALERT_EVENTS_CHANNEL, () => {
				sub.on('message', (_channel, message) => {
					resolve(message);
					sub.unsubscribe().then(() => sub.quit());
				});
			});
		});

		try {
			// Give the subscription a moment to actually register before publishing —
			// Redis pub/sub only delivers to subscribers active at publish time.
			await new Promise((r) => setTimeout(r, 200));
			await persistAlert(alert, raw);

			const message = await Promise.race([
				received,
				new Promise<string>((_, reject) => setTimeout(() => reject(new Error('timed out')), 5_000)),
			]);
			expect(message).toBe(raw);
		} finally {
			await pool.query('DELETE FROM alerts WHERE alert_id = $1', [alert.alert_id]);
		}
	});

	it('persists the row even if the channel has no active subscriber (pub/sub is fire-and-forget)', async () => {
		const alert = buildAlert();
		const raw = JSON.stringify(alert);

		try {
			await persistAlert(alert, raw);
			const { rows } = await pool.query('SELECT alert_id FROM alerts WHERE alert_id = $1', [
				alert.alert_id,
			]);
			expect(rows).toHaveLength(1);
		} finally {
			await pool.query('DELETE FROM alerts WHERE alert_id = $1', [alert.alert_id]);
		}
	});
});
