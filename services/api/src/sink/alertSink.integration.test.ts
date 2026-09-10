// Integration tests for alert persistence idempotency and Pre-CP5B composite
// supersession convergence. Runs against REAL TimescaleDB, Redis, and
// Redpanda (docker-compose), not mocks, alerts.alert_id is a real Postgres
// primary key, pg_advisory_xact_lock is a real Postgres mechanism, and the
// redelivery/failure tests below depend on a real Kafka consumer group's
// offset-commit behavior, none of which a mocked pool or client could prove.
//
// Requires: `make up && make migrate` (locally) or the CI service containers.
//
// WARNING — do not run this suite against a dev stack with the api service's
// own alert-sink running against the real `alerts` topic/group. These tests
// use disposable group IDs for the Kafka-consumer tests, but persistAlert
// itself writes directly to the real `alerts` table; clean up with
// `DELETE FROM alerts WHERE alert_id LIKE 'test-%'` if interrupted.
import { randomUUID } from 'node:crypto';
import { Kafka, Partitioners } from 'kafkajs';
import pg from 'pg';
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from 'vitest';
import { pool } from '../db.js';
import { redis } from '../redis.js';
import { config } from '../config.js';
import { persistAlert, startAlertSink, type AlertMessage } from './alertSink.js';
import {
	ALERT_SUPERSESSION_LOCK_NAMESPACE,
	AlertSupersessionInvariantError,
	persistCompositeAlert,
	persistIndividualAlert,
} from './compositeSupersession.js';

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

function buildComposite(
	supersedesAlertIds: string[],
	overrides: Partial<AlertMessage> = {},
): AlertMessage {
	return {
		alert_id: `test-composite-${randomUUID()}`,
		entity_id: 'test-entity',
		counterparty_entity_id: 'test-counterparty',
		entity_type: 'aircraft',
		alert_type: 'COMPOSITE',
		priority: 'ELEVATED',
		status: 'NEW',
		detected_at_ms: 1_700_000_100_000,
		payload: {
			signal_loss: { dark_since_ms: 1_699_999_000_000, loss_source: 'ACTIVE', resumed_at_ms: null },
			proximity: {},
			correlation_window_ms: 120_000,
			supersedes_alert_ids: supersedesAlertIds,
		},
		...overrides,
	};
}

async function cleanupAlerts(...alertIds: string[]): Promise<void> {
	await pool.query('DELETE FROM pending_alert_supersessions WHERE composite_alert_id = ANY($1)', [
		alertIds,
	]);
	await pool.query('DELETE FROM alerts WHERE alert_id = ANY($1)', [alertIds]);
}

// File-level hooks, not nested in any one describe: pool/redis are shared
// across every describe block below, and tearing them down after only the
// first block's tests finish would break every sibling block that runs
// after it (a real bug caught while first running this suite).
beforeAll(async () => {
	await pool.query('SELECT 1'); // fail fast with a clear error if Postgres is unreachable
	await redis.ping(); // fail fast if Redis is unreachable
});

afterAll(async () => {
	await pool.end();
	await redis.quit();
});

describe('persistAlert — idempotent by alert_id (integration)', () => {
	it('replaying the same alert_id never produces a second row, and the first write wins', async () => {
		const first = buildAlert({ status: 'NEW', priority: 'STANDARD' });
		await persistAlert(first);

		try {
			// Simulated redelivery of the same episode's alert: same alert_id,
			// deliberately different mutable fields so it's unambiguous whether
			// this landed a second write or a real overwrite.
			const replay = buildAlert({
				alert_id: first.alert_id,
				status: 'ACKNOWLEDGED',
				priority: 'CRITICAL',
			});
			await persistAlert(replay);

			const { rows } = await pool.query('SELECT status, priority FROM alerts WHERE alert_id = $1', [
				first.alert_id,
			]);
			expect(rows).toHaveLength(1);
			expect(rows[0].status).toBe('NEW');
			expect(rows[0].priority).toBe('STANDARD');
		} finally {
			await cleanupAlerts(first.alert_id);
		}
	});

	it('persists counterparty_entity_id for a proximity-style alert', async () => {
		const alert = buildAlert({
			alert_type: 'UNSCHEDULED_PROXIMITY',
			counterparty_entity_id: 'test-counterparty',
			payload: { pair_key: 'test-entity:test-counterparty', distance_metres: 42.5 },
		});

		try {
			await persistAlert(alert);

			const { rows } = await pool.query(
				'SELECT entity_id, counterparty_entity_id FROM alerts WHERE alert_id = $1',
				[alert.alert_id],
			);
			expect(rows).toHaveLength(1);
			expect(rows[0].entity_id).toBe('test-entity');
			expect(rows[0].counterparty_entity_id).toBe('test-counterparty');
		} finally {
			await cleanupAlerts(alert.alert_id);
		}
	});

	it('leaves counterparty_entity_id null for an alert type with no counterparty', async () => {
		const alert = buildAlert(); // SIGNAL_LOSS, no counterparty_entity_id set

		try {
			await persistAlert(alert);

			const { rows } = await pool.query(
				'SELECT counterparty_entity_id FROM alerts WHERE alert_id = $1',
				[alert.alert_id],
			);
			expect(rows[0].counterparty_entity_id).toBeNull();
		} finally {
			await cleanupAlerts(alert.alert_id);
		}
	});

	it('a different alert_id is a distinct row', async () => {
		const a = buildAlert();
		const b = buildAlert();

		try {
			await persistAlert(a);
			await persistAlert(b);

			const { rows } = await pool.query(
				'SELECT alert_id FROM alerts WHERE alert_id = ANY($1) ORDER BY alert_id',
				[[a.alert_id, b.alert_id]],
			);
			expect(rows.map((r: { alert_id: string }) => r.alert_id).sort()).toEqual(
				[a.alert_id, b.alert_id].sort(),
			);
		} finally {
			await cleanupAlerts(a.alert_id, b.alert_id);
		}
	});

	it('returns the persisted row for the caller to publish, reflecting real DB state', async () => {
		const alert = buildAlert();

		try {
			const [published] = await persistAlert(alert);
			expect(published.alert_id).toBe(alert.alert_id);
			expect(published.status).toBe('NEW');
			expect(published.superseded_by).toBeNull();
			expect(published.detected_at_ms).toBe(alert.detected_at_ms);
			expect(published.payload).toEqual(alert.payload);
		} finally {
			await cleanupAlerts(alert.alert_id);
		}
	});
});

// Pre-CP5B: composite supersession convergence, regardless of arrival order,
// against real Postgres. See DATA_MODEL.md's "Pre-CP5B: composite
// supersession convergence protocol" for the accepted design this proves.
describe('composite supersession convergence (Pre-CP5B, integration)', () => {
	it('SIGNAL_LOSS then COMPOSITE: referenced row transitions NEW -> SUPERSEDED', async () => {
		const signalLoss = buildAlert();
		const composite = buildComposite([signalLoss.alert_id]);

		try {
			await persistAlert(signalLoss);
			const published = await persistCompositeAlert(composite);

			expect(published).toHaveLength(2);
			const referenced = published.find((p) => p.alert_id === signalLoss.alert_id);
			expect(referenced?.status).toBe('SUPERSEDED');
			expect(referenced?.superseded_by).toBe(composite.alert_id);

			const { rows } = await pool.query(
				'SELECT status, superseded_by FROM alerts WHERE alert_id = $1',
				[signalLoss.alert_id],
			);
			expect(rows[0].status).toBe('SUPERSEDED');
			expect(rows[0].superseded_by).toBe(composite.alert_id);
		} finally {
			await cleanupAlerts(signalLoss.alert_id, composite.alert_id);
		}
	});

	it('COMPOSITE then SIGNAL_LOSS: the referenced alert lands directly as SUPERSEDED, never observably NEW', async () => {
		const signalLoss = buildAlert();
		const composite = buildComposite([signalLoss.alert_id]);

		try {
			const compositePublished = await persistCompositeAlert(composite);
			// Nothing to publish yet for the referenced id, it doesn't exist.
			expect(compositePublished).toHaveLength(1);
			expect(compositePublished[0].alert_id).toBe(composite.alert_id);

			const pendingBefore = await pool.query(
				'SELECT composite_alert_id FROM pending_alert_supersessions WHERE referenced_alert_id = $1',
				[signalLoss.alert_id],
			);
			expect(pendingBefore.rows[0].composite_alert_id).toBe(composite.alert_id);

			const published = await persistIndividualAlert(signalLoss);
			expect(published.status).toBe('SUPERSEDED');
			expect(published.superseded_by).toBe(composite.alert_id);

			const { rows } = await pool.query(
				'SELECT status, superseded_by FROM alerts WHERE alert_id = $1',
				[signalLoss.alert_id],
			);
			expect(rows[0].status).toBe('SUPERSEDED');
			expect(rows[0].superseded_by).toBe(composite.alert_id);

			const pendingAfter = await pool.query(
				'SELECT 1 FROM pending_alert_supersessions WHERE referenced_alert_id = $1',
				[signalLoss.alert_id],
			);
			expect(pendingAfter.rows).toHaveLength(0); // consumed
		} finally {
			await cleanupAlerts(signalLoss.alert_id, composite.alert_id);
		}
	});

	it("never fabricates the late-arriving alert's canonical fields: real payload/detected_at/entity_type persist, not the composite's own", async () => {
		const signalLoss = buildAlert({
			entity_type: 'vessel',
			detected_at_ms: 1_650_000_000_000, // deliberately far from the composite's own detected_at_ms
			payload: { dark_since_ms: 1_649_999_000_000, callsign: 'TEST123', last_known_lat: 51.5 },
		});
		const composite = buildComposite([signalLoss.alert_id], { detected_at_ms: 1_700_000_100_000 });

		try {
			await persistCompositeAlert(composite);
			const published = await persistIndividualAlert(signalLoss);

			expect(published.entity_type).toBe('vessel');
			expect(published.detected_at_ms).toBe(1_650_000_000_000);
			expect(published.payload).toEqual({
				dark_since_ms: 1_649_999_000_000,
				callsign: 'TEST123',
				last_known_lat: 51.5,
			});

			const { rows } = await pool.query(
				'SELECT entity_type, detected_at, payload FROM alerts WHERE alert_id = $1',
				[signalLoss.alert_id],
			);
			expect(rows[0].entity_type).toBe('vessel');
			expect(new Date(rows[0].detected_at).getTime()).toBe(1_650_000_000_000);
			expect(rows[0].payload).toEqual({
				dark_since_ms: 1_649_999_000_000,
				callsign: 'TEST123',
				last_known_lat: 51.5,
			});
		} finally {
			await cleanupAlerts(signalLoss.alert_id, composite.alert_id);
		}
	});

	it('ACKNOWLEDGED -> SUPERSEDED, in normal order', async () => {
		const signalLoss = buildAlert();
		const composite = buildComposite([signalLoss.alert_id]);

		try {
			await persistAlert(signalLoss);
			await pool.query("UPDATE alerts SET status = 'ACKNOWLEDGED' WHERE alert_id = $1", [
				signalLoss.alert_id,
			]);

			await persistCompositeAlert(composite);

			const { rows } = await pool.query(
				'SELECT status, superseded_by FROM alerts WHERE alert_id = $1',
				[signalLoss.alert_id],
			);
			expect(rows[0].status).toBe('SUPERSEDED');
			expect(rows[0].superseded_by).toBe(composite.alert_id);
		} finally {
			await cleanupAlerts(signalLoss.alert_id, composite.alert_id);
		}
	});

	it('RESOLVED remains terminal: never retroactively superseded', async () => {
		const signalLoss = buildAlert();
		const composite = buildComposite([signalLoss.alert_id]);

		try {
			await persistAlert(signalLoss);
			await pool.query(
				"UPDATE alerts SET status = 'RESOLVED', resolved_at = now() WHERE alert_id = $1",
				[signalLoss.alert_id],
			);

			const published = await persistCompositeAlert(composite);
			// Only the composite itself is published; RESOLVED is untouched, nothing changed for it.
			expect(published).toHaveLength(1);
			expect(published[0].alert_id).toBe(composite.alert_id);

			const { rows } = await pool.query(
				'SELECT status, superseded_by FROM alerts WHERE alert_id = $1',
				[signalLoss.alert_id],
			);
			expect(rows[0].status).toBe('RESOLVED');
			expect(rows[0].superseded_by).toBeNull();
		} finally {
			await cleanupAlerts(signalLoss.alert_id, composite.alert_id);
		}
	});

	it('already SUPERSEDED by the same composite is an idempotent replay', async () => {
		const signalLoss = buildAlert();
		const composite = buildComposite([signalLoss.alert_id]);

		try {
			await persistAlert(signalLoss);
			await persistCompositeAlert(composite);

			// Redelivery of the identical COMPOSITE message.
			const published = await persistCompositeAlert(composite);
			expect(published).toHaveLength(2);
			const referenced = published.find((p) => p.alert_id === signalLoss.alert_id);
			expect(referenced?.status).toBe('SUPERSEDED');
			expect(referenced?.superseded_by).toBe(composite.alert_id);

			const { rows } = await pool.query(
				'SELECT status, superseded_by, updated_at FROM alerts WHERE alert_id = $1',
				[signalLoss.alert_id],
			);
			expect(rows).toHaveLength(1);
			expect(rows[0].status).toBe('SUPERSEDED');
			expect(rows[0].superseded_by).toBe(composite.alert_id);
		} finally {
			await cleanupAlerts(signalLoss.alert_id, composite.alert_id);
		}
	});

	it('already SUPERSEDED by a different composite (existing row) is an invariant failure, and rolls back', async () => {
		const signalLoss = buildAlert();
		const firstComposite = buildComposite([signalLoss.alert_id]);
		const secondComposite = buildComposite([signalLoss.alert_id]);

		try {
			await persistAlert(signalLoss);
			await persistCompositeAlert(firstComposite);

			await expect(persistCompositeAlert(secondComposite)).rejects.toBeInstanceOf(
				AlertSupersessionInvariantError,
			);

			// Rolled back: the second composite's own row must not exist either,
			// the whole transaction, including its own insert, is one unit.
			const { rows } = await pool.query('SELECT 1 FROM alerts WHERE alert_id = $1', [
				secondComposite.alert_id,
			]);
			expect(rows).toHaveLength(0);

			// The original supersession is untouched.
			const referenced = await pool.query('SELECT superseded_by FROM alerts WHERE alert_id = $1', [
				signalLoss.alert_id,
			]);
			expect(referenced.rows[0].superseded_by).toBe(firstComposite.alert_id);
		} finally {
			await cleanupAlerts(signalLoss.alert_id, firstComposite.alert_id, secondComposite.alert_id);
		}
	});

	it('a pending supersession owned by a different composite is an invariant failure, and rolls back', async () => {
		const signalLoss = buildAlert();
		const firstComposite = buildComposite([signalLoss.alert_id]);
		const secondComposite = buildComposite([signalLoss.alert_id]);

		try {
			// Out-of-order: first composite arrives before the referenced alert, pending row created.
			await persistCompositeAlert(firstComposite);

			await expect(persistCompositeAlert(secondComposite)).rejects.toBeInstanceOf(
				AlertSupersessionInvariantError,
			);

			const { rows } = await pool.query('SELECT 1 FROM alerts WHERE alert_id = $1', [
				secondComposite.alert_id,
			]);
			expect(rows).toHaveLength(0);

			const pending = await pool.query(
				'SELECT composite_alert_id FROM pending_alert_supersessions WHERE referenced_alert_id = $1',
				[signalLoss.alert_id],
			);
			expect(pending.rows[0].composite_alert_id).toBe(firstComposite.alert_id); // unchanged
		} finally {
			await cleanupAlerts(signalLoss.alert_id, firstComposite.alert_id, secondComposite.alert_id);
		}
	});

	it('COMPOSITE replay: idempotent even when the retry mutates nothing (referenced row already SUPERSEDED)', async () => {
		const signalLoss = buildAlert();
		const composite = buildComposite([signalLoss.alert_id]);

		try {
			await persistAlert(signalLoss);
			await persistCompositeAlert(composite);

			// Second delivery: composite INSERT is ON CONFLICT DO NOTHING, referenced
			// UPDATE affects 0 rows (already SUPERSEDED, not NEW/ACKNOWLEDGED).
			const published = await persistCompositeAlert(composite);
			expect(published).toHaveLength(2);

			const alerts = await pool.query('SELECT alert_id FROM alerts WHERE alert_id = ANY($1)', [
				[signalLoss.alert_id, composite.alert_id],
			]);
			expect(alerts.rows).toHaveLength(2); // no duplicate rows
		} finally {
			await cleanupAlerts(signalLoss.alert_id, composite.alert_id);
		}
	});

	it('late SIGNAL_LOSS replay: idempotent after being consumed via the pending path', async () => {
		const signalLoss = buildAlert();
		const composite = buildComposite([signalLoss.alert_id]);

		try {
			await persistCompositeAlert(composite);
			await persistIndividualAlert(signalLoss); // consumes the pending row

			// Redelivery of the identical SIGNAL_LOSS message.
			const published = await persistIndividualAlert(signalLoss);
			expect(published.status).toBe('SUPERSEDED');
			expect(published.superseded_by).toBe(composite.alert_id);

			const { rows } = await pool.query(
				'SELECT status, superseded_by FROM alerts WHERE alert_id = $1',
				[signalLoss.alert_id],
			);
			expect(rows).toHaveLength(1);
			expect(rows[0].status).toBe('SUPERSEDED'); // never reverted to NEW
		} finally {
			await cleanupAlerts(signalLoss.alert_id, composite.alert_id);
		}
	});

	it('a referenced alert that never arrives leaves a valid, harmless pending row and no fabricated alert row', async () => {
		const signalLossAlertId = `test-alert-${randomUUID()}`;
		const composite = buildComposite([signalLossAlertId]);

		try {
			await persistCompositeAlert(composite);

			const { rows: alertRows } = await pool.query('SELECT 1 FROM alerts WHERE alert_id = $1', [
				signalLossAlertId,
			]);
			expect(alertRows).toHaveLength(0); // no placeholder row ever created

			const pending = await pool.query(
				'SELECT composite_alert_id FROM pending_alert_supersessions WHERE referenced_alert_id = $1',
				[signalLossAlertId],
			);
			expect(pending.rows[0].composite_alert_id).toBe(composite.alert_id);
		} finally {
			await cleanupAlerts(signalLossAlertId, composite.alert_id);
		}
	});
});

// Advisory-lock mechanism and the concurrency guarantees it exists to prove.
// Real Postgres locking, not simulated timing.
describe('advisory locking (Pre-CP5B, integration)', () => {
	it('pg_advisory_xact_lock genuinely blocks a second transaction on the same alert_id', async () => {
		const alertId = `test-alert-${randomUUID()}`;
		const clientA = await pool.connect();
		const clientB = await pool.connect();

		try {
			await clientA.query('BEGIN');
			await clientA.query('SELECT pg_advisory_xact_lock($1, hashtext($2))', [
				ALERT_SUPERSESSION_LOCK_NAMESPACE,
				alertId,
			]);

			await clientB.query('BEGIN');
			let bResolved = false;
			const bLock = clientB
				.query('SELECT pg_advisory_xact_lock($1, hashtext($2))', [
					ALERT_SUPERSESSION_LOCK_NAMESPACE,
					alertId,
				])
				.then(() => {
					bResolved = true;
				});

			await new Promise((r) => setTimeout(r, 300));
			expect(bResolved).toBe(false); // still blocked while A holds the lock

			await clientA.query('COMMIT'); // releases A's lock
			await bLock; // now resolves
			expect(bResolved).toBe(true);

			await clientB.query('COMMIT');
		} finally {
			clientA.release();
			clientB.release();
		}
	});

	it('real concurrent arrival converges to exactly one canonical decision, no dangling claim either way', async () => {
		const signalLoss = buildAlert();
		const composite = buildComposite([signalLoss.alert_id]);

		try {
			await Promise.all([persistCompositeAlert(composite), persistIndividualAlert(signalLoss)]);

			const { rows } = await pool.query(
				'SELECT status, superseded_by FROM alerts WHERE alert_id = $1',
				[signalLoss.alert_id],
			);
			expect(rows).toHaveLength(1);
			expect(rows[0].status).toBe('SUPERSEDED');
			expect(rows[0].superseded_by).toBe(composite.alert_id);

			const pending = await pool.query(
				'SELECT 1 FROM pending_alert_supersessions WHERE referenced_alert_id = $1',
				[signalLoss.alert_id],
			);
			expect(pending.rows).toHaveLength(0); // never left dangling either way
		} finally {
			await cleanupAlerts(signalLoss.alert_id, composite.alert_id);
		}
	});

	it('sorted multi-lock acquisition: a COMPOSITE with two referenced ids racing two individual arrivals completes without deadlock', async () => {
		const a = buildAlert();
		const b = buildAlert();
		const composite = buildComposite([a.alert_id, b.alert_id]);

		try {
			const results = await Promise.race([
				Promise.all([
					persistCompositeAlert(composite),
					persistIndividualAlert(a),
					persistIndividualAlert(b),
				]),
				new Promise((_, reject) => setTimeout(() => reject(new Error('deadlock timeout')), 10_000)),
			]);
			expect(results).toBeDefined(); // completed, did not hit the timeout

			const { rows } = await pool.query(
				'SELECT alert_id, status, superseded_by FROM alerts WHERE alert_id = ANY($1) ORDER BY alert_id',
				[[a.alert_id, b.alert_id]],
			);
			expect(rows).toHaveLength(2);
			for (const row of rows) {
				expect(row.status).toBe('SUPERSEDED');
				expect(row.superseded_by).toBe(composite.alert_id);
			}
		} finally {
			await cleanupAlerts(a.alert_id, b.alert_id, composite.alert_id);
		}
	}, 15_000);
});

// Full consumer loop: real Redpanda, disposable group IDs so this never
// joins the real api alert-sink group. Publish-after-commit and the
// specific replay-safety failure mode (a later publish in one message's
// fan-out fails, redelivery must republish everything, not only what it
// failed on) can only be proven end-to-end through the real eachMessage
// wrapper, not through persistAlert alone.
describe('startAlertSink publish after commit and replay-safe redelivery (integration)', () => {
	const testKafka = new Kafka({
		clientId: 'api-alertsink-test',
		brokers: config.KAFKA_BROKERS,
		logLevel: 0,
	});
	const producer = testKafka.producer({ createPartitioner: Partitioners.LegacyPartitioner });

	const admin = testKafka.admin();

	beforeAll(async () => {
		await producer.connect();
		await admin.connect();
	});

	afterAll(async () => {
		await producer.disconnect();
		await admin.disconnect();
	});

	// A fresh consumer group's initial offset (fromBeginning: false) is only
	// resolved once the group has actually joined and been assigned its
	// partition. consumer.run() resolves once the run loop starts, not once
	// that join/assignment has completed, so producing immediately after
	// startAlertSink() races the group actually being ready to receive,
	// intermittently missing the very first message. Poll real group state
	// (the same admin.describeGroups pattern the Alert Evaluator's own
	// ADR-005 tests use) rather than guessing at a fixed delay.
	async function waitForGroupReady(groupId: string, timeoutMs = 15_000): Promise<void> {
		const deadline = Date.now() + timeoutMs;
		while (Date.now() < deadline) {
			const { groups } = await admin.describeGroups([groupId]);
			if ((groups[0]?.members.length ?? 0) > 0) return;
			await new Promise((r) => setTimeout(r, 200));
		}
		throw new Error(`group ${groupId} never became ready within ${timeoutMs}ms`);
	}

	async function waitForAlertEvents(
		predicate: (msg: Record<string, unknown>) => boolean,
		count: number,
		timeoutMs = 10_000,
	): Promise<Record<string, unknown>[]> {
		const sub = redis.duplicate();
		const received: Record<string, unknown>[] = [];
		await new Promise<void>((resolve) =>
			sub.subscribe(config.ALERT_EVENTS_CHANNEL, () => resolve()),
		);
		sub.on('message', (_channel, message) => {
			const parsed = JSON.parse(message) as Record<string, unknown>;
			if (predicate(parsed)) received.push(parsed);
		});

		const deadline = Date.now() + timeoutMs;
		while (received.length < count && Date.now() < deadline) {
			await new Promise((r) => setTimeout(r, 100));
		}
		await sub.unsubscribe();
		await sub.quit();
		return received;
	}

	it('persists and publishes the canonical row after commit, for a plain individual alert', async () => {
		const groupId = `test-api-alertsink-${randomUUID()}`;
		const alert = buildAlert();

		const session = await startAlertSink(groupId);
		try {
			await waitForGroupReady(groupId);
			const eventsPromise = waitForAlertEvents((m) => m['alert_id'] === alert.alert_id, 1);
			await producer.send({
				topic: config.ALERTS_TOPIC,
				messages: [{ key: alert.alert_id, value: JSON.stringify(alert) }],
			});

			const events = await eventsPromise;
			expect(events).toHaveLength(1);
			expect(events[0]['status']).toBe('NEW');
			expect(events[0]['alert_type']).toBe('SIGNAL_LOSS');
		} finally {
			await session.stop();
			await cleanupAlerts(alert.alert_id);
		}
	}, 40_000);

	it('a Redis publish failure on the second of two publishes is not committed; redelivery republishes both durable events even though it performs no new DB transition', async () => {
		const groupId = `test-api-alertsink-${randomUUID()}`;
		const signalLoss = buildAlert();
		const composite = buildComposite([signalLoss.alert_id]);

		// Seed the referenced alert as already NEW so the COMPOSITE message's
		// own transaction does real work (supersedes it) on its first delivery.
		await persistAlert(signalLoss);

		// Subscribe before producing anything: Redis pub/sub has no replay, and
		// KafkaJS retries a throwing eachMessage internally (bounded by its own
		// retry policy) before this test ever gets a chance to intervene, so the
		// only reliable way to observe both the failed and the eventually
		// successful publish is to already be listening when it happens.
		const sub = redis.duplicate();
		const seen: Record<string, unknown>[] = [];
		await new Promise<void>((resolve) =>
			sub.subscribe(config.ALERT_EVENTS_CHANNEL, () => resolve()),
		);
		sub.on('message', (_channel, message) => {
			const parsed = JSON.parse(message) as Record<string, unknown>;
			if (parsed['alert_id'] === composite.alert_id || parsed['alert_id'] === signalLoss.alert_id) {
				seen.push(parsed);
			}
		});

		const realPublish = redis.publish.bind(redis);
		let callCount = 0;
		let failedOnce = false;
		const publishSpy = vi
			.spyOn(redis, 'publish')
			.mockImplementation(async (channel: string | Buffer, message: string | Buffer) => {
				callCount++;
				if (callCount === 2 && !failedOnce) {
					failedOnce = true;
					throw new Error('simulated Redis publish failure');
				}
				return realPublish(channel, message);
			});

		const session = await startAlertSink(groupId);
		try {
			await waitForGroupReady(groupId);
			await producer.send({
				topic: config.ALERTS_TOPIC,
				messages: [{ key: composite.alert_id, value: JSON.stringify(composite) }],
			});

			// Wait for the eventual, fully-successful outcome: both the composite
			// and the referenced alert (as SUPERSEDED) observed on alert-events.
			// This happens through KafkaJS's own retry of the still-uncommitted
			// message, not a manual restart, real redelivery under an unchanged
			// offset, exactly the mechanism the offset-commit-last ordering exists
			// to trigger.
			const deadline = Date.now() + 90_000;
			while (
				Date.now() < deadline &&
				!(
					seen.some((e) => e['alert_id'] === composite.alert_id) &&
					seen.some((e) => e['alert_id'] === signalLoss.alert_id && e['status'] === 'SUPERSEDED')
				)
			) {
				await new Promise((r) => setTimeout(r, 100));
			}

			expect(failedOnce).toBe(true); // the simulated failure genuinely happened
			expect(callCount).toBeGreaterThanOrEqual(3); // attempt 1 (success, failure) + at least one retried publish

			const compositeEvent = seen.find((e) => e['alert_id'] === composite.alert_id);
			const referencedEvent = seen.find(
				(e) => e['alert_id'] === signalLoss.alert_id && e['status'] === 'SUPERSEDED',
			);
			expect(compositeEvent).toBeDefined(); // republished even though a retry's own DB work is a no-op
			expect(referencedEvent).toBeDefined();
			expect(referencedEvent?.['superseded_by']).toBe(composite.alert_id);

			// The DB itself was already fully correct after attempt 1's
			// transaction, only its second publish failed; confirm no duplicate
			// rows exist regardless of how many retries ran.
			const finalRows = await pool.query(
				'SELECT alert_id, status, superseded_by FROM alerts WHERE alert_id = ANY($1)',
				[[signalLoss.alert_id, composite.alert_id]],
			);
			expect(finalRows.rows).toHaveLength(2);
			const referencedRow = finalRows.rows.find(
				(r: { alert_id: string }) => r.alert_id === signalLoss.alert_id,
			);
			expect(referencedRow.status).toBe('SUPERSEDED');
			expect(referencedRow.superseded_by).toBe(composite.alert_id);
		} finally {
			publishSpy.mockRestore();
			await sub.unsubscribe();
			await sub.quit();
			await session.stop();
			await cleanupAlerts(signalLoss.alert_id, composite.alert_id);
		}
	}, 100_000);
});
