// Integration tests for the PATCH /alerts/:alert_id transition contract
// (Phase 08 CP2). Runs against REAL TimescaleDB — alerts.alert_id is a real
// Postgres primary key and pg_advisory_xact_lock is a real Postgres
// mechanism, neither of which a mocked pool could prove.
//
// Requires: `make up && make migrate` (locally) or the CI service containers.
import { randomUUID } from 'node:crypto';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';
import { pool } from '../db.js';
import { transitionAlert } from './alertLifecycle.js';
import {
	ALERT_SUPERSESSION_LOCK_NAMESPACE,
	persistCompositeAlert,
	persistIndividualAlert,
} from '../sink/compositeSupersession.js';
import type { AlertMessage } from '../sink/alertSink.js';

// acknowledged_by/resolved_by are REFERENCES users(user_id) (migration 006)
// -- real rows are required, not just plausible-looking UUIDs, or the
// transition's own UPDATE fails its FK check.
const ACTOR_USER_ID = randomUUID();
const SECOND_ACTOR_USER_ID = randomUUID();

async function seedUser(userId: string): Promise<void> {
	await pool.query(
		`INSERT INTO users (user_id, google_sub, email, last_login_at, created_at)
		 VALUES ($1, $2, $3, now(), now())`,
		[userId, `google-${userId}`, `${userId}@example.com`],
	);
}

function buildAlert(overrides: Partial<AlertMessage> = {}): AlertMessage {
	return {
		alert_id: `test-lifecycle-${randomUUID()}`,
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
		alert_id: `test-lifecycle-composite-${randomUUID()}`,
		entity_id: 'test-entity',
		entity_type: 'aircraft',
		alert_type: 'COMPOSITE',
		priority: 'ELEVATED',
		status: 'NEW',
		detected_at_ms: 1_700_000_100_000,
		payload: { supersedes_alert_ids: supersedesAlertIds },
		...overrides,
	};
}

async function cleanupAlerts(...alertIds: string[]): Promise<void> {
	await pool.query('DELETE FROM alerts WHERE alert_id = ANY($1)', [alertIds]);
	await pool.query('DELETE FROM pending_alert_supersessions WHERE referenced_alert_id = ANY($1)', [
		alertIds,
	]);
}

describe('transitionAlert (integration)', () => {
	const seededIds: string[] = [];

	beforeAll(async () => {
		await seedUser(ACTOR_USER_ID);
		await seedUser(SECOND_ACTOR_USER_ID);
	});

	afterAll(async () => {
		await pool.query('DELETE FROM users WHERE user_id = ANY($1)', [
			[ACTOR_USER_ID, SECOND_ACTOR_USER_ID],
		]);
	});

	afterEach(async () => {
		if (seededIds.length > 0) {
			await cleanupAlerts(...seededIds);
			seededIds.length = 0;
		}
	});

	it('returns not_found for an alert_id that does not exist', async () => {
		const outcome = await transitionAlert('does-not-exist', 'ACKNOWLEDGED', ACTOR_USER_ID);
		expect(outcome.kind).toBe('not_found');
	});

	it('NEW -> ACKNOWLEDGED sets acknowledged_at/by and leaves resolved fields null', async () => {
		const alert = buildAlert();
		await persistIndividualAlert(alert);
		seededIds.push(alert.alert_id);

		const outcome = await transitionAlert(alert.alert_id, 'ACKNOWLEDGED', ACTOR_USER_ID);
		expect(outcome.kind).toBe('applied');

		const { rows } = await pool.query(
			'SELECT status, acknowledged_at, acknowledged_by, resolved_at, resolved_by FROM alerts WHERE alert_id = $1',
			[alert.alert_id],
		);
		expect(rows[0].status).toBe('ACKNOWLEDGED');
		expect(rows[0].acknowledged_at).not.toBeNull();
		expect(rows[0].acknowledged_by).toBe(ACTOR_USER_ID);
		expect(rows[0].resolved_at).toBeNull();
		expect(rows[0].resolved_by).toBeNull();
	});

	it('NEW -> RESOLVED sets resolved_at/by directly, acknowledged fields stay null', async () => {
		const alert = buildAlert();
		await persistIndividualAlert(alert);
		seededIds.push(alert.alert_id);

		const outcome = await transitionAlert(alert.alert_id, 'RESOLVED', ACTOR_USER_ID);
		expect(outcome.kind).toBe('applied');

		const { rows } = await pool.query(
			'SELECT status, acknowledged_at, resolved_at, resolved_by FROM alerts WHERE alert_id = $1',
			[alert.alert_id],
		);
		expect(rows[0].status).toBe('RESOLVED');
		expect(rows[0].acknowledged_at).toBeNull();
		expect(rows[0].resolved_at).not.toBeNull();
		expect(rows[0].resolved_by).toBe(ACTOR_USER_ID);
	});

	it('ACKNOWLEDGED -> RESOLVED sets resolved fields without disturbing the earlier acknowledged fields', async () => {
		const alert = buildAlert();
		await persistIndividualAlert(alert);
		seededIds.push(alert.alert_id);

		await transitionAlert(alert.alert_id, 'ACKNOWLEDGED', ACTOR_USER_ID);
		const { rows: afterAck } = await pool.query(
			'SELECT acknowledged_at FROM alerts WHERE alert_id = $1',
			[alert.alert_id],
		);
		const acknowledgedAt = afterAck[0].acknowledged_at as Date;

		const outcome = await transitionAlert(alert.alert_id, 'RESOLVED', SECOND_ACTOR_USER_ID);
		expect(outcome.kind).toBe('applied');

		const { rows } = await pool.query(
			'SELECT status, acknowledged_at, resolved_at, resolved_by FROM alerts WHERE alert_id = $1',
			[alert.alert_id],
		);
		expect(rows[0].status).toBe('RESOLVED');
		expect(rows[0].acknowledged_at).toEqual(acknowledgedAt);
		expect(rows[0].resolved_by).toBe(SECOND_ACTOR_USER_ID);
	});

	it('replaying the same target status is an idempotent no-op, not a second write', async () => {
		const alert = buildAlert();
		await persistIndividualAlert(alert);
		seededIds.push(alert.alert_id);

		await transitionAlert(alert.alert_id, 'ACKNOWLEDGED', ACTOR_USER_ID);
		const { rows: firstAck } = await pool.query(
			'SELECT acknowledged_at, updated_at FROM alerts WHERE alert_id = $1',
			[alert.alert_id],
		);

		// A different actor id replaying the same target must not steal
		// acknowledged_by from whoever actually performed the transition.
		const outcome = await transitionAlert(alert.alert_id, 'ACKNOWLEDGED', randomUUID());
		expect(outcome.kind).toBe('idempotent');

		const { rows: secondAck } = await pool.query(
			'SELECT acknowledged_at, acknowledged_by, updated_at FROM alerts WHERE alert_id = $1',
			[alert.alert_id],
		);
		expect(secondAck[0].acknowledged_at).toEqual(firstAck[0].acknowledged_at);
		expect(secondAck[0].acknowledged_by).toBe(ACTOR_USER_ID);
		expect(secondAck[0].updated_at).toEqual(firstAck[0].updated_at);
	});

	it('RESOLVED -> ACKNOWLEDGED is rejected as an invalid transition, returning the real current row', async () => {
		const alert = buildAlert();
		await persistIndividualAlert(alert);
		seededIds.push(alert.alert_id);

		await transitionAlert(alert.alert_id, 'RESOLVED', ACTOR_USER_ID);
		const outcome = await transitionAlert(alert.alert_id, 'ACKNOWLEDGED', ACTOR_USER_ID);

		expect(outcome.kind).toBe('invalid_transition');
		if (outcome.kind === 'invalid_transition') {
			expect(outcome.alert.status).toBe('RESOLVED');
		}
	});

	it('SUPERSEDED is terminal: an operator can never move a system-superseded alert', async () => {
		const signalLoss = buildAlert();
		const composite = buildComposite([signalLoss.alert_id]);
		await persistIndividualAlert(signalLoss);
		await persistCompositeAlert(composite);
		seededIds.push(signalLoss.alert_id, composite.alert_id);

		const outcome = await transitionAlert(signalLoss.alert_id, 'ACKNOWLEDGED', ACTOR_USER_ID);
		expect(outcome.kind).toBe('invalid_transition');
		if (outcome.kind === 'invalid_transition') {
			expect(outcome.alert.status).toBe('SUPERSEDED');
			expect(outcome.alert.superseded_by).toBe(composite.alert_id);
		}
	});

	it('an operator ACK that wins the lock race still lets a concurrent COMPOSITE supersede it afterward', async () => {
		const signalLoss = buildAlert();
		const composite = buildComposite([signalLoss.alert_id]);
		await persistIndividualAlert(signalLoss);
		seededIds.push(signalLoss.alert_id, composite.alert_id);

		// The PATCH-triggered transition and the Kafka-triggered supersession
		// both lock this exact alert_id; running them concurrently proves they
		// serialize instead of racing, whichever order Postgres grants the
		// advisory lock in.
		await Promise.all([
			transitionAlert(signalLoss.alert_id, 'ACKNOWLEDGED', ACTOR_USER_ID),
			persistCompositeAlert(composite),
		]);

		const { rows } = await pool.query(
			'SELECT status, superseded_by, acknowledged_by FROM alerts WHERE alert_id = $1',
			[signalLoss.alert_id],
		);
		// Regardless of arrival order, the alert converges to SUPERSEDED --
		// ACKNOWLEDGED is in compositeSupersession.ts's own WHERE status IN
		// ('NEW','ACKNOWLEDGED') clause, so an ACK that lands first is still a
		// valid supersession target.
		expect(rows[0].status).toBe('SUPERSEDED');
		expect(rows[0].superseded_by).toBe(composite.alert_id);
	});

	it("two concurrent transitionAlert calls for the same alert_id serialize under the advisory lock, don't run interleaved", async () => {
		const alert = buildAlert();
		await persistIndividualAlert(alert);
		seededIds.push(alert.alert_id);

		const clientA = await pool.connect();
		const clientB = await pool.connect();
		try {
			await clientA.query('BEGIN');
			await clientA.query('SELECT pg_advisory_xact_lock($1, hashtext($2))', [
				ALERT_SUPERSESSION_LOCK_NAMESPACE,
				alert.alert_id,
			]);

			let bResolved = false;
			const bLock = clientB
				.query('SELECT pg_advisory_xact_lock($1, hashtext($2))', [
					ALERT_SUPERSESSION_LOCK_NAMESPACE,
					alert.alert_id,
				])
				.then(() => {
					bResolved = true;
				});

			await new Promise((r) => setTimeout(r, 300));
			expect(bResolved).toBe(false); // still blocked while A holds the lock

			await clientA.query('COMMIT');
			await bLock;
			expect(bResolved).toBe(true);
			await clientB.query('COMMIT');
		} finally {
			clientA.release();
			clientB.release();
		}
	});
});
