import type pg from 'pg';
import { withTransaction } from '../db.js';
import type { AlertMessage } from './alertSink.js';

// Pre-CP5B: converges COMPOSITE/referenced-alert persistence regardless of
// which order the two Kafka messages arrive in, without fabricating any
// alert row. See DATA_MODEL.md's "Pre-CP5B: composite supersession
// convergence protocol" for the accepted design this implements.

// Reserves this int4 space for pg_advisory_xact_lock keys; nothing else in
// this codebase takes an advisory lock today.
export const ALERT_SUPERSESSION_LOCK_NAMESPACE = 1001;

export interface AlertRow {
	alert_id: string;
	entity_id: string;
	counterparty_entity_id: string | null;
	entity_type: string;
	alert_type: string;
	priority: string;
	status: string;
	superseded_by: string | null;
	payload: Record<string, unknown>;
	detected_at: Date;
	updated_at: Date;
	acknowledged_at: Date | null;
	acknowledged_by: string | null;
	resolved_at: Date | null;
	resolved_by: string | null;
}

// The canonical shape published to alert-events: derived from the persisted
// row, never from the raw incoming Kafka bytes, so a row whose actual status
// differs from what a message originally carried (a late alert consumed by a
// pending supersession) publishes what it really is.
export interface PublishedAlert {
	alert_id: string;
	entity_id: string;
	counterparty_entity_id: string | null;
	entity_type: string;
	alert_type: string;
	priority: string;
	status: string;
	superseded_by: string | null;
	detected_at_ms: number;
	payload: Record<string, unknown>;
}

// Thrown when a referenced alert (existing row or pending entry) is already
// owned by a different composite_alert_id than the one being processed. Per
// Phase 06's claim protocol, a signal-loss episode can be claimed by at most
// one composite, so this should be structurally unreachable; encountering it
// is an invariant violation, not a routine outcome, the same fail-closed
// posture as CandidateDecisionConflictError/CompositeFinalizeInvariantError
// upstream in the Alert Evaluator.
export class AlertSupersessionInvariantError extends Error {
	constructor(
		public readonly referencedAlertId: string,
		public readonly existingCompositeAlertId: string,
		public readonly requestedCompositeAlertId: string,
	) {
		super(
			`alert ${referencedAlertId} is already superseded by ${existingCompositeAlertId}, cannot also be superseded by ${requestedCompositeAlertId}`,
		);
		this.name = 'AlertSupersessionInvariantError';
	}
}

function rowToPublishedAlert(row: AlertRow): PublishedAlert {
	return {
		alert_id: row.alert_id,
		entity_id: row.entity_id,
		counterparty_entity_id: row.counterparty_entity_id,
		entity_type: row.entity_type,
		alert_type: row.alert_type,
		priority: row.priority,
		status: row.status,
		superseded_by: row.superseded_by,
		detected_at_ms: row.detected_at.getTime(),
		payload: row.payload,
	};
}

async function lockAlertId(client: pg.PoolClient, alertId: string): Promise<void> {
	await client.query('SELECT pg_advisory_xact_lock($1, hashtext($2))', [
		ALERT_SUPERSESSION_LOCK_NAMESPACE,
		alertId,
	]);
}

// Inserts alert idempotently (ON CONFLICT (alert_id) DO NOTHING), optionally
// overriding status/superseded_by from what the message itself carries (used
// when a pending supersession decides this alert lands as SUPERSEDED
// directly). On redelivery, RETURNING gives nothing back since the insert
// was a no-op, so the row's actual current state is read back explicitly
// rather than assumed to match what this call would have inserted, real
// data can differ from a stale in-memory guess if another attempt already
// wrote it.
async function insertAlertRow(
	client: pg.PoolClient,
	alert: AlertMessage,
	overrides: { status?: string; supersededBy?: string | null } = {},
): Promise<AlertRow> {
	const status = overrides.status ?? alert.status;
	const supersededBy = overrides.supersededBy ?? null;

	const inserted = await client.query<AlertRow>(
		`INSERT INTO alerts
			 (alert_id, entity_id, counterparty_entity_id, entity_type, alert_type, priority, status, superseded_by, payload, detected_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $10)
		 ON CONFLICT (alert_id) DO NOTHING
		 RETURNING *`,
		[
			alert.alert_id,
			alert.entity_id,
			alert.counterparty_entity_id ?? null,
			alert.entity_type,
			alert.alert_type,
			alert.priority,
			status,
			supersededBy,
			JSON.stringify(alert.payload),
			new Date(alert.detected_at_ms),
		],
	);
	if (inserted.rows[0]) return inserted.rows[0];

	const existing = await client.query<AlertRow>('SELECT * FROM alerts WHERE alert_id = $1', [
		alert.alert_id,
	]);
	const row = existing.rows[0];
	if (!row)
		throw new Error(`alert ${alert.alert_id}: insert conflicted but no row found on read-back`);
	return row;
}

// Conflict-aware pending-supersession upsert: idempotent only when the
// existing composite_alert_id matches; a different one is an invariant
// failure, mirrored below at the row level once the referenced alert
// actually exists (see the SUPERSEDED branch in persistCompositeAlert).
async function upsertPendingSupersession(
	client: pg.PoolClient,
	referencedAlertId: string,
	compositeAlertId: string,
): Promise<void> {
	const result = await client.query<{ referenced_alert_id: string }>(
		`INSERT INTO pending_alert_supersessions (referenced_alert_id, composite_alert_id, created_at)
		 VALUES ($1, $2, now())
		 ON CONFLICT (referenced_alert_id) DO UPDATE
		   SET composite_alert_id = EXCLUDED.composite_alert_id
		   WHERE pending_alert_supersessions.composite_alert_id = EXCLUDED.composite_alert_id
		 RETURNING referenced_alert_id`,
		[referencedAlertId, compositeAlertId],
	);
	if (result.rows[0]) return;

	const existing = await client.query<{ composite_alert_id: string }>(
		'SELECT composite_alert_id FROM pending_alert_supersessions WHERE referenced_alert_id = $1',
		[referencedAlertId],
	);
	throw new AlertSupersessionInvariantError(
		referencedAlertId,
		existing.rows[0]?.composite_alert_id ?? 'unknown',
		compositeAlertId,
	);
}

function extractSupersedesAlertIds(payload: Record<string, unknown>): string[] {
	const raw = payload['supersedes_alert_ids'];
	if (!Array.isArray(raw)) return [];
	return raw.filter((id): id is string => typeof id === 'string');
}

// Persists a COMPOSITE alert and converges every alert it references,
// regardless of whether each referenced alert already exists. Returns the
// canonical current state of the composite plus every referenced alert this
// composite actually owns as SUPERSEDED, always, even ones this call didn't
// itself mutate, so a caller that always republishes what's returned here
// never depends on this attempt having done the original work.
export async function persistCompositeAlert(alert: AlertMessage): Promise<PublishedAlert[]> {
	const sortedRefIds = [...extractSupersedesAlertIds(alert.payload)].sort();

	return withTransaction(async (client) => {
		for (const refId of sortedRefIds) {
			await lockAlertId(client, refId);
		}

		const compositeRow = await insertAlertRow(client, alert);
		const published: PublishedAlert[] = [rowToPublishedAlert(compositeRow)];

		for (const refId of sortedRefIds) {
			const updated = await client.query<AlertRow>(
				`UPDATE alerts SET status='SUPERSEDED', superseded_by=$2, updated_at=now()
				 WHERE alert_id=$1 AND status IN ('NEW','ACKNOWLEDGED')
				 RETURNING *`,
				[refId, alert.alert_id],
			);
			if (updated.rows[0]) {
				published.push(rowToPublishedAlert(updated.rows[0]));
				continue;
			}

			const existing = await client.query<AlertRow>('SELECT * FROM alerts WHERE alert_id = $1', [
				refId,
			]);
			const row = existing.rows[0];
			if (row) {
				if (row.status !== 'SUPERSEDED') {
					// RESOLVED (or some other terminal state): stays untouched,
					// never retroactively superseded, nothing new to publish.
					continue;
				}
				if (row.superseded_by === alert.alert_id) {
					// Idempotent replay: an earlier attempt (this delivery or a
					// prior one) already superseded it under this exact composite.
					published.push(rowToPublishedAlert(row));
					continue;
				}
				throw new AlertSupersessionInvariantError(
					refId,
					row.superseded_by ?? 'unknown',
					alert.alert_id,
				);
			}

			// Referenced alert doesn't exist yet: out-of-order arrival.
			await upsertPendingSupersession(client, refId, alert.alert_id);
		}

		return published;
	});
}

// Persists any non-COMPOSITE alert. Under this alert_id's own lock, consumes
// a pending supersession if one is waiting, landing directly as SUPERSEDED
// with superseded_by set, never transiently NEW. All canonical fields
// (detected_at, payload, entity_type, ...) come from the real incoming
// message, nothing is ever fabricated; only status/superseded_by are
// overridden by the pending decision.
export async function persistIndividualAlert(alert: AlertMessage): Promise<PublishedAlert> {
	return withTransaction(async (client) => {
		await lockAlertId(client, alert.alert_id);

		const pending = await client.query<{ composite_alert_id: string }>(
			'DELETE FROM pending_alert_supersessions WHERE referenced_alert_id = $1 RETURNING composite_alert_id',
			[alert.alert_id],
		);

		const row = pending.rows[0]
			? await insertAlertRow(client, alert, {
					status: 'SUPERSEDED',
					supersededBy: pending.rows[0].composite_alert_id,
				})
			: await insertAlertRow(client, alert);

		return rowToPublishedAlert(row);
	});
}
