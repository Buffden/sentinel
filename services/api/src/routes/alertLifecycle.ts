import { withTransaction } from '../db.js';
import {
	lockAlertId,
	rowToPublishedAlert,
	type AlertRow,
	type PublishedAlert,
} from '../sink/compositeSupersession.js';

export type LifecycleTargetStatus = 'ACKNOWLEDGED' | 'RESOLVED';

export type TransitionOutcome =
	| { kind: 'applied'; alert: PublishedAlert }
	| { kind: 'idempotent'; alert: PublishedAlert }
	| { kind: 'not_found' }
	| { kind: 'invalid_transition'; alert: PublishedAlert };

// Legal operator-driven transitions per US-13: NEW -> ACKNOWLEDGED,
// NEW -> RESOLVED, ACKNOWLEDGED -> RESOLVED. SUPERSEDED is system-only
// (composite supersession) and never reachable through this path; RESOLVED
// and SUPERSEDED are both terminal.
function isLegalTransition(current: string, target: LifecycleTargetStatus): boolean {
	if (current === 'NEW') return true;
	if (current === 'ACKNOWLEDGED') return target === 'RESOLVED';
	return false;
}

// Locks this alert_id under the same advisory-lock namespace
// compositeSupersession.ts's writers use, so an operator PATCH and a
// concurrent COMPOSITE supersession for the same alert_id always serialize
// rather than racing: whichever transaction locks first commits first, and
// the other always observes that committed result before deciding its own
// outcome.
//
// Idempotent by design: replaying the same target status (client retry,
// double-click) is a no-op that still re-publishes the canonical row, so a
// lost publish from an earlier attempt is recoverable by simply retrying
// the PATCH. An illegal transition (terminal current status, or a status
// change a composite already made) never throws -- it returns the alert's
// real current state so the caller can reconcile instead of guessing.
export async function transitionAlert(
	alertId: string,
	target: LifecycleTargetStatus,
	actorUserId: string,
): Promise<TransitionOutcome> {
	return withTransaction(async (client) => {
		await lockAlertId(client, alertId);

		const existing = await client.query<AlertRow>('SELECT * FROM alerts WHERE alert_id = $1', [
			alertId,
		]);
		const row = existing.rows[0];
		if (!row) return { kind: 'not_found' };

		if (row.status === target) {
			return { kind: 'idempotent', alert: rowToPublishedAlert(row) };
		}

		if (!isLegalTransition(row.status, target)) {
			return { kind: 'invalid_transition', alert: rowToPublishedAlert(row) };
		}

		const settingAcknowledged = target === 'ACKNOWLEDGED';
		const settingResolved = target === 'RESOLVED';
		const updated = await client.query<AlertRow>(
			`UPDATE alerts
			 SET status = $2,
			     updated_at = now(),
			     acknowledged_at = CASE WHEN $3 THEN now() ELSE acknowledged_at END,
			     acknowledged_by = CASE WHEN $3 THEN $5 ELSE acknowledged_by END,
			     resolved_at = CASE WHEN $4 THEN now() ELSE resolved_at END,
			     resolved_by = CASE WHEN $4 THEN $5 ELSE resolved_by END
			 WHERE alert_id = $1
			 RETURNING *`,
			[alertId, target, settingAcknowledged, settingResolved, actorUserId],
		);
		const newRow = updated.rows[0];
		if (!newRow) throw new Error(`alert ${alertId}: transition update returned no row`);
		return { kind: 'applied', alert: rowToPublishedAlert(newRow) };
	});
}
