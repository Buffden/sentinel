import { describe, expect, it } from 'vitest'
import { applyAlertUpdate, signalLossDarkSinceMs, supersededEvidenceIds, type Alert } from './model'

function buildAlert(overrides: Partial<Alert> = {}): Alert {
	return {
		id: 'alert-1',
		alertType: 'SIGNAL_LOSS',
		entityId: '4bca1c',
		counterpartyEntityId: null,
		entityType: 'aircraft',
		status: 'NEW',
		priority: 'STANDARD',
		supersededBy: null,
		detectedAtMs: 1_700_000_000_000,
		payload: { dark_since_ms: 1_699_999_000_000 },
		...overrides,
	}
}

describe('applyAlertUpdate', () => {
	it('adds a new alert keyed by id', () => {
		const result = applyAlertUpdate(new Map(), buildAlert())
		expect(result.get('alert-1')).toEqual(buildAlert())
	})

	// This is CP7j's specific exit proof: "Same alert event delivered twice
	// to the WebSocket client ... dashboard shows exactly one logical alert
	// entry for that alert_id." A redelivered Kafka message (e.g. after a
	// crash before offset commit) republishes identical content — receiving
	// it twice must not create a second panel entry.
	it('delivering the same alert_id twice produces exactly one entry, not two', () => {
		const first = applyAlertUpdate(new Map(), buildAlert())
		const second = applyAlertUpdate(first, buildAlert())
		expect(second.size).toBe(1)
		expect(Array.from(second.values())).toHaveLength(1)
	})

	it('a different alert_id is a distinct entry', () => {
		const first = applyAlertUpdate(new Map(), buildAlert({ id: 'alert-1' }))
		const second = applyAlertUpdate(first, buildAlert({ id: 'alert-2' }))
		expect(second.size).toBe(2)
	})

	it('does not mutate the map passed in — returns a new Map', () => {
		const current = new Map<string, Alert>()
		const result = applyAlertUpdate(current, buildAlert())
		expect(result).not.toBe(current)
		expect(current.size).toBe(0)
	})

	// DATA_MODEL.md's Pre-CP5B(f): a COMPOSITE and the individual alert it
	// supersedes publish from two separately committed API transactions with
	// no cross-message ordering guarantee, so a stale NEW frame for an
	// already-superseded alert can arrive after the SUPERSEDED one.
	it('does not let a stale NEW frame regress an alert already rendered as SUPERSEDED', () => {
		const superseded = applyAlertUpdate(
			new Map(),
			buildAlert({ status: 'SUPERSEDED', supersededBy: 'composite-1' }),
		)
		const result = applyAlertUpdate(superseded, buildAlert({ status: 'NEW', supersededBy: null }))
		expect(result.get('alert-1')?.status).toBe('SUPERSEDED')
	})

	it('does not let a stale NEW frame regress an alert already rendered as RESOLVED', () => {
		const resolved = applyAlertUpdate(new Map(), buildAlert({ status: 'RESOLVED' }))
		const result = applyAlertUpdate(resolved, buildAlert({ status: 'NEW' }))
		expect(result.get('alert-1')?.status).toBe('RESOLVED')
	})

	it('still applies a redelivered duplicate of the same terminal status', () => {
		const superseded = applyAlertUpdate(
			new Map(),
			buildAlert({ status: 'SUPERSEDED', supersededBy: 'composite-1' }),
		)
		const result = applyAlertUpdate(
			superseded,
			buildAlert({ status: 'SUPERSEDED', supersededBy: 'composite-1' }),
		)
		expect(result.get('alert-1')?.status).toBe('SUPERSEDED')
	})

	it('applies a forward transition normally (NEW to SUPERSEDED)', () => {
		const current = applyAlertUpdate(new Map(), buildAlert({ status: 'NEW' }))
		const result = applyAlertUpdate(
			current,
			buildAlert({ status: 'SUPERSEDED', supersededBy: 'composite-1' }),
		)
		expect(result.get('alert-1')?.status).toBe('SUPERSEDED')
	})
})

describe('supersededEvidenceIds', () => {
	it('collects supersedes_alert_ids from every COMPOSITE alert in view', () => {
		const composite = buildAlert({
			id: 'composite-1',
			alertType: 'COMPOSITE',
			payload: { supersedes_alert_ids: ['alert-1', 'alert-2'] },
		})
		expect(supersededEvidenceIds([composite])).toEqual(new Set(['alert-1', 'alert-2']))
	})

	it('ignores non-COMPOSITE alerts', () => {
		expect(supersededEvidenceIds([buildAlert()])).toEqual(new Set())
	})

	it('ignores a malformed supersedes_alert_ids rather than throwing', () => {
		const composite = buildAlert({
			id: 'composite-1',
			alertType: 'COMPOSITE',
			payload: { supersedes_alert_ids: 'not-an-array' },
		})
		expect(supersededEvidenceIds([composite])).toEqual(new Set())
	})
})

describe('signalLossDarkSinceMs', () => {
	it('reads dark_since_ms from the payload', () => {
		const alert = buildAlert({ payload: { dark_since_ms: 1_699_999_000_000 } })
		expect(signalLossDarkSinceMs(alert)).toBe(1_699_999_000_000)
	})

	it('returns null when the payload has no dark_since_ms', () => {
		const alert = buildAlert({ payload: {} })
		expect(signalLossDarkSinceMs(alert)).toBeNull()
	})

	it('returns null rather than a wrong-typed value when dark_since_ms is not a number', () => {
		const alert = buildAlert({ payload: { dark_since_ms: 'not-a-number' } })
		expect(signalLossDarkSinceMs(alert)).toBeNull()
	})
})
