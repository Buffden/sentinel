import { describe, expect, it } from 'vitest'
import { applyAlertUpdate, signalLossDarkSinceMs, type Alert } from './model'

function buildAlert(overrides: Partial<Alert> = {}): Alert {
	return {
		id: 'alert-1',
		alertType: 'SIGNAL_LOSS',
		entityId: '4bca1c',
		entityType: 'aircraft',
		status: 'NEW',
		priority: 'STANDARD',
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
