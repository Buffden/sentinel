import { describe, expect, it } from 'vitest'
import { applyPositionUpdate, type TrackedEntity, type TrackedEntityUpdate } from './model'

function baseUpdate(overrides: Partial<TrackedEntityUpdate> = {}): TrackedEntityUpdate {
	return {
		id: 'entity-1',
		lat: 51.5,
		lon: -0.1,
		eventTimeMs: 1_700_000_000_000,
		...overrides,
	}
}

describe('applyPositionUpdate', () => {
	it('accepts the first sighting of an entity and fills unset optional fields with null', () => {
		const result = applyPositionUpdate(new Map(), baseUpdate({ callsign: 'BAW442' }))
		const entity = result.get('entity-1')
		expect(entity).toBeDefined()
		expect(entity?.callsign).toBe('BAW442')
		expect(entity?.onGround).toBeNull()
		expect(entity?.entitySubtype).toBeNull()
	})

	it('accepts a strictly newer update and overwrites the changed fields', () => {
		const first = applyPositionUpdate(
			new Map(),
			baseUpdate({ eventTimeMs: 1_700_000_000_000, lat: 51.5 }),
		)
		const second = applyPositionUpdate(
			first,
			baseUpdate({ eventTimeMs: 1_700_000_060_000, lat: 52.0 }),
		)
		expect(second.get('entity-1')?.lat).toBe(52.0)
		expect(second.get('entity-1')?.eventTimeMs).toBe(1_700_000_060_000)
	})

	it('discards a stale update (older eventTimeMs) without mutating state', () => {
		const current = applyPositionUpdate(
			new Map(),
			baseUpdate({ eventTimeMs: 1_700_000_060_000, lat: 52.0 }),
		)
		const result = applyPositionUpdate(
			current,
			baseUpdate({ eventTimeMs: 1_700_000_000_000, lat: 0 }),
		)
		// Same reference: the reducer must bail out entirely on a stale frame,
		// not just leave the values unchanged — callers rely on this to skip
		// a re-render (see MapWidget's setEntities(applyPositionUpdate(...))).
		expect(result).toBe(current)
		expect(result.get('entity-1')?.lat).toBe(52.0)
	})

	it('discards an update with an equal eventTimeMs — strictly-newer is required, not newer-or-equal', () => {
		const current = applyPositionUpdate(
			new Map(),
			baseUpdate({ eventTimeMs: 1_700_000_000_000, lat: 51.5 }),
		)
		const result = applyPositionUpdate(
			current,
			baseUpdate({ eventTimeMs: 1_700_000_000_000, lat: 99 }),
		)
		expect(result.get('entity-1')?.lat).toBe(51.5)
	})

	it('merges by key: a field absent from the incoming update preserves the existing value', () => {
		// Mirrors the real bug fixed in CP7g: the WebSocket position-updates
		// frame never carries on_ground/entity_subtype. Without a merge, a
		// REST-hydrated entity's real ground status would be wiped to null
		// the instant its first live position update arrived.
		const hydrated: TrackedEntity = {
			id: 'entity-1',
			lat: 51.5,
			lon: -0.1,
			altitudeM: null,
			speedMps: null,
			courseDeg: null,
			eventTimeMs: 1_700_000_000_000,
			entityType: 'aircraft',
			entitySubtype: null,
			callsign: 'BAW442',
			onGround: true,
		}
		const current = new Map([['entity-1', hydrated]])

		// A WS-shaped update: no entitySubtype or onGround key at all.
		const wsUpdate: TrackedEntityUpdate = {
			id: 'entity-1',
			lat: 51.6,
			lon: -0.2,
			eventTimeMs: 1_700_000_060_000,
			callsign: 'BAW442',
		}
		const result = applyPositionUpdate(current, wsUpdate)
		const entity = result.get('entity-1')
		expect(entity?.lat).toBe(51.6)
		expect(entity?.onGround).toBe(true) // preserved, not wiped to null
		expect(entity?.entityType).toBe('aircraft') // preserved
	})

	it('does not mutate the map passed in — returns a new Map on acceptance', () => {
		const current = new Map<string, TrackedEntity>()
		const result = applyPositionUpdate(current, baseUpdate())
		expect(result).not.toBe(current)
		expect(current.size).toBe(0)
	})
})
