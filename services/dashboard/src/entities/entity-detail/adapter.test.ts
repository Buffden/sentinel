import { describe, expect, it } from 'vitest'
import { wireToEntityDetail, isValidWireEntityDetailDto, type WireEntityDetailDto } from './adapter'
import type { WireEntityDto } from '@/entities/tracked-entity/adapter'
import type { WireAlertDto } from '@/entities/alert/adapter'

function buildWireEntity(overrides: Partial<WireEntityDto> = {}): WireEntityDto {
	return {
		entity_id: '427c0b',
		lat: 45.0,
		lon: 2.0,
		altitude_m: 11_582,
		speed_mps: 250,
		course_deg: 90,
		last_seen_ms: 1_700_000_000_000,
		entity_type: 'aircraft',
		entity_subtype: null,
		callsign: 'EZY92XM',
		on_ground: false,
		...overrides,
	}
}

function buildWireAlert(overrides: Partial<WireAlertDto> = {}): WireAlertDto {
	return {
		alert_id: 'alert-1',
		entity_id: '427c0b',
		counterparty_entity_id: null,
		entity_type: 'aircraft',
		alert_type: 'SIGNAL_LOSS',
		priority: 'STANDARD',
		status: 'NEW',
		superseded_by: null,
		payload: {},
		detected_at: '2026-01-01T00:00:00.000Z',
		updated_at: '2026-01-01T00:00:00.000Z',
		acknowledged_at: null,
		resolved_at: null,
		...overrides,
	}
}

describe('wireToEntityDetail', () => {
	it('adapts a live entity plus its alerts', () => {
		const dto: WireEntityDetailDto = { entity: buildWireEntity(), alerts: [buildWireAlert()] }
		const result = wireToEntityDetail('427c0b', dto)

		expect(result.entityId).toBe('427c0b')
		expect(result.entity?.callsign).toBe('EZY92XM')
		expect(result.alerts).toHaveLength(1)
		expect(result.alerts[0]?.id).toBe('alert-1')
	})

	// Mirrors the real API contract (docs/DATA_MODEL.md's GET
	// /entities/:entity_id): entity: null is a dark entity, not an error --
	// the adapter must preserve that distinction, not coerce it away.
	it('preserves entity: null for a dark entity with alert history', () => {
		const dto: WireEntityDetailDto = { entity: null, alerts: [buildWireAlert()] }
		const result = wireToEntityDetail('427c0b', dto)

		expect(result.entity).toBeNull()
		expect(result.alerts).toHaveLength(1)
	})

	it('produces an empty alerts array when none are present', () => {
		const dto: WireEntityDetailDto = { entity: buildWireEntity(), alerts: [] }
		const result = wireToEntityDetail('427c0b', dto)

		expect(result.alerts).toEqual([])
	})

	it('drops a malformed alert entry rather than throwing', () => {
		const dto = {
			entity: buildWireEntity(),
			alerts: [buildWireAlert(), { alert_id: 'bad' }],
		} as unknown as WireEntityDetailDto
		const result = wireToEntityDetail('427c0b', dto)

		expect(result.alerts).toHaveLength(1)
	})
})

describe('isValidWireEntityDetailDto', () => {
	it('accepts a live-entity envelope', () => {
		expect(isValidWireEntityDetailDto({ entity: buildWireEntity(), alerts: [] })).toBe(true)
	})

	it('accepts a dark-entity envelope (entity: null)', () => {
		expect(isValidWireEntityDetailDto({ entity: null, alerts: [] })).toBe(true)
	})

	it('rejects a missing alerts array', () => {
		expect(isValidWireEntityDetailDto({ entity: null })).toBe(false)
	})

	it('rejects a non-object value', () => {
		expect(isValidWireEntityDetailDto(null)).toBe(false)
		expect(isValidWireEntityDetailDto('nope')).toBe(false)
	})
})
