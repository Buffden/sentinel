import { describe, expect, it } from 'vitest'
import { wireToHistoryPoint, isValidWireHistoryPointDto, type WireHistoryPointDto } from './adapter'

function buildWirePoint(overrides: Partial<WireHistoryPointDto> = {}): WireHistoryPointDto {
	return {
		entity_id: '427c0b',
		timestamp_ms: 1_700_000_000_000,
		lat: 45.0,
		lon: 2.0,
		altitude_m: 11_582,
		speed_mps: 250,
		course_deg: 90,
		...overrides,
	}
}

describe('wireToHistoryPoint', () => {
	it('adapts a full wire point', () => {
		const result = wireToHistoryPoint(buildWirePoint())

		expect(result.timestampMs).toBe(1_700_000_000_000)
		expect(result.lat).toBe(45.0)
		expect(result.lon).toBe(2.0)
		expect(result.altitudeM).toBe(11_582)
		expect(result.speedMps).toBe(250)
		expect(result.courseDeg).toBe(90)
	})

	it('preserves a null altitude rather than coercing it', () => {
		const result = wireToHistoryPoint(buildWirePoint({ altitude_m: null }))
		expect(result.altitudeM).toBeNull()
	})
})

describe('isValidWireHistoryPointDto', () => {
	it('accepts a well-formed point', () => {
		expect(isValidWireHistoryPointDto(buildWirePoint())).toBe(true)
	})

	it('rejects a point missing lat/lon', () => {
		const point = buildWirePoint() as unknown as Record<string, unknown>
		delete point['lat']
		expect(isValidWireHistoryPointDto(point)).toBe(false)
	})

	it('rejects a non-object value', () => {
		expect(isValidWireHistoryPointDto(null)).toBe(false)
		expect(isValidWireHistoryPointDto('nope')).toBe(false)
	})
})
