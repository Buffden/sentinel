import { describe, expect, it } from 'vitest'
import {
	wireToGraphEdge,
	isValidWireGraphEdgeDto,
	isValidWireEntityGraphDto,
	type WireGraphEdgeDto,
} from './adapter'

function buildWireProximityEdge(overrides: Partial<WireGraphEdgeDto> = {}): WireGraphEdgeDto {
	return {
		edge_type: 'PROXIMITY_EVENT',
		other_entity_id: 'a5a30a',
		other_entity_type: 'aircraft',
		episode_start_ms: 1_700_000_000_000,
		last_seen_ms: 1_700_000_010_000,
		min_distance_metres: 250.5,
		established_at: null,
		known_associate_type: null,
		...overrides,
	}
}

describe('wireToGraphEdge', () => {
	it('adapts a PROXIMITY_EVENT edge', () => {
		const result = wireToGraphEdge(buildWireProximityEdge())
		expect(result?.edgeType).toBe('PROXIMITY_EVENT')
		expect(result?.otherEntityId).toBe('a5a30a')
		expect(result?.minDistanceMetres).toBe(250.5)
		expect(result?.knownAssociateType).toBeNull()
	})

	it('adapts a KNOWN_ASSOCIATE edge', () => {
		const result = wireToGraphEdge(
			buildWireProximityEdge({
				edge_type: 'KNOWN_ASSOCIATE',
				episode_start_ms: null,
				last_seen_ms: null,
				min_distance_metres: null,
				established_at: '2026-01-01T00:00:00.000Z',
				known_associate_type: 'same_fleet',
			}),
		)
		expect(result?.edgeType).toBe('KNOWN_ASSOCIATE')
		expect(result?.knownAssociateType).toBe('same_fleet')
		expect(result?.episodeStartMs).toBeNull()
	})

	// The API's edge_type is a plain string on the wire; a value outside the
	// two real Neo4j relationship types would mean a contract drift the
	// frontend should never silently render as if it were valid.
	it('returns null for an unrecognized edge_type rather than fabricating one', () => {
		const result = wireToGraphEdge(buildWireProximityEdge({ edge_type: 'SOMETHING_ELSE' }))
		expect(result).toBeNull()
	})
})

describe('isValidWireGraphEdgeDto', () => {
	it('accepts a well-formed edge', () => {
		expect(isValidWireGraphEdgeDto(buildWireProximityEdge())).toBe(true)
	})

	it('rejects a non-object value', () => {
		expect(isValidWireGraphEdgeDto(null)).toBe(false)
	})
})

describe('isValidWireEntityGraphDto', () => {
	it('accepts a valid envelope', () => {
		expect(isValidWireEntityGraphDto({ entity_id: '427c0b', edges: [] })).toBe(true)
	})

	it('rejects a missing edges array', () => {
		expect(isValidWireEntityGraphDto({ entity_id: '427c0b' })).toBe(false)
	})
})
