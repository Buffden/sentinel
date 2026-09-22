// Frontend domain model for GET /entities/:entity_id/graph's response
// (Phase 09 CP4). Not the database row and not the wire DTO.

export type GraphEdgeType = 'PROXIMITY_EVENT' | 'KNOWN_ASSOCIATE'

export interface GraphEdge {
	edgeType: GraphEdgeType
	otherEntityId: string
	otherEntityType: string | null
	episodeStartMs: number | null
	lastSeenMs: number | null
	minDistanceMetres: number | null
	establishedAt: string | null
	knownAssociateType: string | null
}
