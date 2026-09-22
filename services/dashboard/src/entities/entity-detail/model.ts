// Frontend domain model for GET /entities/:entity_id's response envelope.
// Not the database row and not the wire DTO. Reuses the existing
// TrackedEntity and Alert domain models rather than redefining their shapes —
// this endpoint's `entity` field is byte-identical to GET /entities/live's
// per-entity shape, and its `alerts` field is byte-identical to one element
// of GET /alerts' array.

import type { TrackedEntity } from '@/entities/tracked-entity/model'
import type { Alert } from '@/entities/alert/model'

export interface EntityDetail {
	entityId: string
	// null means Redis has no live hash for this id -- a dark entity, not an
	// error. See docs/DATA_MODEL.md's GET /entities/:entity_id contract.
	entity: TrackedEntity | null
	alerts: Alert[]
}
