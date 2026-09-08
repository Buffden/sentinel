// Canonical pair identity: whichever order two entities are compared in
// (A-triggered-by-B or B-triggered-by-A), they must resolve to the same key,
// so episode state, Neo4j evidence, and the eventual alert are keyed once per
// pair, not twice. Ordering is plain string comparison on entity_id, not
// numeric -- entity IDs are opaque identifiers (ICAO24 hex, MMSI, synthetic),
// never parsed as numbers elsewhere in this codebase.
export function canonicalPairKey(entityIdA: string, entityIdB: string): string {
	const [min, max] = entityIdA <= entityIdB ? [entityIdA, entityIdB] : [entityIdB, entityIdA];
	return `${min}:${max}`;
}
