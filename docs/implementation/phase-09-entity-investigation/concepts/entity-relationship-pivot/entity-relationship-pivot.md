# Entity Relationship Pivot — Design and Learning Reference

Plain language first, then technical depth, then the code. Use this to understand, inspect, and defend FE-CP3 (the Relationships tab of the Entity Detail widget).

---

## What this checkpoint is, and deliberately isn't

FE-CP3 wires up the Entity Detail widget's third and last tab: a 1-hop relationship graph backed by `GET /entities/:entity_id/graph` (Phase 09 CP4), with the neighbor entity as the primary node in the center and its edges arranged around it. Clicking a neighbor opens *that entity's own* Entity Detail widget instance — US-14's "graph pivot."

It does not implement multi-hop traversal, exactly as CP4's own backend never offered it: a pivot is the operator re-entering this same widget with a different `entity_id`, not the server computing several hops in one call. This completes Phase 09's frontend against the approved mockup; all three tabs are now real.

---

## Concepts in plain language

### The third real use of `WorkspacePanelContext`, and why nothing new was needed

FE-CP1 built the panel-opening context to serve `AlertWidget`'s click-through; FE-CP2 extended it with an optional `anchorMs` for the History tab's window default. The Relationships tab's graph pivot needed exactly the same call — `openEntityDetail(neighborId, { anchorMs })` — from a different call site (inside the widget itself, not `AlertWidget`). No new context method, no new plumbing: this is the payoff of building that context as a real, reusable primitive in FE-CP1 rather than a one-off prop.

### Why the neighbor click passes the edge's own `lastSeenMs` as the anchor

Pivoting to a neighbor is itself an investigation act — "when did these two last interact, and what was going on around that time." Anchoring the neighbor's own History tab default window to the edge's `last_seen_ms` (rather than "now," which would show unrelated recent activity) keeps the pivot coherent with why the operator clicked that node in the first place.

### Why the graph layout caps at 12 neighbors, with an explicit count of what's hidden

The backend's own cap (`ENTITY_GRAPH_MAX_EDGES`, 200) is correct for a data cap but wrong for a circular layout — a real entity in this dev environment already has 23 edges, and 200 nodes in a circle would be unreadable regardless. Rather than render everything (broken) or silently truncate (misleading), the layout shows the first 12 (already ordered by recency from the backend) and an explicit "+N more relationships not shown" caption. Confirmed against real data: `a12f72`'s real graph rendered exactly 12 nodes plus "+11 more."

### Why `RelationshipsTab`'s fetch is a separate component keyed by `entityId`, from the start

FE-CP2 discovered the `react-hooks/set-state-in-effect` and `react-hooks/purity` lint issues the hard way, mid-implementation. FE-CP3 applies the same keyed-remount pattern (`RelationshipsGraphLoader`, mounted fresh per `entityId`) from the first draft, since `entityId` is the only thing that changes this tab's fetch and a fresh mount's own initial state already covers "show loading." Zero lint issues this time, unlike FE-CP2 — evidence the earlier lesson actually transferred, not just got fixed once.

---

## Map to code

| Concept | Where it lives |
| --- | --- |
| Domain model + adapter + API client for graph edges | `services/dashboard/src/entities/entity-graph/{model,adapter,api}.ts` |
| Unrecognized `edge_type` handling (returns `null`, filtered out, never fabricated) | `services/dashboard/src/entities/entity-graph/adapter.ts` (`wireToGraphEdge`) |
| Circular layout, 12-node cap, solid/dashed edge styling, native `<title>` tooltips, graph pivot | `services/dashboard/src/widgets/entity-detail-widget/RelationshipsTab.tsx` |
| All three tabs enabled | `services/dashboard/src/widgets/entity-detail-widget/EntityDetailWidget.tsx` (`ENABLED_TABS`) |
| Proof against real wire shapes | `services/dashboard/src/entities/entity-graph/adapter.test.ts` |

---

## Retention questions

1. Why did the graph-pivot click-handler need zero changes to `WorkspacePanelContext` itself?
2. Why does a neighbor pivot pass the edge's `lastSeenMs` as the anchor instead of leaving it unset?
3. Walk through why 12 was chosen as the rendered-neighbor cap, and what real data confirmed the truncation message works.
4. Why does `wireToGraphEdge` return `null` for an unrecognized `edge_type` instead of passing the raw string through?
5. What lesson from FE-CP2 did this checkpoint apply from the start, and how do you know it actually worked (not just that it wasn't tested)?

---

## Completion checklist

- [ ] I can trace a full pivot chain: alert click → Overview → Relationships tab → neighbor click → a second, independent panel
- [ ] I can explain why the pivot doesn't just re-target the same panel's `entityId`
- [ ] I can explain the 12-neighbor cap and point to the real dev-data entity that proves it renders correctly
- [ ] I can explain why an unrecognized edge type is dropped rather than rendered with a guessed style
- [ ] I have opened the real dashboard myself, opened an entity with real Neo4j relationships, and pivoted to a neighbor to confirm a second panel opens with its own correct state
