# Phase 05 Concepts

Concept notes and debrief records, in the order you'd read them while working through this phase.

| Folder | Observable result |
| --- | --- |
| [h3-candidate-lookup/](h3-candidate-lookup/) | `findProximityCandidates` correctly finds a same-cell candidate, correctly finds a boundary-crossing candidate only once k reaches the ring it's actually in, and correctly excludes stale and self members, verified against real Redis |
| [exact-distance-filtering/](exact-distance-filtering/) | `filterByDistance` keeps a real close candidate, excludes a real far one, skips candidates with no live position instead of throwing, and sorts by distance, verified against real Redis |
