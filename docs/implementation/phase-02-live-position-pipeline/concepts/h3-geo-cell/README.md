# H3 Geo-Cell Indexing

Concept notes and debrief for H3 geo-cell indexing: two resolutions, two access patterns, and the Redis sorted-set boundary-crossing update sequence.

| File | What's inside |
| --- | --- |
| [h3-resolution-design.md](h3-resolution-design.md) | Why two resolutions, what each optimises for, hands-on experiment outputs |
| [geo-cell-sorted-set.md](geo-cell-sorted-set.md) | `geo-cell:{cell}` sorted set design, update sequence, why old cell is read before Lua, the non-atomic gap |
| [h3-geo-cell-debrief.md](h3-geo-cell-debrief.md) | Observed outputs from the first-ping, stale-event, and boundary-crossing experiments |
