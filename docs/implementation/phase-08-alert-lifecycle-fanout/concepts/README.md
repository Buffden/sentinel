# Phase 08 Concepts

Concept notes and debrief records for Phase 08 checkpoints, in the order you'd read them while working through the phase.

| Folder | Observable result |
| --- | --- |
| [alert-lifecycle-ui/](alert-lifecycle-ui/) | The approved mockup (`mockups/alert-lifecycle-controls.svg`) plus the frontend checkpoint's implementation: real Acknowledge/Resolve controls calling the backend's already-proven `PATCH /alerts/:alert_id`, confirmed through an actual rendered browser (headless Chrome driven directly over CDP, no extension available this session) — NEW → Acknowledge → ACKNOWLEDGED badge + Resolve-only button → Resolve → card removed from view, matching the mockup exactly |

**Documentation debt, recorded rather than hidden:** CP1 (lifecycle-write contract design), CP2 (durable `PATCH` transition), CP3 (fan-out on transition), and CP4 (cross-instance convergence + crash-before-publish recovery) are all implemented, tested (109 backend tests passing, including real Postgres advisory-lock races and two real API instances converging over Redis pub/sub), and manually verified against the real running stack — but do not yet have their own concept docs in this directory, the same gap Phase 06 hit and backfilled after the fact (see that phase's own note about not letting this repeat). Backfilling CP1-CP4's concept docs from the real commits (`6e77919` through `19788a3`) is still owed.
