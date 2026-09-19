# Phase 08 Concepts

Concept notes and debrief records for Phase 08 checkpoints, in the order you'd read them while working through the phase.

| Folder | Observable result |
| --- | --- |
| [alert-lifecycle-write-path/](alert-lifecycle-write-path/) | Pre-CP1 design (advisory-lock reuse against `compositeSupersession.ts`'s existing writer, the transition matrix, idempotent-replay semantics, the `alert-events` wire-contract documentation correction) plus CP2/CP3's implementation: a real `PATCH /alerts/:alert_id` proven against real Postgres — durable transitions, lock-serialized against a concurrent `COMPOSITE`, idempotent on replay, and fanned out to `alert-events` on every transition — confirmed with 107 tests and a live manual run against the real dev server |
| [cross-instance-alert-convergence/](cross-instance-alert-convergence/) | CP4, the phase file's own "Key Experiment": two independently-isolated API instances converging on the same acknowledged alert through nothing but Redis pub/sub, and a simulated crash between the DB commit and the publish recovering cleanly on retry — which caught a real bug along the way (Express 4 never actually forwarded a route handler's thrown error, despite an earlier fix attempt that looked correct but was never exercised), fixed with a shared `asyncHandler` wrapper applied across the whole service |
| [alert-lifecycle-ui/](alert-lifecycle-ui/) | The approved mockup (`mockups/alert-lifecycle-controls.svg`) plus the frontend checkpoint's implementation: real Acknowledge/Resolve controls calling the backend's already-proven `PATCH /alerts/:alert_id`, confirmed through an actual rendered browser (headless Chrome driven directly over CDP, no extension available that session) — NEW → Acknowledge → ACKNOWLEDGED badge + Resolve-only button → Resolve → card removed from view, matching the mockup exactly |

Phase 08's backend and frontend checkpoints are both complete and documented. Next: two-operator/two-instance production-style verification is already covered by CP4's own tests; nothing further is planned for this phase unless a new checkpoint is explicitly opened.
