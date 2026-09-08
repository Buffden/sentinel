# Phase 06 Concepts

Concept notes and debrief records for Phase 06 checkpoints, in the order you'd read them while working through the phase.

| Folder | Observable result |
| --- | --- |
| [recent-loss-handoff/](recent-loss-handoff/) | Real resume for a previously-dark entity: `recent-loss:{entity_id}` appears with a bounded `PTTL`, `alert-state:{entity_id}` is gone, and the key actually disappears once the window elapses |
| [leader-scoped-candidate-consumption/](leader-scoped-candidate-consumption/) | Two real evaluator instances: only the leader ever appears in `rpk group describe alert-evaluator`; killing the leader hands the group to the follower with no observed overlap |
| [correlation-window-semantics/](correlation-window-semantics/) | `DATA_MODEL.md`'s composite eligibility formula and tie-break, resolved and documented before any code depended on it |
| [composite-eligibility-resolution/](composite-eligibility-resolution/) | `resolveCompositeEligibility` returns the correct winner (or `null`) against real seeded Redis state, and leaves that state byte-for-byte unchanged |
| [composite-claim-protocol/](composite-claim-protocol/) | Design-only: the crash-safe claim/decision protocol traced by hand against every redelivery scenario, resolved and documented in `DATA_MODEL.md` before any code exists against it |
| [atomic-signal-loss-handoff/](atomic-signal-loss-handoff/) | A claim seeded on `alert-state` survives the `recent-loss` handoff byte-for-byte, via a single Lua script replacing the CP1-era read-then-`MULTI`-write |
| [composite-episode-claim/](composite-episode-claim/) | Two concurrent different-candidate claims against real Redis: exactly one wins; the winner's own retry still succeeds; the loser cannot finalize |

Checkpoints not yet in this list (CP3C onward) are still unimplemented — see the phase [`README.md`](../README.md) checkpoint table.
