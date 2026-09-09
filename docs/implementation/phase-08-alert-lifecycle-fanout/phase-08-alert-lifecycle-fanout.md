# Phase 08 — Alert Lifecycle + Distributed Fan-Out

## Goal

Give the operator real control over an alert's lifecycle, and make that control converge correctly across every connected client and every API instance.

## Scope correction (verified against the current code before writing this)

Two things this phase's plan previously claimed are not actually still open:

- **Atomic `COMPOSITE` insert + referenced-alert `SUPERSEDED` update** moved to Phase 06 (CP5B). Without it, Phase 06 wouldn't deliver its own goal — see that phase's plan for why it had to move rather than stay here.
- **The basic `alert-events` → API instances → WebSocket fan-out mechanism already exists.** Confirmed directly: `alertSink.ts`'s `persistAlert` already does idempotent INSERT *and* `redis.publish(ALERT_EVENTS_CHANNEL, raw)` for every alert generically, and `wsServer.ts` already subscribes to that channel and fans out to local clients — built in Phase 03 to serve the first `SIGNAL_LOSS` alert end-to-end. This phase does not need to build that mechanism from scratch.

What's genuinely still missing: nothing writes to `alerts` after the initial insert (no `PATCH`, no lifecycle UPDATE), and nothing publishes a fan-out event *on* a lifecycle change — only on first arrival. That's this phase's real remaining scope.

## Backend

- `PATCH /alerts/:alert_id` for acknowledge/resolve
- durable `NEW → ACKNOWLEDGED → RESOLVED` transitions with audit fields (`acknowledged_at`/`acknowledged_by`, `resolved_at`/`resolved_by` — columns already exist from the original migration, unused until now)
- publish a lifecycle-change event to `alert-events` on each transition (the existing publish-on-insert path doesn't cover this — it fires once, at Kafka-consumption time)
- client dedup semantics for lifecycle events (by `alert_id` + status, since WebSocket delivery is at-least-once)
- multi-instance convergence specifically for lifecycle-change events — the initial-delivery fan-out is already proven from Phase 03; ACK/RESOLVE propagating identically to all connected clients across API instances is new and needs its own proof

## Frontend

A dedicated lifecycle-UI checkpoint, after the backend checkpoints above. Follows the mockup → approval → implementation gate.

- ACK / Resolve controls on an alert card
- `NEW` / `ACKNOWLEDGED` / `RESOLVED` / `SUPERSEDED` status presented distinctly (semantic status colors per `CLAUDE.md`'s Workspace Visual Language — do not invent new ad-hoc colors)
- this is also where Phase 06 CP5C's `COMPOSITE`/`SUPERSEDED` presentation gets its real ACK/Resolve controls wired in, if CP5C left them out (it should have — see Phase 06's plan)

## Vertical-Slice Exit

Operator acknowledges or resolves an alert → the durable state change persists → every connected client, across every API instance, converges on the same visible status. Backend-only completion (transitions persist correctly, fan-out proven via two API instances + direct WebSocket inspection, no UI) is a checkpoint milestone inside this phase, not the phase's own exit criterion.

## Key Experiment

Run two API instances with one WebSocket client on each. Acknowledge an alert through one instance's `PATCH` and verify both clients converge on `ACKNOWLEDGED`, even though only one API instance's DB write actually happened. Then crash after DB persistence but before the lifecycle-event publish and verify no duplicate durable transition while a duplicate/missed push is handled by client dedup.

## Delivery Semantics

Kafka-to-database processing (the original alert insert) creates **idempotent durable effects**, but WebSocket delivery — both initial and lifecycle-change — is **at least once**, not exactly once. The system must never claim replay guarantees zero duplicate WebSocket deliveries.

## Exit Criteria

Status transitions persist correctly and idempotently, lifecycle-change events fan out to all API instances and converge across all connected clients, and an operator can perform ACK/Resolve through the dashboard and see the result reflected everywhere, not just in their own session.

Not fully broken down into checkpoints yet — refine the backend/frontend checkpoint sequence when this phase actually starts.
