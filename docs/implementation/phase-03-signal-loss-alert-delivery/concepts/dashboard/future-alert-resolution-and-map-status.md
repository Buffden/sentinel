# Deferred: alert auto-resolution + dark-entity map status

Not built in Phase 03. Captured here because the design work (and the trade-offs it surfaces)
came up while reviewing Phase 03's alert widget, but the two features below don't belong to
this phase — they belong to whichever phase actually owns alert lifecycle state, and to a
dashboard follow-up after that. This doc exists so the design isn't lost between now and then.

Two related but separable asks:

1. Alerts should stop appearing in the panel once the flight that triggered them resumes.
2. A dark (open-alert) aircraft should look visibly different on the map — and briefly flash a
   distinct color when it recovers.

---

## Why neither shipped in Phase 03

**(1) is a real lifecycle-state change, not a UI tweak.** Today `alerts.status` is written once
(`NEW`) at insert and never changes — nothing in the codebase transitions it. Phase 03's own
scope note explicitly excludes "acknowledge/resolve lifecycle." The `alerts` table already has
`resolved_at`/`resolved_by` columns, but `resolved_by` is a FK to a user — the schema models
resolution as an **operator action**. What's being asked for here is different: an *automatic*
transition when Position Consumer detects the entity is alive again. That's not the same
feature as Phase 08's planned operator resolve, even though both land on the same `RESOLVED`
status — see "Where this belongs" below.

**(2) is buildable independently** (no backend change strictly required — the dashboard already
fetches both open alerts and live positions, so a marker's "is this entity dark" state can be
derived client-side by cross-referencing the two), but the "flash green on recovery" half reads
much better if it's triggered by a real resolve event rather than a client-side heuristic
guessing that a fresh position update means "recovered." So it's listed here as a pair, not
because they're technically coupled, but because building (2) well benefits from (1) existing
first.

---

## Design: system auto-resolve (belongs to Phase 08)

**Ownership stays put.** Position Consumer already detects resume — it's the exact moment it
currently writes `recent-loss:{entity_id}` and deletes `alert-state:{entity_id}`
(`consumer.ts:629-641`), and it already has `signal_loss_alert_id` in hand from that same hash
read. It must not write to Postgres directly (API owns durable alert lifecycle state per this
project's service ownership rules), so the mechanism is a new Redis pub/sub signal:

1. Position Consumer publishes `{entity_id, alert_id, resumed_at_ms}` on a new channel —
   tentatively `signal-loss-resolved` — right where it already handles the resume.
2. API subscribes (same place it already subscribes to `alert-events`), runs
   `UPDATE alerts SET status='RESOLVED', resolved_at=... WHERE alert_id=$1 AND status='NEW'`
   (idempotent — a duplicate signal is a no-op), and republishes that same alert row — now with
   `status: 'RESOLVED'` — onto the *existing* `alert-events` channel.
3. Nothing new needed client-side: `useLiveFeed.ts`'s `parseAlertFrame` already reads `status`
   off every `alert-events` message and merges by `alert_id` into the `Alert` map. `AlertWidget`
   just needs to stop rendering entries whose `status === 'RESOLVED'`.

**`resolved_by` stays `NULL`.** That's the honest way to keep "system auto-resolved" distinct
from "an operator resolved this" — so Phase 08's actual operator-driven resolve isn't silently
pre-empted by this.

**Known gap, accepted for v1 (matches this project's at-least-once posture everywhere else):**
the publish is fire-and-forget, not transactional with the Redis writes around it. If Position
Consumer crashes right after deleting `alert-state` but before the publish lands, or if API is
down when it publishes, the resolve signal is simply lost — the alert stays `NEW` forever even
though the flight is back. Not a correctness bug, just a known limitation worth being able to
explain.

**Manual inspection, once built:** trigger a signal-loss alert, confirm it appears, then either
let the entity resume naturally or manually re-seed a fresh position via `redis-cli` to simulate
it; watch `psql -c "SELECT alert_id, status, resolved_at FROM alerts WHERE alert_id='...'"` flip
to `RESOLVED`, and watch the dashboard panel — with no refresh — drop that card.

---

## Design: dark-entity map status (dashboard follow-up, no fixed phase)

- Red icon (matches the existing critical/lost semantic color already used for the alert badge)
  for any aircraft with an open SIGNAL_LOSS alert, with a blink/pulse — deck.gl `IconLayer` has
  no built-in animation, so this needs a small time-based uniform (e.g. pulsing opacity/size) in
  `aviationLayer.ts`, driven by a render loop rather than CSS.
- Brief green flash before reverting to normal blue when the entity's alert resolves.
- Needs a small piece of genuinely shared frontend state: "set of entity_ids with an open
  SIGNAL_LOSS alert," derived from the same alert-events stream `AlertWidget` already consumes,
  exposed to `MapWidget` too. Two real consumers as of this design (`AlertWidget`, `MapWidget`),
  so a small shared hook is justified under this project's extensibility rule — not a
  speculative abstraction. Natural home: alongside the existing shared WebSocket singleton
  (`shared-websocket.md`), not a new standalone store.
- Does not strictly require the backend auto-resolve above to exist — the "entity is dark" half
  only needs today's open-alerts list. The "flash on recovery" half can either wait for a real
  `RESOLVED` transition, or use the same lightweight frontend heuristic considered (and shelved
  in favor of the real backend fix) for feature (1): treat a fresh position update for an entity
  with a currently-open alert as "recovered," client-side only, no persistence.

---

## Where this belongs

- **Auto-resolve (backend)**: Phase 08 — `docs/implementation/phase-08-alert-lifecycle-fanout/`.
  Its current plan only covers the operator-driven `PATCH /alerts/:alert_id` path; this is an
  addition to that phase's alert state model (a second way to reach `RESOLVED`), not something
  already covered by it. Phase 08's own doc should get a short addendum for this when that phase
  starts — deliberately not edited here, since this design isn't an accepted scope change yet,
  just a captured proposal.
- **Map visual status**: no existing phase owns "live map marker reflects alert state." Best
  treated as a dashboard enhancement done after Phase 08 lands the real resolve signal (or
  earlier, using the frontend-only heuristic, if that's ever prioritized independently).
