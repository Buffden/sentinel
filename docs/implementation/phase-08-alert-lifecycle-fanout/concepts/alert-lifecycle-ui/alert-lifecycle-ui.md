# Alert Lifecycle UI — Design and Learning Reference

Plain language first, then technical depth, then the code. Use this to understand, inspect, and defend the frontend checkpoint of Phase 08.

---

## What this checkpoint is, and deliberately isn't

This checkpoint gives an operator real ACK/Resolve controls on an alert card, built against the mockup approved before any of this code was written (`concepts/alert-lifecycle-ui/mockups/alert-lifecycle-controls.svg`). It calls the backend `PATCH /alerts/:alert_id` endpoint CP2/CP3 already built and proved (durable transition, fan-out, idempotent replay) — it introduces no new backend behavior, only a UI that makes that endpoint reachable by clicking instead of `curl`.

It does not add an "ACKNOWLEDGED BY" field to the detail view, even though the approved mockup sketched one — `acknowledged_by` isn't carried by `PublishedAlert` (the shape both the PATCH response and the `alert-events` WS frame use), so showing it would have meant extending three wire contracts (API response shape, WS frame shape, frontend domain model) for a checkpoint whose actual goal was "give operators lifecycle control," not "surface every audit column." Trimmed deliberately, not an oversight — flagged when the mockup was approved.

---

## Concepts in plain language

### Why the PATCH response is parsed with a different shape than `GET /alerts`

`GET /alerts` returns rows shaped like the Postgres table (`detected_at` as an ISO string, `updated_at`/`acknowledged_at`/`resolved_at` present). `PATCH /alerts/:alert_id` returns `PublishedAlert` — the same flat shape the `alert-events` WS channel carries (`detected_at_ms` as a number, no audit columns). These are genuinely two different wire contracts for the same underlying row, documented in `DATA_MODEL.md`'s CP1 correction. `entities/alert/api.ts`'s `patchedAlertToAlert` mirrors `useLiveFeed.ts`'s existing `parseAlertFrame`, not `entities/alert/adapter.ts`'s `wireToAlert` — using the REST adapter here would have silently produced `NaN` for `detectedAtMs` the first time a PATCH response was parsed with `new Date(dto.detected_at)` against a field that doesn't exist on this shape.

### Why the PATCH response is applied directly instead of waiting for the WebSocket echo

The PATCH the operator's own click triggers also causes the API to publish to `alert-events`, which will arrive back over the same operator's own WebSocket connection a moment later. Waiting for that echo to update the UI would tie the button's loading state to WS delivery timing (best-effort, at-least-once, not instant) instead of the HTTP round-trip that already has the authoritative answer. `handleTransition` applies the PATCH response immediately via the same `applyAlertUpdate` reducer the live feed uses; the WS echo arrives afterward and merges again, harmlessly — `applyAlertUpdate` overwrites by `alert_id` and is safe to apply twice with identical data.

### Why a 409 response is treated as data, not just an error

`PATCH` returns `409` with the alert's real current row when the requested transition is illegal (e.g., a composite superseded it moments before the click landed). `patchAlertStatus` parses `409` the same way as `200` — both carry the same `PublishedAlert` shape — so the operator's UI converges to reality (a SUPERSEDED badge, no buttons) instead of just showing a generic error toast that leaves the stale NEW/ACKNOWLEDGED buttons visible and clickable.

### Why a RESOLVED alert disappears immediately instead of showing a RESOLVED badge

`GET /alerts` has only ever returned `NEW`/`ACKNOWLEDGED` rows (a decision from Phase 07, not something this checkpoint changes). If a resolved card lingered with a badge until the next reload, a page refresh would silently make it vanish — a live-session/reload inconsistency. `topLevel`'s filter drops `status === 'RESOLVED'` immediately, so both paths agree at every moment, not just eventually.

### Why nested (superseded) cards never show real-time status in their badge

`renderAlert(alert, nested=true)` always renders `<StatusBadge status="SUPERSEDED" />`, never `alert.status` — unchanged behavior from before this checkpoint, extended to the new badge component. A card can be structurally nested (its `alert_id` appears in its COMPOSITE parent's `payload.supersedes_alert_ids`) before its own `SUPERSEDED` WS event has arrived, per the existing `supersededEvidenceIds` comment. Showing the real (possibly still `NEW`) status in that gap would render a confusing "NEW" badge under a composite that has already structurally superseded it.

---

## Map to code

| Concept | Where it lives |
| --- | --- |
| Approved mockup | `concepts/alert-lifecycle-ui/mockups/alert-lifecycle-controls.svg` |
| `patchAlertStatus`, `PatchedAlertDto`, `AlertTransitionError` | `services/dashboard/src/entities/alert/api.ts` |
| Status badge, action buttons, transition handler, role gating | `services/dashboard/src/widgets/alert-widget/AlertWidget.tsx` |
| RESOLVED-disappears filter | `AlertWidget.tsx`'s `topLevel` computation |
| Backend contract this UI calls | `services/api/src/routes/alertLifecycle.ts` (CP2/CP3) |

---

## Retention questions

1. Why does `patchAlertStatus` reuse the WS-frame parsing shape instead of the `GET /alerts` REST adapter?
2. Walk through what happens on screen if the operator's PATCH succeeds but the resulting `alert-events` WebSocket push never arrives.
3. Why does a `409` response update the UI instead of just showing an error message?
4. Why does a nested/superseded card's badge never reflect its own `status` field?
5. What would have to change in three different places (name them) to add the "ACKNOWLEDGED BY" field the mockup sketched but this checkpoint didn't build?

---

## Completion checklist

- [ ] I can explain why the PATCH response and the `GET /alerts` response need two different parsers, not one
- [ ] I can trace what happens to the UI on a 409 versus a 500 from the PATCH endpoint
- [ ] I can justify the RESOLVED-disappears behavior against the existing `GET /alerts` contract
- [ ] I can explain why applying the PATCH response directly doesn't create a double-apply bug once the WS echo also arrives
- [ ] I have exercised the real UI myself — acknowledged and resolved a real alert, watched the badge and buttons change — not just read this document
