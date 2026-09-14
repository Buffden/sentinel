# Workspace Scope Controls — Design and Learning Reference

Plain language first, then technical depth, then the code. Use this to understand, inspect, and defend CP5.

---

## What this checkpoint is, and deliberately isn't

CP5 gives an operator a real place to see and change their saved scope, built against the mockup approved before any of this code was written (`concepts/workspace-scope-controls/mockups/workspace-scope-editor.svg`). It calls CP1's persistence, CP1's region catalog, and CP4's reconnect — it introduces no new backend behavior at all, only a UI that finally makes CP1 through CP4 reachable by clicking instead of by hand-typing `fetch(...)` in a console.

This checkpoint does not add operator ACK/Resolve controls, does not touch the map's own layer-toggle overlay, and does not build anything for demo sessions — demo has no workspace to edit (CP1) and the control renders nothing for that role.

---

## Concepts in plain language

### Why this is two components, not one

`WorkspaceScopeControl` knows about *when* to show the editor: it checks the operator's role, loads the region catalog and current scope once, and decides whether this is a first-time setup (no saved scope) or a normal re-open. `WorkspaceScopeModal` knows *what* the editor looks like and does — the form itself, validation, calling `saveWorkspaceScope`. Splitting them means the modal has no idea whether it was opened by a click or by the auto-open-on-first-login path; it only knows `firstTime`, a single boolean, and renders accordingly (no close button, no cancel button, a warning banner). The alternative — one component juggling both concerns — would make the modal's own logic depend on how it got mounted, which is exactly the kind of implicit coupling that makes a component hard to reason about later.

### Why the region dropdown doesn't invent its own bounds for a named region

The mockup shows a plain dropdown, but the actual value it drives is real: selecting "France" doesn't let the client fabricate French coordinates — `resolveGeoRegion` on the server (CP1) always overwrites a named selection with the catalog's own bounds regardless of what the client sends. The modal's `bounds` computation mirrors this by looking the selected name up in the fetched catalog, not by tracking user-edited lat/lon fields for anything but the explicit "Custom bounds" option. This means the client and server can never disagree about what "France" means.

### Why entity types are rendered, not just hidden

The mockup deliberately shows "Aircraft" checked and disabled rather than omitting entity types from the UI entirely. An operator who never sees the field might reasonably assume some other entity type could be selected; showing it locked communicates the actual v1 constraint (CP1's `entity_types: ["aircraft"]` restriction) instead of leaving it to be discovered as an unexplained absence.

### Why Route Deviation appears, disabled, instead of not appearing at all

Same reasoning as entity types: `ROUTE_DEVIATION` is a real value in the data model, just one nothing produces yet (Phase 04 is deferred). Showing it grayed out with "Phase 4 pending" is more honest than hiding it — an operator who later notices deviation alerts never arrive despite Sentinel apparently "supporting" the type would have no way to know it was never wired up, versus an operator who can see right now that it's disabled and why.

### Why Save is disabled until at least one alert type is checked

The server's own validation (CP1) rejects an empty `alert_types` array — `isSubsetOfAllowed` requires a non-empty subset. The modal enforces the same rule client-side purely for a faster feedback loop (no round trip needed to learn the save would fail); the server remains the actual authority, unchanged.

---

## Map to code

| Concept | Where it lives |
| --- | --- |
| Approved mockup | `concepts/workspace-scope-controls/mockups/workspace-scope-editor.svg` |
| Role check, first-time detection, icon button | `services/dashboard/src/features/workspace/WorkspaceScopeControl.tsx` |
| The editor form itself | `services/dashboard/src/features/workspace/WorkspaceScopeModal.tsx` |
| `getWorkspaceScope` / `saveWorkspaceScope` / `getRegions` | `services/dashboard/src/features/workspace/workspaceApi.ts` (CP1/CP4 additions, `getRegions` new for CP5) |
| Wired into the app shell | `services/dashboard/src/app/dashboard/page.tsx` (top nav `right` slot) |

---

## Retention questions

1. Why does the modal not need to know *why* it was opened (click vs. first-login), only whether `firstTime` is true?
2. Walk through what would go wrong if the client computed a named region's bounds itself instead of looking them up from the fetched catalog.
3. Why show a disabled "Route Deviation" checkbox instead of simply not rendering it until Phase 04 exists?
4. What actually enforces the "at least one alert type" rule — the client, the server, or both, and why does that split matter?
5. Why does `WorkspaceScopeControl` render nothing at all for a demo session, rather than rendering a disabled control?

---

## Completion checklist

- [ ] I can explain the actual division of responsibility between `WorkspaceScopeControl` and `WorkspaceScopeModal`
- [ ] I can trace how a selected named region turns into the exact bounds the server will independently recompute
- [ ] I can justify showing disabled options (entity types, Route Deviation) as a UX decision, not an oversight
- [ ] I can explain why client-side validation here is a UX nicety, not the actual guarantee
- [ ] I have exercised the real UI myself — opened the editor, changed a scope, watched the alert feed change — not just read this document
