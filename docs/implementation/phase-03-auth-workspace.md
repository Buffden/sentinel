# Phase 03 — Auth + Workspace Scope

## Goal

Operators authenticate via Google OAuth. Every API route and WebSocket connection requires a valid JWT. Operators configure a workspace scope (geo region, entity types) and the API applies it server-side — each WebSocket connection only receives events matching its scope.

## Dependencies

- Phase 02 (API and dashboard running)

## Tasks

### API — Auth

- [ ] `POST /auth/google` — accepts Google ID token, verifies with Google, upserts user in `users` table (`ON CONFLICT (google_sub) DO UPDATE SET last_login_at = now()`), returns signed JWT
  - This is the only unauthenticated endpoint
- [ ] JWT middleware applied to all other routes and WebSocket upgrades
- [ ] `GET /users/me` — returns current operator profile

### API — Workspace

- [ ] `GET /users/me/workspace` — returns operator's saved `scope` JSONB from `user_workspaces`; returns 404 if no workspace saved yet
- [ ] `PUT /users/me/workspace` — upserts scope: `{ geo_region: { name, bounds }, entity_types, alert_types }` (`ON CONFLICT (user_id) DO UPDATE`)
- [ ] On WebSocket upgrade (`GET /stream?token=<JWT>`): load scope from `user_workspaces`, attach to connection in the in-memory connection map
- [ ] Scope filtering on `position-updates` fan-out:
  - Entity position must fall within `scope.geo_region.bounds`
  - `entity_type` must be in `scope.entity_types`
- [ ] Scope reloaded from `user_workspaces` on each new WebSocket upgrade (not cached across reconnects)

### Dashboard — Auth + Scope

- [ ] `/login` route with Google Sign-In button
- [ ] On sign-in: send Google ID token to `POST /auth/google`, store JWT in memory only — not localStorage, not a cookie
- [ ] JWT attached to all API calls and WebSocket upgrade header / query param
- [ ] Redirect unauthenticated users to `/login`
- [ ] On load: call `GET /users/me/workspace`
  - 404 → show scope setup prompt (clean map, no alerts yet)
  - 200 → restore saved scope immediately, no prompt
- [ ] Predefined static region list with bounding boxes (e.g. France, UK, Mediterranean) — no external geocoder; operator selects from list
- [ ] Workspace scope panel: region selector + entity type checkboxes + alert type checkboxes
- [ ] On scope save (`PUT /users/me/workspace`): reconnect WebSocket so new scope is applied server-side

## Done When

- Unauthenticated requests to any endpoint except `POST /auth/google` return 401
- Google Sign-In flow completes and a JWT is issued
- First-time visitor sees scope setup prompt; returning operator's saved scope restores automatically
- `GET /users/me/workspace` returns 404 before scope is saved; 200 with scope JSON after
- A WebSocket connection scoped to a geo region does not receive position events outside that region
- Changing scope and reconnecting applies the new filter immediately
