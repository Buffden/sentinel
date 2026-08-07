# ADR-011: Google OAuth 2.0 for Operator Authentication

**Status:** Accepted
**Date:** 2026-08-07

---

## Context

Sentinel stores per-operator workspace preferences (geographic scope, entity type filter, alert rule filter) so that an operator's view is restored when they return to the dashboard. This requires identity - a workspace must be keyed to a specific person, not to a browser session or device.

Requirements:
- Operators must be identifiable across sessions and devices
- Workspace preferences must be retrievable by user identity via the API
- The WebSocket alert stream must be scoped per-operator (scope is loaded server-side from the user's saved workspace)
- Credentials must never be stored or managed by Sentinel itself
- No dedicated identity infrastructure should be added

---

## Decision

Use Google OAuth 2.0 for operator authentication. The flow is:

1. The Angular dashboard initiates a Google OAuth popup using the Google Identity Services SDK
2. On success, Google returns an ID token to the client
3. The client sends the ID token to `POST /auth/google` on the Express API
4. The Express API verifies the token with Google's token info endpoint (or via the `google-auth-library` package)
5. The API extracts the Google `sub` (stable user ID) and `email` from the verified token
6. The API upserts a row in the `users` table on TimescaleDB: `(user_id, google_sub, email, last_login_at)`
7. The API issues a short-lived JWT (signed with a server secret, 8-hour expiry) containing `user_id`
8. All subsequent REST calls and the WebSocket connection carry this JWT in the `Authorization` header or as a query parameter

The JWT is verified by Express middleware on every protected route and WebSocket upgrade.

---

## Reasoning

**Credentials must not live in Sentinel.** Username/password storage requires hashing, reset flows, breach handling, and audit. This is not what this project demonstrates. Delegating to Google means Sentinel never touches a credential.

**Google accounts are universal for the operator persona.** The intended users of this dashboard are analysts or engineers who already have Google accounts. There is no onboarding friction.

**No new infrastructure.** A `users` table on the existing TimescaleDB instance stores only the Google `sub`, email, and last login timestamp. No dedicated auth database, no Redis session store, no external auth service.

**JWT is stateless and fits the WebSocket model.** The WebSocket connection carries the JWT as a query parameter at upgrade time. The server validates it once at connection open and loads the operator's saved workspace from the `user_workspaces` table. No session lookup on every message.

**Short-lived tokens limit blast radius.** An 8-hour expiry means a leaked token expires within the same working day. Refresh is handled by re-initiating the Google OAuth flow (silent re-auth if the Google session is still active).

---

## Alternatives Considered

### Auth0 / Cognito / Supabase Auth (rejected)

- Each provides a fully managed identity platform with social login, refresh tokens, and user management UI
- All add an external service dependency that must be configured, priced, and operated
- Sentinel is a portfolio project; adding a managed auth platform obscures the authentication flow behind a black box
- Rolling the Google OAuth verification directly in Express (30 lines) is more demonstrable and more interview-friendly

### Username and password (rejected)

- Requires credential storage, hashing (bcrypt), reset flows, and security hardening
- Adds implementation surface area with no architectural value for this project
- Google OAuth provides stronger security (no password to leak) with less code

### No authentication, localStorage workspace (rejected)

- Workspace preferences cannot follow an operator across devices or browsers
- If any operator-specific feature is added later (alert assignment, team views), there is no identity to key it to
- Retrofitting auth after the fact is more disruptive than starting with a lightweight flow from day one

### Redis session store (rejected)

- A session token stored in Redis requires a Redis lookup on every request
- JWT validation is local (no network call) and sufficient given the short expiry and single-operator use case
- Redis is already used for live entity state; adding session management conflates two unrelated concerns in the same store

---

## Consequences

- A `users` table is added to TimescaleDB: `(user_id UUID PK, google_sub TEXT UNIQUE, email TEXT, last_login_at TIMESTAMPTZ)`
- A `user_workspaces` table is added to TimescaleDB - see ADR-012
- `POST /auth/google` is the only unauthenticated endpoint (beyond the health check)
- All other REST routes and the WebSocket upgrade require a valid JWT in the `Authorization: Bearer <token>` header
- The Angular dashboard handles the Google OAuth popup using `@google/identity` and stores the Sentinel JWT in memory (not localStorage) for the session duration
- The JWT secret is an environment variable - never committed to source
