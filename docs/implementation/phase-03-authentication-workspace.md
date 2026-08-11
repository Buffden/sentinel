# Phase 03 — Authentication + Workspace

## Goal

Build the operator API foundation.

Implement:

- API service scaffold (Express + Node.js)
- Google OAuth 2.0 ID token verification
- user persistence (`users` table)
- workspace persistence (`user_workspaces` table)
- workspace geographic scope
- authenticated REST endpoints
- authenticated WebSocket setup

Keep frontend work minimal in this phase — the goal is the API contract and auth plumbing, not UI polish.

---

## Learning Goals

- authentication vs authorization
- OAuth 2.0 flow and ID token verification
- JWT issuance and validation
- identity persistence (upsert on first login)
- WebSocket session lifecycle
- user-specific state (workspace scope)
- server-side access control

---

## Suggested Checkpoints

1. Express server starts with structured JSON logging
2. `POST /auth/google` verifies a real Google ID token and returns a JWT
3. User upsert works — first login creates a row, subsequent logins update `last_login_at`
4. JWT middleware rejects invalid/expired tokens on all other routes
5. `GET /workspaces` and `PUT /workspaces` persist and retrieve operator scope
6. WebSocket upgrade requires a valid JWT; unauthenticated connections are rejected

---

## Exit Criteria

- an authenticated operator can log in, save a workspace scope, and establish a WebSocket connection
- all routes except `POST /auth/google` reject unauthenticated requests
- workspace scope is persisted to `user_workspaces` and reloaded on reconnect
- server-side scope enforcement is in place (operator receives only events within their geo region)
