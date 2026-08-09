# ADR-008: Express (Node.js) for the API Layer

**Status:** Accepted
**Date:** 2026-08-06

---

## Context

The platform needs an API layer that serves the Angular dashboard over REST and WebSocket. Requirements:

- REST endpoints for querying position history, entity state, alerts, and investigation timelines
- WebSocket server for pushing live position updates and alert events to the dashboard, filtered by each operator's saved scope
- Operator authentication: verify Google ID tokens (ADR-011), issue short-lived JWTs, protect all routes and WebSocket upgrades
- Workspace management: persist and restore per-operator scope (geo region, entity type, alert rule filter) to TimescaleDB (ADR-012)
- Alert lifecycle: consume from the `alerts` Kafka topic, write to the `alerts` table in TimescaleDB, serve `PATCH /alerts/:alert_id` for status transitions (ADR-010)
- Investigation: serve entity timeline, evidence panel, and relationship pivot queries across TimescaleDB, Neo4j, and Redis in parallel
- Sits between the persistence stores / Kafka / Google OAuth and the frontend - reads from Redis, TimescaleDB, and Neo4j; writes to TimescaleDB (alerts, users, user_workspaces)

The API is an independently deployable service. It must not be co-located with the frontend.

---

## Decision

Use Express (Node.js) for the API layer.

---

## Reasoning

**Lightweight and readable.** Express has minimal boilerplate. The routing, middleware, and WebSocket setup are immediately legible in a code review or interview context - no annotation scanning, no dependency injection container to trace through.

**Native async I/O.** Node's event loop is a natural fit for a server that holds many concurrent WebSocket connections open and fans live updates out to dashboard clients. Blocking I/O is not a concern here - all downstream calls are network I/O (Redis, TimescaleDB, Neo4j).

**WebSocket story is simple.** The `ws` library attaches directly to an Express HTTP server in a few lines. No separate server process, no framework-specific abstraction to learn.

**Same runtime as the ingestion poller.** The poller is also Node.js (ADR-013), so shared utilities (schema validators, Kafka client config, `position.normalized` event types) live in a shared internal TypeScript package without crossing a language boundary.

**Interview readability.** A portfolio project is read, not operated. Express routes are self-documenting in a way that Spring Boot controller classes with annotations are not.

---

## Alternatives Considered

### Spring Boot (rejected)

- Strong typing and structure, but significant ceremony for a service this focused
- WebSocket setup via STOMP/SockJS introduces concepts that are irrelevant to what the dashboard actually needs
- Java is a valid choice if the team is Java-first, but nothing else in this stack is Java - adding a second language for one service is not justified

### Next.js API Routes (rejected)

- Would co-locate the API with the frontend, violating the hard constraint that services are independently deployable
- Next.js API routes are process-coupled to the Next.js frontend server - the API cannot be scaled or deployed independently
- Ruled out by architecture, not by capability

### Fastify (considered, not chosen)

- Faster than Express in benchmarks, better TypeScript support out of the box
- Would be a reasonable alternative - the trade-off is slightly more setup for marginally better performance that does not matter at this traffic volume
- Express is more universally recognised and requires less explanation

---

## Consequences

- API service is Node.js - same runtime as the ingestion poller (ADR-013); one runtime across the entire backend
- WebSocket connections are stateful - horizontal scaling requires a shared pub/sub layer (Redis pub/sub) to broadcast events across API instances; this is a known pattern and does not require additional infrastructure beyond the Redis already in the stack
- No type-safe RPC contract between API and dashboard by default - a shared TypeScript types package or OpenAPI spec should be introduced before the dashboard is built
- The API is the sole consumer of the `alerts` Kafka topic - it writes to the `alerts` table on TimescaleDB and fans out to scoped WebSocket connections
- JWT validation middleware protects every route and WebSocket upgrade; `POST /auth/google` is the only unauthenticated endpoint
- The in-memory WebSocket connection map (`connection_id -> scope`) must be rebuilt on API restart - scope is reloaded from `user_workspaces` on each new WebSocket upgrade, so no persistent state is lost
- Investigation endpoints (`GET /alerts/:id/investigation`, `GET /entities/:id/relationships`) issue parallel queries to TimescaleDB, Neo4j, and Redis - Express's async I/O model handles this without blocking other requests
