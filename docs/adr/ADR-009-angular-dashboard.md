# ADR-009: Angular + Leaflet for the Dashboard

**Status:** Superseded by ADR-016
**Date:** 2026-08-06

---

## Context

The platform needs a live map dashboard that:

- Handles Google OAuth login via the Google Identity Services SDK and stores the resulting JWT in memory for the session
- Presents a scope setup prompt on first visit (geo region, entity type, alert rule filter); restores the saved workspace on return visits
- Renders a real-time map showing only entities and alerts within the operator's saved scope
- Consumes a scoped WebSocket feed (`/stream?token=<JWT>`) for live position updates and alert events filtered server-side
- Displays an alert feed with lifecycle controls (acknowledge, resolve) via `PATCH /alerts/:alert_id`
- Provides an entity investigation panel: position timeline on the map, proximity event markers, evidence panel assembled from all three stores, and a relationship graph pivot
- Is client-side rendered - there is no meaningful SEO or first-paint concern for an operator dashboard

The dashboard is explicitly deprioritized in this project. Its primary backend design implications are the WebSocket contract, the auth flow, and the investigation API it consumes from the API layer. Visual polish and UX completeness are out of scope.

---

## Decision

Use Angular (with RxJS) for the dashboard shell and Leaflet (via ngx-leaflet) for the map.

---

## Reasoning

**RxJS Observables map directly to WebSocket streams.** Angular's first-class RxJS integration means a WebSocket connection is naturally modeled as an Observable stream. Live position updates and alert events can be piped, filtered, debounced, and combined using standard RxJS operators - no custom event-emitter plumbing required. This is the strongest technical argument for Angular over React in this specific use case.

**TypeScript-first.** Angular enforces TypeScript throughout. Given that the API layer (Express) will expose typed response shapes, keeping the dashboard in TypeScript reduces the surface area for contract mismatches.

**Leaflet is framework-agnostic.** Leaflet does not care whether it runs inside Angular, React, or plain HTML. ngx-leaflet provides a thin Angular wrapper. The map rendering choice is independent of the framework choice.

**Portfolio breadth.** Demonstrating Angular alongside Node, Kafka, TimescaleDB, Neo4j, and Redis shows a wider range than a React/Node/Postgres stack would.

---

## Alternatives Considered

### Next.js + React (rejected for this role)

- Next.js is primarily an SSR/SSG framework. This dashboard is pure CSR - live map, WebSocket feed, no static pages. SSR adds deployment complexity (server process) with no benefit.
- React with a plain Vite build would be a valid FE-only alternative, but the RxJS argument for Angular applies and Angular was already the working assumption
- Next.js API routes as a combined FE+BE were ruled out in ADR-008: they would co-locate the API with the frontend and break service independence

### Vue (rejected)

- Lighter than Angular, but lacks Angular's built-in RxJS integration
- Smaller portfolio signal in enterprise/backend-adjacent contexts
- No material advantage over Angular for this specific use case

### Plain React (Vite, no Next.js)

- Reasonable alternative if the team is React-first
- WebSocket state management requires an external library (Zustand, Redux) or manual useEffect wiring - more boilerplate than RxJS Observables for a streaming-heavy UI
- Would be the first choice if Angular familiarity were not already assumed

---

## Consequences

- The dashboard is the lowest-priority component in the project - it will be built last and kept minimal
- The WebSocket message schema between the Express API and the Angular dashboard must be defined as a shared TypeScript types package to avoid drift
- Leaflet is not SSR-compatible - this is not a concern since the app is CSR-only, but it means the dashboard cannot be migrated to Next.js SSR without replacing the map library
- The `@google/identity` SDK is required for the Google OAuth popup flow; the Sentinel JWT is stored in memory only (not localStorage, not a cookie) and is lost on tab close - Google silent re-auth handles seamless re-login
- The scope setup component is the entry point of the app; no WebSocket connection is opened and no entities or alerts are shown until a scope is saved
- Alert lifecycle controls (acknowledge, resolve) are REST calls (`PATCH /alerts/:alert_id`) - not over WebSocket - so they need no special RxJS handling beyond a standard HTTP call
- The investigation panel requires three parallel REST calls on open (`GET /alerts/:id/investigation`); RxJS `forkJoin` is the natural fit here given Angular's first-class RxJS integration
