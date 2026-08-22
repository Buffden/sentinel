# ADR-016: Replace Angular with Next.js + React for the Dashboard

**Status:** Accepted
**Date:** 2026-08-21
**Supersedes:** ADR-009 (Angular + Leaflet for the Dashboard)

---

## Context

ADR-009 chose Angular + Leaflet for the dashboard with three arguments: RxJS for WebSocket streams, TypeScript-first enforcement, and portfolio breadth. The dashboard has not been built yet — no Angular code exists.

Since ADR-009 was written, a clearer picture of the dashboard's requirements has emerged:

- The UI needs to look and feel like a professional geospatial intelligence platform (Palantir Gotham / Foundry aesthetic): dense data tables, alert feeds, investigation panels, relationship graph views, and a live map.
- Palantir open-sourced **Blueprint.js**, a React component library purpose-built for exactly this class of application. It provides pre-built tables, data grids, popovers, dialogs, toasts, and a dark theme — all production-quality and designed for dense, data-heavy operator dashboards.
- The broader React ecosystem has significantly stronger geospatial tooling: `react-leaflet`, `deck.gl` for WebGL-accelerated track rendering, and `react-force-graph` / `@antv/g6` for the Neo4j proximity relationship graph.
- Next.js used as a pure CSR app (no SSR, no API routes) gives access to the full React ecosystem with no architectural compromises.

The cost of switching is zero: the dashboard has not been started.

---

## Decision

Replace Angular + Leaflet (ADR-009) with **Next.js (CSR only) + Blueprint.js + react-leaflet** for the dashboard.

The backend is unaffected. Express, Kafka services, TimescaleDB, Redis, and Neo4j remain unchanged.

---

## Reasoning

**Blueprint.js eliminates building UI components from scratch.** The dashboard requires alert tables, entity inspection panels, lifecycle controls, dialogs, and a sidebar shell. Blueprint.js provides all of these at production quality with the exact aesthetic Sentinel targets. Building equivalent components in Angular from scratch would take significantly longer and produce an inferior result.

**React ecosystem has better geospatial and graph tooling.** `react-leaflet` is a direct equivalent of `ngx-leaflet`. `deck.gl` enables high-performance WebGL rendering of large numbers of aircraft tracks if needed. `react-force-graph` and `@antv/g6` provide graph visualisation for the Neo4j proximity evidence panel — both are React-native with no Angular equivalent of comparable quality.

**Next.js as CSR is not an architectural compromise.** The dashboard is a pure client-side rendered operator tool — no SEO, no public pages, no static generation. SSR is disabled. Next.js is used only as a well-supported React project scaffold with file-based routing. None of Next.js's server features are used, and no API routes are introduced (the API layer remains Express per ADR-008).

**The backend cannot move to Next.js API routes regardless of the FE choice.** Kafka consumers, WebSocket fan-out, leader election, and long-running alert evaluation are persistent server processes. Next.js API routes are stateless serverless-style handlers and cannot host any of these. The FE/BE split is architecturally justified on its own merits, independent of which FE framework is chosen.

**The RxJS argument from ADR-009 does not hold at Sentinel's scale.** ADR-009 argued that Angular's first-class RxJS integration is the strongest reason to choose Angular for a streaming-heavy UI. In practice, Sentinel's WebSocket usage is a single scoped feed per operator session. Managing this with a `useEffect` + `useRef` hook or Zustand is straightforward and does not require Observable composition operators.

**Portfolio signal is stronger with this stack.** Next.js + Blueprint.js + deck.gl alongside Kafka, TimescaleDB, Neo4j, and Redis is a more distinctive and recognisable stack for a geospatial intelligence platform than Angular + Leaflet. It more directly reflects the tooling used by companies building this class of product.

---

## Alternatives Considered

### Keep Angular (rejected)

- No Blueprint.js — component library would need to be built from scratch or a lesser Angular alternative used
- Weaker geospatial and graph visualisation ecosystem
- No practical advantage over React for Sentinel's specific dashboard requirements
- The RxJS streaming argument does not outweigh the ecosystem gap at this scale

### Plain React with Vite (not chosen)

- Technically valid — avoids the SSR overhead of Next.js
- Next.js is preferred because it provides file-based routing, a standard project structure, and broad community tooling with no meaningful downside when SSR is disabled

### Next.js with SSR / API routes (rejected)

- SSR adds a server process with no benefit for an authenticated real-time dashboard
- API routes would co-locate the API with the frontend and break service independence (ADR-008)
- CSR-only Next.js is the correct configuration

---

## Consequences

- ADR-009 is superseded. Angular is removed from the accepted stack. `CLAUDE.md` and `ARCHITECTURE.md` must be updated to reflect Next.js + Blueprint.js + react-leaflet.
- The dashboard is still the lowest-priority component — it will be built last and kept minimal. This decision does not change that priority.
- Next.js is configured as a pure CSR app: `output: 'export'` or `ssr: false` on all pages. No server-side rendering, no API routes.
- Blueprint.js requires React 18. Next.js 14+ uses React 18 by default. No version conflicts.
- `react-leaflet` replaces `ngx-leaflet`. Leaflet itself is unchanged — the same tiles, markers, and map primitives apply.
- The WebSocket connection is managed with a custom React hook (`useAlertStream` or similar) using `useEffect` and `useRef`. Zustand manages global state (active entities, alert feed, operator scope).
- The Google OAuth flow uses `@react-oauth/google`. The JWT handling contract (memory-only, lost on tab close, silent re-auth) is unchanged from ADR-009.
- The shared TypeScript types package (ADR-013 consequence) applies equally here — the dashboard consumes the same `position.normalized` and alert event types as before.
- Leaflet is not SSR-compatible. This is not a concern since SSR is disabled.
