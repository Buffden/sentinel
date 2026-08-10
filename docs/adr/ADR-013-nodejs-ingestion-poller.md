# ADR-013: Node.js for the Ingestion Poller

**Status:** Accepted
**Date:** 2026-08-08

---

## Context

The ingestion poller polls external ADS-B (OpenSky) and AIS (AISHub) feeds on a fixed interval and publishes raw position events to Kafka (`adsb.raw`, `ais.raw`). A language must be chosen for this service.

Two options were considered:

1. Python
2. Node.js

---

## Decision

Use Node.js for the ingestion poller.

---

## Reasoning

**One runtime across the backend.** The API layer (ADR-008) is already Node.js. Using Node.js for the poller means a single runtime across all backend services. A shared internal TypeScript package can hold Kafka client configuration, event schema types, and validation logic without crossing a language boundary.

**Shared Kafka client config.** Both the poller and the API use the same Kafka broker. A shared `kafkaClient` factory and topic constant definitions avoid duplicating connection config and reduce the surface area for misconfiguration.

**Shared TypeScript types.** The raw event schemas (`adsb.raw`, `ais.raw`) must be parseable by the position consumer, and the `position.normalized` schema must be consistent across all services that produce or consume it. A shared types package enforces both contracts at compile time — the poller produces raw events; the position consumer normalises them and produces `position.normalized`.

**Async I/O is a natural fit.** The poller's work is network I/O — HTTP requests to external feeds, Kafka produce calls. Node's event loop handles this efficiently without threads.

---

## Alternatives Considered

### Python (rejected)

- Richer ecosystem for AIS/ADS-B parsing libraries (e.g. `pyais`, `pyModeS`)
- More natural for scripting and data pipeline tasks
- Cost: introduces a second runtime, making shared types and Kafka config impossible without duplication or a separate schema registry
- The AIS/ADS-B parsing libraries available in the Node ecosystem (or custom parsing of the OpenSky REST response) are sufficient for the feed formats used — no Python-specific library is strictly required

---

## Consequences

- A shared internal TypeScript package (`packages/shared` or similar) should be created to hold Kafka client config, topic constants, and `position.normalized` event types
- The poller is an independently deployable Node.js service — it does not share a process with the API
- AIS/ADS-B parsing must be handled in Node.js — either via available npm libraries or direct JSON parsing of the OpenSky REST and AISHub feed responses
