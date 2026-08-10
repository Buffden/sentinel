# Implementation Plan

Phases are ordered around vertical slices — each phase produces something visible and testable end-to-end. Get the core pipeline working before adding rules and lifecycle on top.

---

## Phase Overview

| Phase | Goal | Depends On |
|---|---|---|
| [01](phase-01-infra-schema.md) | All backing services running + schemas initialised | — |
| [02](phase-02-live-pipeline.md) | Moving dots on a map — full pipeline end-to-end | 01 |
| [03](phase-03-auth-workspace.md) | Operator auth + scoped WebSocket filtering | 02 |
| [04](phase-04-alert-pipeline.md) | Signal loss + route deviation alerts visible on dashboard | 03 |
| [05](phase-05-correlation.md) | Proximity edges in Neo4j from live position stream | 02 |
| [06](phase-06-proximity-composite.md) | Proximity + composite alert rules | 04, 05 |
| [07](phase-07-alert-lifecycle.md) | Acknowledge + resolve alerts, audit trail | 04 |
| [08](phase-08-entity-investigation.md) | Evidence panel — parallel fetch from Redis + TimescaleDB + Neo4j | 06, 07 |
| [09](phase-09-observability.md) | Observability + failure injection | 08 |

---

## Branch Map

```
main
 ├── infra/docker-compose + schema/init
 ├── ingestion/poller + position-consumer/core + api/core + dashboard/live-map
 ├── api/auth + dashboard/workspace
 ├── deviation-detector/core + alert-evaluator/signal-loss + alert-evaluator/route-deviation + api/alerts + dashboard/alert-panel
 ├── correlation/core
 ├── alert-evaluator/proximity + alert-evaluator/composite
 ├── api/alert-lifecycle + dashboard/alert-actions
 └── api/investigation + dashboard/evidence-panel
```
