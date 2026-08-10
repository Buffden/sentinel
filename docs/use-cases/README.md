# Use Cases

This folder defines what Sentinel does from the perspective of its users and the system itself. Each use case is written at the level needed to justify the architectural decisions in the ADRs - not to specify UI behavior or product polish.

---

## Actors

| Actor | Description |
|---|---|
| Operator | A person monitoring the live dashboard. Wants to be alerted to meaningful anomalies without being flooded with noise. |
| Ingestion client | The ADS-B/AIS poller. Produces a continuous stream of positional telemetry from external feeds. |
| System | Sentinel itself - the pipeline, consumers, correlation engine, and alert evaluator acting autonomously. |

---

## Use Case Index

### Operator - Live Tracking

| ID | Title | ADR | Diagrams |
|---|---|---|---|
| [US-01](US-01-live-entity-tracking/live-entity-tracking.md) | Live entity tracking on map | ADR-004, ADR-012 | write path, read path, entity expiry |
| [US-02](US-02-live-map-updates/live-map-updates.md) | Continuous map updates via WebSocket | ADR-008, ADR-012 | connection setup, update push |

### Operator - Anomaly Alerts

| ID | Title | ADR | Diagrams |
|---|---|---|---|
| [US-03](US-03-signal-loss-alert/signal-loss-alert.md) | Signal loss alert | ADR-002 | detection, alert delivery, alert suppression |
| [US-04](US-04-route-deviation-alert/route-deviation-alert.md) | Route deviation alert | ADR-002, ADR-014 | baseline computation, deviation detection, transient vs sustained |
| [US-05](US-05-unscheduled-proximity/unscheduled-proximity.md) | Unscheduled proximity alert | ADR-003, ADR-014 | proximity detection, graph update |
| [US-06](US-06-composite-alert/composite-alert.md) | Composite correlated alert | ADR-003, ADR-014 | signal correlation, composite emission, single signal path |
| [US-07](US-07-duplicate-free-alerts/duplicate-free-alerts.md) | Duplicate-free alert emission | ADR-005 | leader election, failover, race condition without election |
| [US-13](US-13-alert-lifecycle/alert-lifecycle.md) | Alert lifecycle management | ADR-010 | state transitions, acknowledge flow, resolve and reopen |

### Operator - Investigation

| ID | Title | ADR | Diagrams |
|---|---|---|---|
| [US-14](US-14-entity-investigation/entity-investigation.md) | Entity investigation timeline | ADR-002, ADR-003, ADR-004 | timeline query, evidence panel, graph pivot |

### Operator - Workspace and Authentication

| ID | Title | ADR | Diagrams |
|---|---|---|---|
| [US-15](US-15-scoped-alert-subscription/scoped-alert-subscription.md) | Scoped alert subscription and workspace | ADR-011, ADR-012 | scope setup, scoped alert delivery, workspace restore |

### System - Ingestion Reliability

| ID | Title | ADR | Diagrams |
|---|---|---|---|
| [US-08](US-08-ingestion-reliability/ingestion-reliability.md) | Burst-tolerant ingestion | ADR-001 | normal ingestion, consumer outage recovery |
| [US-09](US-09-dead-letter-queue/dead-letter-queue.md) | Dead-letter queue for malformed events | ADR-001 | malformed event routing, DLQ inspection and recovery |
| [US-10](US-10-event-replay/event-replay.md) | Event replay from Kafka offset | ADR-001 | automatic restart replay, intentional backfill |

### System - Write Correctness

| ID | Title | ADR | Diagrams |
|---|---|---|---|
| [US-11](US-11-idempotent-writes/idempotent-writes.md) | Idempotent writes under replay | ADR-007 | per-store idempotency, duplicate delivery |

### System - Geo-spatial Efficiency

| ID | Title | ADR | Diagrams |
|---|---|---|---|
| [US-12](US-12-geo-spatial-efficiency/geo-spatial-efficiency.md) | Efficient regional position queries | ADR-006 | geo-cell write, regional query, hot-spot distribution |

---

## Out of Scope for v1

- Additional identity providers beyond Google OAuth (see ADR-011)
- RBAC beyond a single operator role - multi-role access control is not justified until there is a real multi-role requirement
- ML-based anomaly scoring - all detection is rule-based and correlation-based
- Historical alert replay or audit log browsing in the dashboard
- Multi-tenant operator isolation
- Mobile or responsive dashboard layout
