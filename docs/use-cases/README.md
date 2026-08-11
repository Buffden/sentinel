# Use Cases

This folder defines Sentinel behavior from the perspective of operators and the system. Use cases describe the **final v1 behavior** unless a document explicitly says otherwise. Implementation phase boundaries are authoritative in `docs/implementation/`; do not implement later-phase behavior early just because a final-state use-case diagram shows it.

---

## Actors

| Actor | Description |
| --- | --- |
| Operator | Uses the dashboard to monitor entities, alerts, and investigation evidence. |
| Ingestion client | Polls ADS-B/AIS sources and publishes raw telemetry. |
| System | Sentinel's consumers, detectors, evaluator, API, and persistence components. |

---

## Use Case Index

### Live Tracking

| ID | Title | Main ADRs |
| --- | --- | --- |
| [US-01](US-01-live-entity-tracking/live-entity-tracking.md) | Live entity tracking on map | ADR-004, ADR-012 |
| [US-02](US-02-live-map-updates/live-map-updates.md) | Continuous map updates via WebSocket | ADR-008, ADR-012 |

### Anomaly Alerts

| ID | Title | Main ADRs |
| --- | --- | --- |
| [US-03](US-03-signal-loss-alert/signal-loss-alert.md) | Signal loss alert | ADR-004, ADR-005, ADR-010, ADR-014 |
| [US-04](US-04-route-deviation-alert/route-deviation-alert.md) | Route deviation alert | ADR-014, ADR-015 |
| [US-05](US-05-unscheduled-proximity/unscheduled-proximity.md) | Unscheduled proximity | ADR-003, ADR-006, ADR-014 |
| [US-06](US-06-composite-alert/composite-alert.md) | Composite correlated alert | ADR-010, ADR-014 |
| [US-07](US-07-duplicate-free-alerts/duplicate-free-alerts.md) | Replay-safe alert emission and failover | ADR-005, ADR-007 |
| [US-13](US-13-alert-lifecycle/alert-lifecycle.md) | Alert lifecycle management | ADR-010 |

### Investigation / Workspace

| ID | Title | Main ADRs |
| --- | --- | --- |
| [US-14](US-14-entity-investigation/entity-investigation.md) | Entity investigation | ADR-002, ADR-003, ADR-004 |
| [US-15](US-15-scoped-alert-subscription/scoped-alert-subscription.md) | Scoped alert subscription/workspace | ADR-011, ADR-012 |

### Reliability / Correctness

| ID | Title | Main ADRs |
| --- | --- | --- |
| [US-08](US-08-ingestion-reliability/ingestion-reliability.md) | Burst-tolerant ingestion | ADR-001 |
| [US-09](US-09-dead-letter-queue/dead-letter-queue.md) | Dead-letter routing | ADR-001 |
| [US-10](US-10-event-replay/event-replay.md) | Event replay | ADR-001, ADR-007 |
| [US-11](US-11-idempotent-writes/idempotent-writes.md) | Idempotent writes under replay | ADR-007 |
| [US-12](US-12-geo-spatial-efficiency/geo-spatial-efficiency.md) | Historical geo filtering + live spatial scoping | ADR-006 |

---

## Final-State vs Phase Boundary

Examples:

- US-03 final-state alert delivery may show Redis `alert-events` and scope-matched multi-instance WebSockets. Phase 03 only needs first-instance authenticated delivery; distributed fan-out arrives in Phase 08 and workspace scope in Phase 07.
- US-13 describes final lifecycle behavior; Phase 03 only persists `NEW` alerts, while acknowledge/resolve/supersede fan-out is completed in Phase 08.
- Investigation behavior belongs to Phase 09 even though Neo4j/TimescaleDB contracts are defined earlier.

Always read the current phase file before implementing a use-case path.

---

## Out of Scope for v1

- identity providers beyond Google OAuth;
- elaborate RBAC/multi-role product behavior;
- ML-based anomaly scoring;
- multi-tenant organization isolation;
- speculative mobile/responsive product polish;
- infrastructure not justified by an accepted ADR.
