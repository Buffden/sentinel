# POC Plan

Before writing production service code, validate the riskiest integrations in isolated proof-of-concept branches. Each POC proves a specific architectural claim made in the ADRs and validates the acceptance criteria of the use cases that depend on it. If a POC fails or reveals a wrong assumption, the ADR is updated before implementation begins - not after.

---

## POC Order

```
POC-01  ->  POC-02  ->  POC-03  ->  POC-04  ->  POC-05
```

The first two are blockers. They validate the local environment and the primary data source before any pipeline code is written. The remaining three can overlap with early implementation work once the first two pass.

---

## POC Index

| POC | Branch | Risk area | ADR | Use cases |
| --- | --- | --- | --- | --- |
| [POC-01](POC-01-infra-baseline/infra-baseline.md) | `poc/infra-baseline` | Local dev environment | All | All |
| [POC-02](POC-02-opensky-feed/opensky-feed.md) | `poc/opensky-feed` | Live feed access and DLQ routing | ADR-001 | US-08, US-09, US-10 |
| [POC-03](POC-03-h3-timescaledb/h3-timescaledb.md) | `poc/h3-timescaledb` | Geo-cell sharding and continuous aggregates | ADR-002, ADR-006 | US-04, US-11, US-12 |
| [POC-04](POC-04-neo4j-proximity/neo4j-proximity.md) | `poc/neo4j-proximity` | Graph traversal and idempotent edge writes | ADR-003, ADR-007 | US-05, US-06, US-11 |
| [POC-05](POC-05-redis-leader-election/redis-leader-election.md) | `poc/redis-leader-election` | Leader election, alert state, entity TTL | ADR-005 | US-01, US-03, US-07 |

---

## POC Outcome Protocol

Each POC branch is merged only after its "Done when" criteria are met. If a POC reveals a wrong assumption:

1. Update the relevant ADR to reflect the corrected decision
2. Update the affected use case acceptance criteria if the behaviour changes
3. Document what the POC found and why the original assumption was wrong
4. Do not begin implementation of the affected service until the ADR is updated

---

## What Does Not Need a POC

| Component | Reason | Use cases served |
| --- | --- | --- |
| Kafka/Redpanda producer-consumer | Standard, well-documented pattern. Low integration risk. | US-08, US-09, US-10 (covered by POC-02) |
| Express + WebSocket (`ws` library) | Textbook setup. No novel integration. | US-02 |
| Angular + Leaflet dashboard | Lowest-priority component. Built last, kept minimal. | US-01, US-02 (read path only) |
| Google OAuth 2.0 + JWT (ADR-011) | `google-auth-library` handles token verification in ~10 lines. JWT issuance via `jsonwebtoken` is standard. No novel integration risk. | US-15 |
| Alert lifecycle state - TimescaleDB alerts table (ADR-010) | A plain PostgreSQL table with `ON CONFLICT DO NOTHING`. Covered by the TimescaleDB connectivity test in POC-01. | US-13 |
| Workspace scope + server-side filter (ADR-012) | In-process Map lookup and bounding-box check. No external dependency. | US-15 |
