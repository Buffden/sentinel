# Phase 01 — Infrastructure + Canonical Schemas + Observability Skeleton

## Goal

Create the local platform and shared engineering conventions required by every later phase.

Run locally: Redpanda, TimescaleDB, Redis, and Neo4j. Create Docker Compose configuration, environment configuration, database migrations, canonical schemas, Kafka topics, Neo4j constraints/indexes, a baseline structured-logging convention, and a baseline health/readiness convention.

Do not build a centralized observability platform yet. Establish the conventions every service will follow when it is introduced.

## Hands-On Requirement

Before substantial application code, manually publish/consume and inspect Kafka offsets; connect to TimescaleDB and inspect hypertables; manipulate Redis hashes, sorted sets and TTLs; and create/query Neo4j nodes and relationships.

## Suggested Checkpoints

1. Docker Compose starts all services cleanly.
2. TimescaleDB migrations run from scratch and are idempotent.
3. Kafka topic creation is idempotent.
4. Neo4j constraints/indexes are applied.
5. Manual verification of every store is complete.
6. Define structured JSON log fields (`service`, `level`, `timestamp`, plus contextual IDs when available).
7. Define health/readiness behavior for services introduced later.
8. Basic infrastructure health is inspectable from CLI/container tooling.

## Exit Criteria

All infrastructure is reproducible and inspectable, and later services have documented logging and health conventions to follow from their first commit.
