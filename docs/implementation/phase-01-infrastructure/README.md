# Phase 01: Infrastructure

Create the local platform and shared engineering conventions required by every later phase: Redpanda, TimescaleDB, Redis, and Neo4j running in Docker Compose, with canonical schemas, Kafka topics, Neo4j constraints, a structured-logging convention, and a health/readiness convention.

No application code is written in this phase.

## Goal

All infrastructure reproducible and inspectable from the CLI. Later services have documented logging and health conventions to follow from their first commit.

## Hands-on requirement

Before substantial application code, manually publish/consume and inspect Kafka offsets; connect to TimescaleDB and inspect hypertables; manipulate Redis hashes, sorted sets, and TTLs; and create/query Neo4j nodes and relationships.

## What was built

| Concern | Artifact |
| --- | --- |
| Container orchestration | `docker-compose.yml` with named volumes and healthchecks |
| TimescaleDB schema | `infra/migrations/001-006` applied via `infra/scripts/migrate.sh` |
| Kafka topics | 8 canonical topics via `infra/kafka/topics.sh` |
| Neo4j schema | 2 uniqueness constraints via `infra/neo4j/schema.cypher` |
| Logging convention | Structured JSON log contract for all Phase 02+ services |
| Health convention | Liveness and readiness probe contract for all Phase 02+ services |

## Bootstrap sequence

```bash
make up           # start all four containers, wait for healthy
make migrate      # apply TimescaleDB migrations 001-006
make topics       # provision 8 canonical Kafka topics
make neo4j-schema # apply Neo4j uniqueness constraints
```

All four commands are idempotent and safe to re-run.

## Checkpoints

1. Docker Compose starts all services cleanly
2. TimescaleDB migrations run from scratch and are idempotent
3. Kafka topic creation is idempotent
4. Neo4j constraints and indexes are applied
5. Manual verification of every store is complete
6. Structured JSON log field contract defined
7. Health and readiness behavior defined for all future services
8. Infrastructure health inspectable from CLI and container tooling

## Contents

| File | Description |
| --- | --- |
| [`concepts/`](concepts/README.md) | Concept notes and checkpoint debriefs, in reading order |
| [`exit-verification.md`](exit-verification.md) | Final store inspection, reproducibility check, and Phase 01 exit criteria |
