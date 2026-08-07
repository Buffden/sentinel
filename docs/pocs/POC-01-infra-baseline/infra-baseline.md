# POC-01: Infrastructure Baseline

**Branch:** `poc/infra-baseline`
**Status:** Not started

---

## Risk

If the local dev environment does not work cleanly, every downstream task blocks.

---

## Goal

All backing services boot together via Docker Compose, pass health checks, and are reachable from the host.

---

## Services to Validate

- Redpanda (Kafka-compatible broker)
- TimescaleDB (PostgreSQL + TimescaleDB extension)
- Neo4j
- Redis

---

## Validate

- `docker compose up -d` starts all services without errors
- Each service passes its health check
- A basic connectivity test succeeds for each store (redis-cli ping, psql query, Neo4j browser, Redpanda admin UI)

---

## Done When

- All four services start cleanly and pass health checks
- Each store is reachable from the host machine
- docker-compose.yml and any seed configuration is committed to the repo

---

## ADR Coverage

Prerequisite for all ADRs.

## Use Case Coverage

Prerequisite for all use cases.
