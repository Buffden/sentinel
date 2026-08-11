# Phase 01 — Infrastructure + Canonical Schemas

## Goal

Create the local infrastructure required by Sentinel.

Run locally:

- Redpanda
- TimescaleDB
- Redis
- Neo4j

Create:

- Docker Compose configuration
- environment configuration
- database migrations
- canonical schemas
- Kafka topics
- Neo4j constraints/indexes
- basic health verification

---

## Learning Goals

Understand:

- containers
- Docker networking
- ports
- volumes
- service dependencies
- message brokers
- relational/time-series storage
- ephemeral state
- graph storage
- polyglot persistence

---

## Hands-On Requirement

Before writing substantial application code, manually interact with every infrastructure component.

### Redpanda

- create a topic
- publish one message
- consume it
- inspect partitions and offsets

### TimescaleDB

- connect via `psql`
- inspect the schema
- insert a row manually
- query it
- inspect hypertable metadata

### Redis

- set and get a key
- inspect a hash
- set a TTL and watch it expire
- inspect a sorted set

### Neo4j

- create a node
- create a relationship
- run a `MATCH` query
- run a `MERGE` and confirm idempotency

---

## Suggested Checkpoints

1. Docker Compose starts all four services cleanly with no port conflicts
2. TimescaleDB migrations run from scratch and are idempotent
3. Kafka topic creation script runs idempotently
4. Neo4j constraints and indexes are applied
5. Manual hands-on verification of each store complete
6. Basic health check confirms all services reachable

---

## Exit Criteria

- all infrastructure starts locally with `docker compose up -d`
- each service is reachable on its configured port
- schemas can be torn down and recreated from scratch using migrations
- Kafka topics exist with correct retention and partition config
- developer can inspect every datastore manually using CLI tools
- basic health is observable (container status, port checks)
