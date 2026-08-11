# Phase 09 — Observability + Failure Hardening

## Goal

Make Sentinel debuggable and verifiably correct under failure.

Avoid enterprise observability overengineering. The goal is practical: the developer should be able to answer "is this working?" for every meaningful component without reading source code.

---

## Observability

Add to every service:

- structured JSON logs with `service`, `level`, and `correlation_id` on every line
- error counters (DLQ events, parse failures, write errors)
- consumer lag visibility (Redpanda console or `kafka-consumer-groups`)
- `/health` endpoint with dependency checks (TimescaleDB, Redis, Neo4j, Kafka reachability)
- meaningful log lines at every important state transition (alert emitted, leader acquired, episode created, offset committed)

---

## Failure Lab

Deliberately break the system and observe recovery.

For each experiment, answer before running it:

> What do I expect to happen?

Then run it and compare.

### Position Consumer

- kill it mid-stream
- restart; confirm Kafka replay resumes from the last committed offset
- confirm no duplicate rows in `position_history`

### Redis

- kill Redis
- observe which services degrade and which fail hard
- restart Redis; confirm state is rebuilt from live Kafka tail (not a full history replay)

### TimescaleDB

- kill TimescaleDB
- observe Position Consumer and API behavior
- restart; confirm no data loss (Kafka holds the events)

### Neo4j

- kill Neo4j
- observe Correlation Worker behavior — it should not crash, it should fail the write and retry
- restart; confirm graph is eventually consistent

### Alert Evaluator leader

- kill the leader
- confirm a follower acquires the lease within one TTL interval
- confirm no duplicate alerts are emitted during the transition

### Replay

- reset a consumer group offset to an earlier point
- replay events through the pipeline
- confirm all writes remain idempotent (no duplicate rows, no duplicate alerts, no Redis regression)

### Duplicate delivery

- produce the same Kafka event twice
- confirm exactly one row in `position_history`, one Redis write, one Neo4j edge

---

## Learning Goals

- fault isolation: which failures cascade and which are contained
- graceful degradation vs hard failure
- consumer lag as a leading indicator of pipeline health
- the difference between data loss and processing delay
- why idempotency makes replay safe
- distributed debugging without a unified trace

---

## Exit Criteria

- every service has a `/health` endpoint that reports dependency status
- structured logs make it possible to trace a single entity's event through the pipeline
- the developer can answer "is the pipeline working?" using only CLI tools and logs
- all failure lab experiments have been run and the developer can explain what happened and why
