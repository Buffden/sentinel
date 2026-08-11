# ADR-014: Hybrid Input Model for the Alert Evaluator

**Status:** Accepted
**Date:** 2026-08-09

---

## Context

The Alert Evaluator was originally designed to pull directly from multiple persistence stores to detect every anomaly. That made one service responsible for too much raw fact discovery and coupled it to unrelated storage models.

Some anomaly inputs naturally fit event streams, while signal loss does not: an entity that stops broadcasting produces no event announcing its absence.

This ADR defines a hybrid model where dedicated services publish derived facts to Kafka and the Alert Evaluator owns only anomaly-rule state and final alert decisions.

---

## Decision

The Alert Evaluator receives:

- **Signal loss input:** scheduled Redis scan over `entity:live:*` because silence is an absence of events.
- **Route deviation input:** `deviation.candidates` from the stateless Deviation Detector.
- **Proximity input:** `proximity.candidates` from the Correlation Worker.
- **Composite correlation state:** Redis `alert-state:{entity_id}` and `recent-loss:{entity_id}`.

The Alert Evaluator does **not** query Neo4j as part of alert evaluation.

The Correlation Worker owns graph-level proximity fact discovery and `KNOWN_ASSOCIATE` filtering. Therefore, every `proximity.candidates` event already means:

1. an exact-distance proximity episode was detected;
2. the pair is not a `KNOWN_ASSOCIATE`;
3. the event contains the canonical pair, `episode_start_ms`, detection location, and distance needed by the alert rule.

Neo4j remains the durable relationship/evidence store and is read by the API during investigation.

---

## Deviation Detector

`services/deviation-detector/` consumes `position.normalized`, reads the assigned reference route for synthetic entities, and publishes one classification per eligible ping.

| Direction | What |
|---|---|
| Consumes | `position.normalized` |
| Reads | TimescaleDB `route_references` + `route_reference_points` |
| Publishes | `deviation.candidates` |

Contract:

- stateless per ping;
- publishes `OUT_OF_RANGE` or `IN_RANGE`;
- does not own sustained-deviation episode state;
- skips entities without a v1 reference route;
- does not emit alerts.

The Alert Evaluator owns sustained-deviation state in `deviation-state:{entity_id}`.

---

## Correlation Worker

The Correlation Worker consumes `position.normalized`, uses Redis H3 live indexes to reduce candidate pairs, performs exact-distance checks, records `PROXIMITY_EVENT` evidence in Neo4j, and checks `KNOWN_ASSOCIATE` before publishing.

For known associates:

```text
record/refresh proximity episode evidence
→ no proximity.candidates event
```

For unscheduled pairs:

```text
record proximity episode evidence
→ publish one proximity.candidates event per episode
```

The published event carries:

```text
pair_key
entity_a_id
entity_b_id
episode_start_ms
lat
lon
distance_at_detection
```

No additional Neo4j lookup is required downstream to decide whether the episode is unscheduled or to recover its start time.

---

## Alert Evaluator

| Detection | Input | Evaluator state/dependency |
|---|---|---|
| Signal Loss | scheduled Redis scan | `entity:live:*`, `alert-state:*`, TimescaleDB `position_history` for last-known payload |
| Route Deviation | `deviation.candidates` | `deviation-state:*` |
| Unscheduled Proximity | `proximity.candidates` | no graph lookup |
| Composite | `proximity.candidates` | `alert-state:*` and `recent-loss:*` |

### Composite rule

When `proximity.candidates` arrives, check both entities for qualifying signal-loss context.

**Active-dark path:**
- qualifying `alert-state:{entity_id}` exists;
- proximity `episode_start_ms` falls within `COMPOSITE_CORRELATION_WINDOW_MS` of `dark_since_ms`;
- `composite_issued == 0`;
- emit COMPOSITE and set `composite_issued=1`.

**Recent-loss path:**
- qualifying `recent-loss:{entity_id}` exists;
- proximity episode falls within the configured correlation window;
- emit COMPOSITE and consume the `recent-loss` marker.

If neither path qualifies, emit `UNSCHEDULED_PROXIMITY`.

The `proximity.candidates` event already proves that the pair is unscheduled, so the evaluator does not re-check `KNOWN_ASSOCIATE` in Neo4j.

---

## Why Signal Loss Remains a Direct Redis Read

Signal loss is the detection of an absence of events. A stream-only design would require artificial heartbeat/timer machinery without improving correctness. A scheduled scan of `last_seen_ms` is simpler and accurately models the problem.

---

## Why Neo4j Is Not an Alert-Evaluator Dependency

Neo4j is still essential, but its responsibility is different:

- Correlation Worker writes/query-checks relationship evidence while discovering proximity facts.
- API reads graph evidence for operator investigation.
- Alert Evaluator consumes the already-qualified proximity fact from Kafka and combines it with Redis anomaly state.

Removing the evaluator's graph read eliminates duplicated `KNOWN_ASSOCIATE` checks, reduces service coupling, and makes composite-rule tests independent of Neo4j availability.

---

## Kafka Topics

| Topic | Producer | Consumer | Retention | Purpose |
|---|---|---|---|---|
| `deviation.candidates` | Deviation Detector | Alert Evaluator | 1h | Per-ping route classification |
| `proximity.candidates` | Correlation Worker | Alert Evaluator | 1h | One unscheduled proximity fact per episode |

These are transient derived facts. Durable source/evidence remains in TimescaleDB and Neo4j.

---

## Consequences

- Alert Evaluator depends on Redis, Kafka, and a narrow TimescaleDB read for signal-loss payload construction; it does not depend on Neo4j.
- Correlation Worker is the single owner of `KNOWN_ASSOCIATE` filtering before `proximity.candidates` publication.
- `proximity.candidates` must contain enough immutable episode data for proximity/composite alert creation without a graph round-trip.
- Neo4j remains available to the API investigation path.
- Service-level Neo4j failure cannot block route-deviation or composite rule evaluation after a valid proximity candidate has already been published.
