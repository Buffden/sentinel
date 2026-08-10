# US-06: Composite Correlated Alert

**Actor:** Operator
**Status:** Defined

---

## Story

As an operator, I want an unscheduled proximity event and a related signal loss involving the same entity to be correlated into a single composite incident so that I have one elevated, clearly linked alert rather than two disconnected weak signals.

---

## Acceptance Criteria

- SIGNAL_LOSS is emitted immediately when an entity goes dark — it is never held back waiting for potential future correlation
- When an unscheduled proximity event arrives within `COMPOSITE_CORRELATION_WINDOW_MS` of a signal loss episode involving the same entity (whether the entity is still dark or has recently resumed), a COMPOSITE alert (ELEVATED) is emitted
- The COMPOSITE alert atomically supersedes the SIGNAL_LOSS; the operator sees one active incident, not two
- Each signal-loss episode can be upgraded into at most one COMPOSITE incident — a later proximity event for the same entity produces an independent UNSCHEDULED_PROXIMITY alert, not a second composite
- If no signal loss correlation exists when proximity arrives: UNSCHEDULED_PROXIMITY is emitted as normal
- If signal loss occurs with no qualifying proximity within the window: SIGNAL_LOSS remains active

---

## Example

```
Vessel B goes dark
    → SIGNAL_LOSS alert emitted immediately

Vessel A comes within threshold of Vessel B (no prior relationship)
within COMPOSITE_CORRELATION_WINDOW_MS of Vessel B going dark
    → Alert Evaluator finds signal loss for Vessel B
    → COMPOSITE alert emitted (ELEVATED)
    → SIGNAL_LOSS marked SUPERSEDED
    → Operator sees: one COMPOSITE (active, elevated)

OR:

Vessel B goes dark → SIGNAL_LOSS emitted
Vessel B resumes (within COMPOSITE_CORRELATION_WINDOW_MS)
    → correlation opportunity recorded temporarily
Vessel A then comes close to Vessel B
    → correlation opportunity found → COMPOSITE emitted, SIGNAL_LOSS superseded
    → correlation opportunity consumed; further proximity produces independent alerts
```

---

## Flow Diagrams

### Signal Correlation

![Signal Correlation](../../../diagrams/docs/use-cases/US-06-composite-alert/signal-correlation.svg)

When a `proximity.candidates` event arrives, the Alert Evaluator checks for an active signal loss (entity still dark) or a recent signal loss within the correlation window (entity resumed). If found, it checks that the composite has not already been issued for this signal-loss episode, then emits a COMPOSITE alert. The correlation opportunity is consumed so that later proximity events for the same entity produce independent alerts.

### Composite Emission

![Composite Emission](../../../diagrams/docs/use-cases/US-06-composite-alert/composite-emission.svg)

The API atomically inserts the COMPOSITE alert and marks the SIGNAL_LOSS as `SUPERSEDED` in one DB transaction. Both updates are broadcast to all API instances via the `alert-events` channel so every WebSocket connection receives the change.

### Single Signal Path

![Single Signal Path](../../../diagrams/docs/use-cases/US-06-composite-alert/single-signal-path.svg)

When only one condition is present — signal loss with no qualifying proximity, or proximity with no matching signal loss — only the individual alert is emitted. No composite is produced.

---

## Architectural Justification

Justifies: [ADR-003 - Neo4j for Entity Relationship Graph](../../adr/ADR-003-neo4j-entity-graph.md)

Correlation across multiple weak signals requires traversing entity relationships across time. Neo4j expresses "is this pair related?" and "what was their proximity history?" as Cypher pattern matches directly on the graph. The composite lookup targets a specific entity pair — it is a point query, not a scan.
