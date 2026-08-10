# US-06: Composite Correlated Alert

**Actor:** Operator
**Status:** Defined

---

## Story

As an operator, I want an unscheduled proximity event and a related signal loss involving the same entity to be correlated into a single composite incident so that I have one elevated, clearly linked alert rather than two disconnected weak signals.

---

## Acceptance Criteria

- SIGNAL_LOSS is emitted immediately when an entity goes dark — it is never held back waiting for potential future correlation
- When a proximity event arrives within `COMPOSITE_CORRELATION_WINDOW_MS` of a signal loss episode involving the same entity (whether the entity is still dark or has recently resumed), a COMPOSITE alert (ELEVATED) is emitted
- The COMPOSITE alert atomically supersedes the SIGNAL_LOSS: the SIGNAL_LOSS status is set to `SUPERSEDED` in the same DB transaction that inserts the COMPOSITE record
- After correlation, the operator has one active incident (COMPOSITE, ELEVATED). The superseded SIGNAL_LOSS appears in the evidence/history view linked from the composite
- If no signal loss correlation exists when proximity arrives: emit UNSCHEDULED_PROXIMITY as normal
- If signal loss occurs with no prior or subsequent proximity within the correlation window: SIGNAL_LOSS remains active and is never superseded

---

## Example

```
Vessel A comes within threshold of Vessel B (no prior relationship)
    → Proximity episode begins
    + (within COMPOSITE_CORRELATION_WINDOW_MS)
Vessel B goes dark
    → SIGNAL_LOSS alert emitted immediately
    → Proximity episode candidate arrives
    → Alert Evaluator checks: alert-state:vessel_b exists (active dark)
    → COMPOSITE alert emitted (ELEVATED)
    → SIGNAL_LOSS marked SUPERSEDED
    = Operator sees: one COMPOSITE (active, elevated) + superseded SIGNAL_LOSS in evidence view

OR:

Vessel B goes dark
    → SIGNAL_LOSS alert emitted
Vessel B resumes (within COMPOSITE_CORRELATION_WINDOW_MS)
    → recent-loss:vessel_b written with TTL
Vessel A then comes close to Vessel B
    → recent-loss:vessel_b exists → COMPOSITE emitted, SIGNAL_LOSS superseded
```

---

## Flow Diagrams

### Signal Correlation

![Signal Correlation](../../../diagrams/docs/use-cases/US-06-composite-alert/signal-correlation.svg)

When a `proximity.candidates` event (one per episode) arrives, the alert evaluator checks `alert-state:{entity_id}` (active dark) OR `recent-loss:{entity_id}` (was dark, now back online) for both entities. If a signal loss correlation is found within the window, it performs a targeted Neo4j lookup for relationship context and emits a COMPOSITE alert with `supersedes_alert_ids`.

### Composite Emission

![Composite Emission](../../../diagrams/docs/use-cases/US-06-composite-alert/composite-emission.svg)

The API atomically inserts the COMPOSITE alert and marks the SIGNAL_LOSS as `SUPERSEDED` (with `superseded_by = composite_alert_id`) in one DB transaction. Two `alert-events` messages are broadcast: `ALERT_CREATED` for the composite and `ALERT_SUPERSEDED` for the old alert. The dashboard shows COMPOSITE as the active incident and links the superseded SIGNAL_LOSS in the evidence view.

### Single Signal Path

![Single Signal Path](../../../diagrams/docs/use-cases/US-06-composite-alert/single-signal-path.svg)

Shows what happens when only one condition is present: no composite, only the individual alert. SIGNAL_LOSS with no proximity correlation stays active until the operator resolves it.

---

## Architectural Justification

Justifies: [ADR-003 - Neo4j for Entity Relationship Graph](../../adr/ADR-003-neo4j-entity-graph.md)

Correlation across multiple weak signals requires traversing entity relationships across time - querying which entities were near which other entities, whether those entities have prior relationships, and whether the timing matches a signal loss window. This is a multi-hop graph traversal problem. A relational model would require multiple self-joins with time-window conditions, which grows expensive as relationship density increases. Neo4j expresses this as a Cypher pattern match directly on the graph structure.
