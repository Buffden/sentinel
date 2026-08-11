# US-06: Composite Correlated Alert

**Actor:** Operator
**Status:** Defined

---

## Story

As an operator, I want an unscheduled proximity event and a related signal loss involving the same entity to be correlated into a single composite incident so that I have one elevated, clearly linked alert rather than two disconnected weak signals.

---

## Acceptance Criteria

- SIGNAL_LOSS is emitted immediately when an entity goes dark; it is never held back waiting for future correlation.
- `proximity.candidates` contains only unscheduled pairs because the Correlation Worker already filtered `KNOWN_ASSOCIATE` relationships.
- When a proximity episode occurs within `COMPOSITE_CORRELATION_WINDOW_MS` of a qualifying signal-loss episode involving either entity, the Alert Evaluator emits a COMPOSITE alert with `ELEVATED` priority.
- Composite correlation uses the immutable proximity event plus Redis `alert-state:*` / `recent-loss:*`; the Alert Evaluator does not query Neo4j.
- The COMPOSITE atomically supersedes the referenced active SIGNAL_LOSS alert in the API persistence transaction.
- Each signal-loss episode can be upgraded into at most one COMPOSITE incident.
- If no qualifying loss context exists, the same proximity candidate produces UNSCHEDULED_PROXIMITY.
- If signal loss occurs with no qualifying proximity, SIGNAL_LOSS remains the active incident.

---

## Example

```text
Vessel B goes dark
→ SIGNAL_LOSS emitted immediately

Vessel A later enters an unscheduled proximity episode with Vessel B
→ Correlation Worker has already proven proximity and filtered known associates
→ proximity.candidates arrives with pair + episode_start_ms + location + distance
→ Alert Evaluator finds B's active/recent loss state in Redis
→ COMPOSITE emitted
→ API persists COMPOSITE and marks SIGNAL_LOSS SUPERSEDED atomically
```

---

## Flow Diagrams

### Signal Correlation

![Signal Correlation](../../../diagrams/docs/use-cases/US-06-composite-alert/signal-correlation.svg)

A qualified `proximity.candidates` event triggers Redis loss-state checks for both entities. A qualifying active or recent loss produces COMPOSITE; otherwise the event produces UNSCHEDULED_PROXIMITY.

### Composite Emission

![Composite Emission](../../../diagrams/docs/use-cases/US-06-composite-alert/composite-emission.svg)

The API atomically inserts the COMPOSITE and marks referenced active individual alerts `SUPERSEDED`, then broadcasts lifecycle events.

### Single Signal Path

![Single Signal Path](../../../diagrams/docs/use-cases/US-06-composite-alert/single-signal-path.svg)

If only one condition is present, the corresponding individual alert remains independent.

---

## Architectural Justification

Justifies: [ADR-014 - Hybrid Input Model for the Alert Evaluator](../../adr/ADR-014-alert-evaluator-hybrid-input-model.md)

The Correlation Worker owns graph-level fact discovery and known-associate filtering. The Alert Evaluator owns anomaly-rule composition. Passing an immutable qualified proximity fact through Kafka keeps those responsibilities separate: Neo4j remains the durable graph/evidence store, while composite evaluation depends only on the candidate event plus Redis loss episode state.
