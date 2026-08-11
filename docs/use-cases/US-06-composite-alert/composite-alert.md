# US-06: Composite Correlated Alert

**Actor:** Operator
**Status:** Defined

---

## Story

As an operator, I want a signal-loss episode and a related unscheduled-proximity episode involving the same entity to be correlated into one elevated incident so that I see the combined evidence rather than disconnected weak signals.

---

## Acceptance Criteria

- SIGNAL_LOSS is emitted immediately when an entity goes dark; it is never held back waiting for later correlation.
- `proximity.candidates` contains only new unscheduled proximity episodes because `KNOWN_ASSOCIATE` filtering already occurred in the Correlation Worker.
- When a proximity candidate falls within `COMPOSITE_CORRELATION_WINDOW_MS` of an active or recent signal-loss episode involving either member of the pair, the Alert Evaluator emits a COMPOSITE alert.
- The Alert Evaluator determines composite eligibility from the candidate plus Redis `alert-state` / `recent-loss`; it does not query Neo4j.
- Each signal-loss episode can be upgraded into at most one COMPOSITE incident.
- If no qualifying signal-loss state exists, the same candidate produces UNSCHEDULED_PROXIMITY.
- The API atomically persists the COMPOSITE and marks referenced active individual alerts (`NEW` or `ACKNOWLEDGED`) `SUPERSEDED`.
- `RESOLVED` individual alerts are terminal and are not retroactively superseded.

---

## Example

```text
Vessel B goes dark
  → SIGNAL_LOSS emitted immediately
  → alert-state:B records dark_since_ms + signal_loss_alert_id

Vessel A later enters exact proximity with Vessel B
  → Correlation Worker checks KNOWN_ASSOCIATE
  → pair is unscheduled
  → proximity.candidates published

Alert Evaluator receives candidate
  → finds qualifying alert-state:B
  → emits COMPOSITE
  → marks composite_issued=1 for that loss episode

API consumes COMPOSITE
  → INSERT composite
  → UPDATE referenced active SIGNAL_LOSS to SUPERSEDED
  → commit atomically
```

If Vessel B resumes before the proximity candidate arrives, the Position Consumer writes `recent-loss:B` before deleting `alert-state:B`. The Alert Evaluator can still correlate within the bounded TTL and consumes the recent-loss opportunity after successful composite emission.

---

## Flow Diagrams

### Signal Correlation

![Signal Correlation](../../../diagrams/docs/use-cases/US-06-composite-alert/signal-correlation.svg)

The candidate event is already an unscheduled proximity fact. The evaluator checks both pair members for active/recent signal loss and chooses COMPOSITE or UNSCHEDULED_PROXIMITY without a second graph lookup.

### Composite Emission

![Composite Emission](../../../diagrams/docs/use-cases/US-06-composite-alert/composite-emission.svg)

The API performs durable composite insertion and active-alert supersession in one database transaction.

### Single Signal Path

![Single Signal Path](../../../diagrams/docs/use-cases/US-06-composite-alert/single-signal-path.svg)

Signal loss without qualifying proximity remains SIGNAL_LOSS. Unscheduled proximity without qualifying signal loss becomes UNSCHEDULED_PROXIMITY.

---

## Architectural Justification

Justifies: [ADR-014 - Hybrid Input Model for the Alert Evaluator](../../adr/ADR-014-alert-evaluator-hybrid-input-model.md)

Neo4j still owns relationship evidence and is used by the Correlation Worker to determine whether a pair is a known associate. Once an unscheduled candidate is published, composite interpretation is a Kafka + Redis problem: the proximity fact is in the event and signal-loss episode state is in Redis.
