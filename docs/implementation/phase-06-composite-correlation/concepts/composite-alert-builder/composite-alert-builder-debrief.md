# Composite Alert Builder Debrief

CP4, commit `3ce8844`.

---

## Setup

No infrastructure required: `buildCompositeAlert` is pure (no Redis, no Kafka, no config, no clock).

```bash
cd services/alert-evaluator
```

---

## Experiment 1: automated suite

```bash
npx vitest run --exclude '**/*.integration.test.ts'
```

```text
 Test Files  2 passed (2)
      Tests  16 passed (16)
   Start at  21:12:04
   Duration  107ms
```

7 new tests in `composite.test.ts`, covering every scenario required:

| Test | Proves |
| --- | --- |
| builds the literal nested payload shape end-to-end | Matches `DATA_MODEL.md`'s "nested signal-loss + proximity evidence" verbatim |
| identical explicit inputs produce byte-for-byte identical output | The core purity guarantee |
| `entity_id`/`counterparty_entity_id` follow `selected_entity_id`, for either pair member | Primary/counterparty assignment doesn't hardcode which side qualified |
| `RECENT` `loss_source`/`resumed_at_ms` pass through unchanged | `ACTIVE` isn't the only path exercised |
| `priority` is `ELEVATED` | `DATA_MODEL.md`'s priority-by-alert_type mapping; COMPOSITE reads as one elevated incident (US-06), not a standard-priority signal |
| `alert_id` derives from `dark_since_ms`, not `episode_start_ms` | Identity anchors to the loss episode being upgraded |
| throws when `selected_entity_id` isn't a member of the candidate pair | Fail-closed mismatch guard |

`leader.test.ts`'s existing 9 tests are unaffected (16 total, unchanged from before this checkpoint plus the 7 new ones).

**Correction after external review:** the first implementation pass hardcoded `priority: 'STANDARD'`, mirroring the adjacent `SIGNAL_LOSS`/`UNSCHEDULED_PROXIMITY` builders in `evaluator.ts` without checking whether that pattern actually applied to `COMPOSITE`. `DATA_MODEL.md` defines `priority` as `STANDARD` or `ELEVATED` but, at the time, stated no per-`alert_type` mapping; `ELEVATED` was unused anywhere in the codebase. US-06 (`docs/use-cases/US-06-composite-alert/composite-alert.md`) states the story explicitly: "correlated into one elevated incident." `DATA_MODEL.md` now documents the mapping explicitly (`SIGNAL_LOSS`/`UNSCHEDULED_PROXIMITY` are `STANDARD`, `COMPOSITE` is `ELEVATED`, `ROUTE_DEVIATION` undecided pending its own implementation) so this is an accepted contract, not an implicit code-level default.

---

## Experiment 2: manual inspection of real output

Ran `buildCompositeAlert` directly via `tsx` against representative inputs (not a test assertion, the actual object, printed):

```ts
const candidate: ProximityCandidateMessage = {
  pair_key: 'demo-entity-a:demo-entity-b',
  entity_a_id: 'demo-entity-a',
  entity_b_id: 'demo-entity-b',
  episode_start_ms: 1_700_000_030_000,
  lat: 51.5,
  lon: -0.1,
  distance_at_detection: 42.5,
};

const decision: CompositeCandidateDecision = {
  decision: 'COMPOSITE',
  candidate_id: `${candidate.pair_key}:${candidate.episode_start_ms}`,
  selected_entity_id: 'demo-entity-a',
  loss_source: 'ACTIVE',
  dark_since_ms: 1_700_000_000_000,
  signal_loss_alert_id: 'demo-entity-a:SIGNAL_LOSS:1700000000000',
  resumed_at_ms: null,
};

const alert = buildCompositeAlert(decision, candidate, 'aircraft', 1_700_000_031_000, 120_000);
console.log(JSON.stringify(alert, null, 2));
```

Observed:

```json
{
  "alert_id": "demo-entity-a:demo-entity-b:COMPOSITE:1700000000000",
  "entity_id": "demo-entity-a",
  "counterparty_entity_id": "demo-entity-b",
  "entity_type": "aircraft",
  "alert_type": "COMPOSITE",
  "priority": "ELEVATED",
  "status": "NEW",
  "detected_at_ms": 1700000031000,
  "payload": {
    "signal_loss": {
      "dark_since_ms": 1700000000000,
      "loss_source": "ACTIVE",
      "resumed_at_ms": null,
      "signal_loss_alert_id": "demo-entity-a:SIGNAL_LOSS:1700000000000"
    },
    "proximity": {
      "pair_key": "demo-entity-a:demo-entity-b",
      "entity_a_id": "demo-entity-a",
      "entity_b_id": "demo-entity-b",
      "lat": 51.5,
      "lon": -0.1,
      "distance_metres": 42.5,
      "episode_start_ms": 1700000030000
    },
    "correlation_window_ms": 120000,
    "supersedes_alert_ids": [
      "demo-entity-a:SIGNAL_LOSS:1700000000000"
    ]
  }
}
```

Then a repeat call with the identical arguments, plus a deliberately mismatched decision:

```text
deterministic repeat matches: true
mismatch guard threw as expected: composite decision entity someone-else is not a member of candidate pair demo-entity-a:demo-entity-b
```

| Check | Expected | Observed |
| --- | --- | --- |
| `alert_id` is `{pair_key}:COMPOSITE:{dark_since_ms}` | yes | PASS |
| Payload is nested (`signal_loss` / `proximity` sub-objects) | yes | PASS |
| `entity_type` absent from both nested sub-objects | yes | PASS |
| `priority` is `ELEVATED` | yes | PASS |
| `entity_id` = `selected_entity_id`, counterparty = the other pair member | yes | PASS |
| Repeated call with identical arguments is byte-for-byte identical | yes | PASS |
| Mismatched `selected_entity_id` throws rather than guessing | yes | PASS |

---

## Engineering debrief

**Data flow:** `buildCompositeAlert(decision, candidate, entityType, detectedAtMs, correlationWindowMs)` resolves `counterparty_entity_id` by comparing `decision.selected_entity_id` against the candidate's two pair members, then assembles the alert object directly from its five arguments, with no intermediate Redis or Kafka calls.

**Trade-off:** keeping this pure means CP5A (the caller) takes on the responsibility of supplying `entityType` (an `entity:live:*` read), `detectedAtMs`, and `correlationWindowMs` (config), and of doing so consistently across a Kafka redelivery. CP4 itself cannot enforce that consistency; it can only guarantee that *given* consistent inputs, the output is consistent too. That's the intended boundary: a pure, easily-tested core with the operational concerns pushed to the one place (CP5A) that's already responsible for redelivery-safe orchestration.

**Failure behaviour:** the only way this function fails is a `selected_entity_id` that isn't a member of the candidate pair, an invariant violation between two inputs that should never disagree if CP2/CP3C's identity discipline held upstream. It throws rather than defaulting to a guessed counterparty, the same fail-closed posture as `CandidateDecisionConflictError`.

## Manual inspection commands

```bash
cd services/alert-evaluator
npx vitest run src/composite.test.ts
```

## Knowledge-check questions

1. Why does `buildCompositeAlert` take `detectedAtMs` as a parameter instead of calling `Date.now()` internally?
2. Why is `entity_type` excluded from the nested payload even though it's easy to include?
3. What would happen downstream if the counterparty-mismatch guard silently picked a default instead of throwing?
4. Can `buildCompositeAlert` be called with an `UNSCHEDULED_PROXIMITY` decision? Why or why not, at the type level?

## Optional manual tweak

Change the demo script's `episode_start_ms` to a different value and confirm `alert_id` doesn't change; it should depend only on `pair_key` and `dark_since_ms`, never on the candidate's own episode timing.

## Next

CP5A: wire CP2 → CP3B → CP3C → CP4 into `handleProximityCandidate`, publish `COMPOSITE`/`UNSCHEDULED_PROXIMITY` to Kafka, and order decision-record cleanup after the candidate-consumer session's own `commitOffsets()` succeeds.

---

## Key observations

| Concept | Observed |
| --- | --- |
| Automated suite | 16/16 PASS, 7 new tests |
| Real output inspection | Nested payload, correct identity, correct primary/counterparty, no `entity_type` duplication |
| Determinism | Repeated call with identical arguments produced byte-for-byte identical JSON |
| Fail-closed guard | Mismatched `selected_entity_id` threw with a clear message rather than producing a wrong-but-valid-looking alert |
