# Correlation Window Semantics Debrief

Commit: `4952e95` — "docs: define composite correlation eligibility formula and tie-break"

This checkpoint is documentation-only — no application code changed, so there is no test suite to run. The evidence here is the committed diff itself and a direct check that no other accepted document still implies "TTL alone means eligible."

---

## Experiment 1: verify the TTL-superset inequality algebraically

Claim: `recent-loss`'s TTL can never expire before a genuinely eligible candidate's deadline.

```text
resumed_at_ms >= dark_since_ms                          (an entity must go dark before it resumes)

=> resumed_at_ms + WINDOW >= dark_since_ms + WINDOW      (add WINDOW to both sides)

   key expiry                  true eligibility deadline
```

Concrete counter-check (the regression this rule exists to prevent): an entity dark for 10x the window before resuming.

```text
dark_since_ms   = T
resumed_at_ms   = T + 10*WINDOW   (dark for a long stretch)
key expiry      = resumed_at_ms + WINDOW = T + 11*WINDOW   -- still alive
true deadline   = dark_since_ms + WINDOW = T + WINDOW       -- already passed

A candidate at T + 10*WINDOW + 1 (just after resume):
  key exists, PTTL > 0 (alive until T + 11*WINDOW)
  gap_ms = (T + 10*WINDOW + 1) - T = 10*WINDOW + 1 > WINDOW
  -> does NOT qualify, despite the key being genuinely live
```

This exact scenario became CP2's regression-guard test — see `composite-eligibility-resolution-debrief.md`.

---

## Experiment 2: grep for lingering "TTL = eligibility" language

```bash
grep -n "correlate within the bounded TTL\|TTL.*eligib" docs/use-cases/US-06-composite-alert/composite-alert.md docs/DATA_MODEL.md
```

`US-06`'s example section previously read "The Alert Evaluator can still correlate within the bounded TTL" — corrected to explicitly point at the `dark_since_ms` gap check instead, with a cross-link into `DATA_MODEL.md`'s new "Composite eligibility rule" section.

| Check | Expected | Observed |
| --- | --- | --- |
| `DATA_MODEL.md` states one formula for both `alert-state` and `recent-loss` | yes | PASS |
| `DATA_MODEL.md`'s `recent-loss` TTL line no longer implies TTL is the eligibility boundary | yes | PASS — reworded, cross-linked |
| `US-06` acceptance criteria include the both-qualify tie-break | yes | PASS — new bullet added |
| `US-06` example section no longer says "within the bounded TTL" without qualification | yes | PASS |
| Markdown cross-links between `US-06` and `DATA_MODEL.md#composite-eligibility-rule` resolve | yes | PASS (see phase README link-check) |

---

## Engineering debrief

**Data flow:** this is a specification decision, not a runtime one — it constrains what CP2 (and later checkpoints) are allowed to implement. `resolveEntityLossEpisode` (CP2) is the first code to actually execute this formula.

**Trade-off:** committing to `dark_since_ms` as the sole anchor is a stronger, simpler contract than allowing per-representation formulas, at the cost of ruling out ever using `resumed_at_ms` as a *fresh* eligibility window without a further, explicit architectural decision. That was a deliberate choice, not an oversight — see the concept note's "why `dark_since_ms`, not `resumed_at_ms`" section.

**Failure behaviour:** the risk this checkpoint exists to prevent isn't a runtime failure — it's a *design* failure: a future engineer "simplifying" the rule back to `EXISTS recent-loss => qualifies`, silently reintroducing the exact bug the TTL-superset proof rules out. The explicit inequality and the regression-guard test named in Experiment 1 are the permanent defenses against that.

## Manual inspection commands

```bash
# Re-read the canonical rule
sed -n '/Composite eligibility rule/,/^### /p' docs/DATA_MODEL.md

# Confirm the use-case doc cross-links correctly
grep -n "composite-eligibility-rule" docs/use-cases/US-06-composite-alert/composite-alert.md
```

## Knowledge-check questions

1. Derive the TTL-superset inequality from scratch: why must `resumed_at_ms >= dark_since_ms` always hold?
2. Walk through the "dark for 10x the window" example and explain why the key being alive is not sufficient.
3. Why was this resolved as a standalone documentation checkpoint instead of being folded into CP2's implementation commit?

## Optional manual tweak

Pick a different multiplier than 10x in the counter-check above (e.g. dark for exactly `WINDOW + 1ms`) and re-derive whether the candidate qualifies — confirms the boundary is exact, not approximate.

## Next

CP2 (`1efa70c`): implement `resolveEntityLossEpisode`, `selectWinningEpisode`, and `resolveCompositeEligibility` exactly against this rule — read-only, no Redis mutation, no Kafka emission.

---

## Key observations

| Concept | Observed |
| --- | --- |
| TTL-superset inequality | Holds algebraically; `resumed_at_ms >= dark_since_ms` is the only fact required |
| Regression scenario (dark 10x window, resume just before candidate) | Key alive, candidate correctly rejected by the explicit formula |
| `US-06` / `DATA_MODEL.md` cross-links | Both updated and consistent, no other doc still implies TTL-as-eligibility |
