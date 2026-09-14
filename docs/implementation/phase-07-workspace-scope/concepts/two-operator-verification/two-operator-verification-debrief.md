# Two-Operator Verification — Checkpoint Debrief (CP6)

Real evidence for CP6, checked live on 2026-09-14. This is Phase 07's own stated exit test: "two operators with different geographic workspaces receive different alert sets."

---

## Two genuinely different real operators

Not simulated — confirmed directly against the `users` table:

```
user_id                              | email                            | google_sub
ec2e7a28-c38e-4b95-8d69-b77b2feed018 | buffdenslays@gmail.com           | 118129412930166345033
35310fa2-0ab1-4027-8cb1-39059b3712b9 | harshwardhanpatil2310@gmail.com  | 103747600691113387446
```

Two different Google accounts, two different `google_sub` values, two different durable identities — not one account logged in twice.

## Two different saved scopes, two different real results

| Operator | Saved scope | Alerts matching, per an independent Postgres count | Alerts shown live in that operator's own browser session |
| --- | --- | --- | --- |
| `harshwardhanpatil2310@gmail.com` | United States + `UNSCHEDULED_PROXIMITY` | 2724 | ~2724 |
| `buffdenslays@gmail.com` | Western Europe + `SIGNAL_LOSS`/`UNSCHEDULED_PROXIMITY` | 0 | 0 |

The independent count came from a raw SQL query evaluating each scope's bounds against every real alert's actual position — not from asking the API to grade its own homework. All 2724 real `UNSCHEDULED_PROXIMITY` alerts sit at longitude −122.5 to −121.5 (the ingestion-poller's actual San Francisco Bay Area coverage); Western Europe's bounds (−10 to 30) never come close, so `buffdenslays`'s empty result is correct filtering, not a bug — the second operator's "no data" report during this checkpoint was exactly this, confirmed rather than assumed.

## What this proves about the checkpoints underneath it

CP6 didn't test any new code — it's the first time CP1 (persistence), CP2 (REST filtering), CP4 (reconnect), and CP5 (the actual UI) were all exercised together, by two real independent people, at the same time, against real production-scale data (2945 total alerts). Nothing about the individual checkpoints changed; this is the vertical slice they were building toward.

---

## One honest gap, not hidden

This test exercised the **REST-seeded initial view** (`GET /alerts`, loaded when each operator's `AlertWidget` mounts) — both operators' `AlertWidget`s loaded their scoped result directly from Postgres. It did not exercise a **new** alert arriving over the live `alert-events` WebSocket push while both operators were connected, because the full ingestion pipeline (position-consumer, correlation-worker, alert-evaluator) wasn't running for this test — only `api` and `dashboard` were up, by deliberate choice after an earlier memory-pressure incident.

This isn't a blind spot in the code path, though: CP3's own integration tests already proved the exact `matchesScope`-based WebSocket fan-out logic against a real HTTP+WebSocket server, real Redis pub/sub, and real Postgres-backed scope loads — the same code that would run for a live-flowing alert runs identically whether the `alert-events` message came from a genuine detection or a test's `redis.publish()` call. What CP6 adds on top is the REST path's live, human-driven, multi-operator confirmation; a full live pipeline run would add visual confidence for the push path specifically, but wouldn't exercise different code than what's already been proven.

---

## Phase 07 exit criteria, checked against this evidence

| Criterion (from the original phase plan) | Result |
| --- | --- |
| Two users with different geographic workspaces receive different alert sets | PASS — this checkpoint |
| Out-of-scope REST queries do not leak data | PASS — CP2, proven against 2945 real rows |
| Reconnect restores saved workspace | PASS — CP3 (load-at-connection-open) + CP4 (forced reconnect) |
| Changing workspace updates subsequent WebSocket filtering | PASS — CP4, reconnect race reproduced and fixed under test |
| Changing workspace scope in the UI visibly changes what the feed shows, without a page reload | PASS — CP5, confirmed live (edit → save → feed changes, same page) |
| Authenticated users have durable workspace configuration | PASS — CP1 |
| REST and WebSocket data scoped server-side | PASS — CP2 (live-proven), CP3 (test-proven, see gap above) |
| Visibility rules consistent across reconnects | PASS — CP4 |
| Operator can view and change their own scope through the dashboard | PASS — CP5, including a real bug found and fixed via the live click-through |

**Phase 07 exit: substantively complete**, with one explicitly-acknowledged gap (live full-pipeline WebSocket push demonstration) that doesn't correspond to unproven code, only to an unexercised deployment configuration.
