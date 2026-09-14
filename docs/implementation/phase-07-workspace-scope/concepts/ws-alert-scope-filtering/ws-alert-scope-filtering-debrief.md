# WebSocket Alert Scope Filtering — Checkpoint Debrief (CP3)

Real evidence for CP3, checked on 2026-09-13 against real Postgres, real Redis, and a real spun-up HTTP+WebSocket server, not mocks. See [`ws-alert-scope-filtering.md`](ws-alert-scope-filtering.md) for the mental model this checkpoint implements.

---

## Automated tests

`wsServer.integration.test.ts` — a real `http.Server`, a real `ws` client, real Redis pub/sub, and (new for this checkpoint) real Postgres for the workspace lookup:

```
Test Files  1 passed (1)
     Tests  13 passed (13)
```

The five tests that matter for this checkpoint specifically:

- an operator with no saved workspace receives nothing (fail-closed, not fail-open)
- an operator with a saved France scope receives an alert inside it and receives nothing for one outside it, over the same connection
- a demo connection filtered to its subscribed bbox receives an in-box alert and not an out-of-box one
- a demo connection with no subscribed bbox yet receives everything, unfiltered
- (pre-existing, updated) position-updates filtering is unaffected by any of this — proven by the position-bbox tests still passing unchanged

Full API suite: **93/93 passing.**

---

## An old test that had to change, not just be added to

Before this checkpoint, `wsServer.integration.test.ts` had a test literally titled "delivers to every connected client regardless of its position bbox," asserting that every connected client received every alert unconditionally. That assertion described the exact behavior this checkpoint exists to remove. Rather than leaving it to rot or silently deleting it, it was replaced with the five tests above, which assert the new, correct contract for the same scenarios (and then some) — a record that the old behavior was a deliberate target, not an oversight.

## Two real bugs caught while writing this checkpoint's own tests, not by inspection

**A genuine test-ordering leak.** Adding two new demo-role WebSocket tests ahead of the pre-existing "demo session lifecycle" tests in the same file broke a demo-count assertion two tests later — `ws.close()` on the client only starts the close handshake, and the server's own `close` handler (which decrements the shared demo counter) runs asynchronously afterward. `afterEach` was closing sockets but not waiting for the server side to catch up before the next test captured its baseline count. Fixed by adding one wait in `afterEach` itself, benefiting every test in the file, not just the new ones.

**Cross-test interference from a service I'd left running.** The full suite briefly showed two failures in an unrelated file (`alertSink.integration.test.ts`), each expecting exactly one published event and getting two. Reproduced in isolation, still failed — ruling out simple test-file parallelism. The actual cause: the real API service I'd started earlier for CP2's live browser verification was still running in the background, with its own Kafka consumer independently publishing to the same real `alert-events` Redis channel the test was listening on. This is a known, already-documented gotcha from Phase 06's exit-verification ("don't run the API's test suite while the real dev API service is also running"). Stopped the live service, reran — 93/93 passed — then restarted it afterward so the live dashboard session wasn't left broken.

---

## What this proves, and what it doesn't yet

Proves: the exact same `matchesScope` predicate CP2 proved against real Postgres data now correctly gates the live push path too, including the fail-closed connection-open race and demo's bbox reuse, all against a real running server process (not a handler function called directly).

Does **not** yet prove: this against the full live multi-service pipeline the way CP1 and CP2 were — that would need position-consumer, correlation-worker, and alert-evaluator running to generate real qualifying alert events, and those weren't relaunched this pass (only `api` and `dashboard` are up, by request, after the earlier memory event). The already-connected real operator session did successfully reconnect to the rebuilt `wsServer.ts` and re-sent its bbox subscribe message without incident, which is a live smoke test of the connection-handling path, just not of a real alert actually flowing through the new filter end to end.
