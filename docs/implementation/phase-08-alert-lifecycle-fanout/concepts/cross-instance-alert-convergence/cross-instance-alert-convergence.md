# Cross-Instance Alert Convergence — Design and Learning Reference

Plain language first, then technical depth, then the code. Use this to understand, inspect, and defend CP4, the phase file's own "Key Experiment."

---

## What this checkpoint is, and deliberately isn't

CP4 proves the two guarantees the phase file names explicitly: an ACK/Resolve through one API instance converges on every connected client, including one attached to a *different* instance; and a crash between the Postgres commit and the `alert-events` publish is recoverable, not a lost transition. It builds no new production mechanism — the fan-out itself (Redis pub/sub → every subscribed instance → each instance's own local WebSocket clients) has existed since Phase 03/07. CP4's job is entirely verification, plus whatever the verification finds broken.

It found something broken: the async-error-handling "fix" made during CP1–CP3's own testing never actually worked. Fixing that for real is part of this checkpoint's real scope, not a detour from it — a checkpoint whose own crash-recovery test can't get past a hung request isn't actually testing crash recovery.

---

## Concepts in plain language

### Why two in-process test servers are a faithful stand-in for two real instances

Each call to `attachWebSocketServer(server)` creates its own `connections` Map (which sockets are attached to *this* server) and its own Redis subscriber connection (`new Redis(...)`, not shared). That's the entire isolation two real, separately-deployed API processes would have from each other — nothing links them directly. The only things genuinely shared between two real instances — Postgres and Redis — are exactly what these two in-process test servers also share (the same module-level `pool`/`redis` singletons). Spawning two literal `node` processes would prove the same thing at higher cost with no additional guarantee.

### Why the crash test mocks `redis.publish` instead of killing a process

"Crash between the DB commit and the publish" doesn't require an actual process crash to prove — it requires the DB transaction to have already committed while the publish never happens. Mocking `redis.publish` to throw exactly once, after `transitionAlert`'s own transaction has already returned, produces exactly that state deterministically and repeatably; killing and restarting a real process would introduce timing nondeterminism for no added fidelity.

### The real bug this checkpoint caught: Express 4 doesn't auto-forward async rejections

`index.ts` had an error-handling middleware (`app.use((err, req, res, next) => ...)`) added during CP1–CP3's own debugging. It looked like a fix and was never re-broken — it just never worked, because nothing in any route handler ever called `next(err)`. An `async (req, res) => { await ... }` that rejects in Express 4 produces an unhandled promise rejection Express never sees; the request simply hangs until the client times out. This had gone undetected because the test fixture bug that first surfaced it (missing `users` rows for the FK) was fixed by correcting the *test data*, which made the error path stop being exercised at all — the broken error-handling middleware was never actually re-tested against a real thrown error until CP4's crash-injection test hit it again, on purpose this time.

### Why the fix is a wrapper applied everywhere, not a `try`/`catch` in one handler

The gap wasn't specific to the `PATCH` handler — every async Express handler in the service (`GET /alerts`, both workspace routes, `GET /entities/live`) had the identical exposure; `PATCH` was just the first one a test actually drove through a real thrown error. `asyncHandler(fn)` wraps a handler once so its rejection is explicitly forwarded to `next(err)`, and was applied to all of them, not only the one this checkpoint happened to be testing — leaving the others unfixed would mean the same bug resurfaces the next time any of them throws for real.

---

## Map to code

| Concept | Where it lives |
| --- | --- |
| The real fix: forward async rejections to Express | `services/api/src/shared/asyncHandler.ts`, applied in `alerts.ts`, `workspace.ts`, `entitiesLive.ts` |
| Two-instance convergence + crash-before-publish proof | `services/api/src/ws/multiInstanceLifecycle.integration.test.ts` |
| The mechanism being proven (unchanged by this checkpoint) | `services/api/src/ws/wsServer.ts` (per-instance `connections` map + Redis subscriber) |

---

## Retention questions

1. Why do two in-process `attachWebSocketServer` instances prove the same thing two real deployed processes would, rather than being a weaker stand-in?
2. Why does simulating "crash before publish" only require mocking `redis.publish`, not an actual process kill?
3. Walk through exactly why the original `index.ts` error-handling middleware never fired, even though it was syntactically correct.
4. Why was the `asyncHandler` fix applied to every route, not just the one under test?
5. What specific test action in this checkpoint is what actually caught the broken error handling, and why didn't CP1–CP3's own tests catch it first?

---

## Completion checklist

- [ ] I can explain why Redis pub/sub, not any direct instance-to-instance call, is what makes cross-instance convergence work
- [ ] I can explain, from memory, why Express 4 needs `next(err)` called explicitly for an async handler's rejection
- [ ] I can point to all four route handlers `asyncHandler` now wraps and explain why each one needed it
- [ ] I have read the two-instance test file myself and can explain what each of its two tests actually proves
- [ ] I understand this checkpoint is verification-plus-bugfix, not new production mechanism
