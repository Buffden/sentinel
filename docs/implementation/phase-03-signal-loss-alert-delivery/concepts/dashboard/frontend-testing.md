# CP7j: Frontend Testing — Concepts and Implementation Overview

Covers why Vitest was added to the dashboard now, the module-resolution gotcha that came with
it, and the two distributed-system invariants this checkpoint set out to prove: position
staleness rejection and alert deduplication.

---

## Why now, and why Vitest

No test framework existed anywhere in the dashboard before this checkpoint. It became necessary
here specifically because CP7j's own exit proof is "duplicate / stale simulation passes" — a
claim that needs a repeatable, automated check, not a one-time manual click-through. The
dashboard's own concepts README already named the target precisely: "Implement as a pure
reducer ... and unit-test it heavily" for position monotonicity, and the "Most important CP7
tests" list names exactly these three behaviors — stale position ignored, newer position
accepted, duplicate `alert_id` produces one panel entry.

Vitest was the obvious choice: it's already the tool used identically across all four backend
services this session, so there was no new tooling decision to make, just extending an already-
proven pattern to a fifth service. `applyPositionUpdate` and `applyAlertUpdate` are both pure,
dependency-free reducer functions — no React, no DOM, no network — so this needed zero test
infrastructure beyond the runner itself (no jsdom, no React Testing Library, since nothing here
renders a component).

---

## The module-resolution gotcha

`tsconfig.json` maps `@/*` to `./src/*`, but Vitest (built on Vite) doesn't read `tsconfig`
path mappings on its own — every `@/entities/...`-style import in the test files would fail to
resolve without an explicit `resolve.alias` entry in `vitest.config.mts` pointing at the same
target.

The config file is `.mts`, not `.ts`: a plain `.ts` config loaded as CommonJS by Vite's native
config loader printed a deprecation warning, and more importantly `.mts`'s ESM semantics mean
`__dirname` doesn't exist — the config resolves its own directory via
`path.dirname(fileURLToPath(import.meta.url))` instead, the standard replacement.

---

## What was proven

**Position staleness rejection** (`entities/tracked-entity/model.test.ts`): a strictly newer
`eventTimeMs` is accepted and overwrites the changed fields; an older or *equal* `eventTimeMs`
is rejected — strictly-newer is required, not newer-or-equal, matching the same invariant
already proven on the backend for `entity:live:*`'s Lua guard. Rejection returns the exact same
`Map` reference (not just unchanged values), which is what lets `MapWidget` skip a re-render on
a stale frame. Also re-covers the CP7g merge-by-key fix: a WebSocket-shaped update that omits
`onGround`/`entitySubtype` preserves whatever REST hydration already established for those
fields instead of wiping them.

**Alert deduplication** (`entities/alert/model.test.ts`): delivering the identical `alert_id`
twice produces exactly one map entry, not two — CP7j's literal exit proof, simulating a
redelivered Kafka message (a crash before offset commit republishes identical content, and
receiving it twice must not double the panel). Unlike positions, alert dedup has no
staleness/monotonicity check at all: v1 has no acknowledge/resolve lifecycle (Phase 08) yet, so
a given `alert_id`'s content never changes after first publish — a plain upsert-by-key is
correct and sufficient.

Both were verified non-vacuous the same way the backend integration tests were earlier this
session: temporarily broke the guard under test (disabled the staleness check; made
`applyAlertUpdate` key by `id + Math.random()` instead of `id`), confirmed the relevant test
failed for the right reason, then reverted.

---

## What CP7j does not cover

Backend redelivery (a Kafka message actually arriving twice at the WebSocket layer) is not
simulated end-to-end here — that would need the API's alert sink and a real broker, already
covered by `alertSink.integration.test.ts` on the backend. This checkpoint proves the frontend
reducer's own dedup logic in isolation: given two identical `Alert` objects, the map ends up
with one entry. That's the piece that was actually unverified before this checkpoint.

---

## Manual inspection

```
cd services/dashboard
pnpm test
```

Or watch a single failure prove itself: temporarily change `applyAlertUpdate`'s
`next.set(incoming.id, incoming)` to key by something else, re-run, watch the dedup test fail,
revert.
