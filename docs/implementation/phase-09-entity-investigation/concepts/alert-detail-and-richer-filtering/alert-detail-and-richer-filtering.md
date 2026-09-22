# Alert Detail and Richer Filtering — Design and Learning Reference

Plain language first, then technical depth, then the code. Use this to understand, inspect, and defend CP5 (richer `GET /alerts` filtering + `GET /alerts/:alert_id`).

---

## What this checkpoint is, and deliberately isn't

CP5 closes the backend half of Phase 09 with two additions to the already-existing `GET /alerts`:

- `GET /alerts/:alert_id`: a single-alert investigation read, not restricted to `NEW`/`ACKNOWLEDGED` like the list.
- `GET /alerts?status=...&entity_id=...`: two new optional query params on the existing list, additive only.

It does not change `PATCH /alerts/:alert_id` (Phase 08's lifecycle transitions, untouched) and does not add any new datastore -- this checkpoint is entirely inside the `alerts` table CP1-CP4 already had access to, unlike CP1-CP4's progression through Redis, TimescaleDB, and Neo4j.

---

## Concepts in plain language

### Why "richer filtering" had to be proven not to change the default

`GET /alerts` already had one caller depending on its exact current behavior: the dashboard's `AlertWidget`, which calls it with no query params at all and expects exactly `NEW`/`ACKNOWLEDGED` back. Adding query params is safe by construction here specifically because both are optional and both fall back to today's exact behavior when absent (`status` defaults to `['NEW', 'ACKNOWLEDGED']`; `entity_id` defaults to no filter). This was verified twice: the full pre-existing test suite (which never passes either param) needed zero changes and still passes unmodified, and a real manual `curl` with no params confirmed the resolved alert seeded alongside a new one still doesn't appear by default.

### Why `?status=` validates against a known list instead of just passing whatever arrives to SQL

`status = ANY($1)` would happily accept `?status=NOT_A_REAL_STATUS` and silently return zero rows -- indistinguishable from "no alerts match," which would look like correct behavior to an operator who mistyped a status and never find out. Validating against `KNOWN_STATUSES` up front and returning `400` on anything else means a typo is loud, not silent.

### Why `GET /alerts/:alert_id` needed the same fail-closed scope check as the list, but not the entity endpoints' dark-entity fallback

Same reasoning CP2-CP4 already established for entities: any by-id endpoint is reachable directly, so it needs its own authorization check rather than trusting the caller went through the list first. What's simpler here: an alert row is never "dark" the way an entity can be -- it always carries its own `entity_type`, `alert_type`, and `payload` (with the position CP2's ADR-012-corrected field paths already know how to read). There's no fallback branch to write, because there's nothing missing to fall back from; `matchesScope(row, scope)` is a direct, single check.

### Why the by-id read isn't restricted to NEW/ACKNOWLEDGED

The list's status restriction exists for the live dashboard feed -- an operator watching for open work doesn't want closed alerts cluttering the view. Investigation is the opposite need: opening a specific alert's evidence panel (US-14) has to work whether that alert is `NEW`, `RESOLVED`, or `SUPERSEDED` by a later composite. The by-id endpoint therefore has no status restriction at all -- only the `alert_id = $1` lookup, then the same scope check every other read in this phase already applies.

---

## Map to code

| Concept | Where it lives |
| --- | --- |
| Status validation against a known list | `services/api/src/routes/alerts.ts` (`KNOWN_STATUSES`, `parseStatusFilter`) |
| Richer `GET /alerts`: optional `status`/`entity_id` params, default unchanged | `services/api/src/routes/alerts.ts` |
| `GET /alerts/:alert_id`: unrestricted-by-status read, same `matchesScope` fail-closed rule | `services/api/src/routes/alerts.ts` |
| Reused workspace-scope lookup (no new copy needed -- already shared from the CP1-CP3 hardening pass) | `services/api/src/shared/entityAccess.ts` (`fetchWorkspaceScope`) |
| Proof against real Postgres, including the unchanged-default guarantee | `services/api/src/routes/alerts.integration.test.ts` |

---

## Retention questions

1. Why is it safe to add two new query params to an endpoint the dashboard already depends on, and what specifically was checked to be sure?
2. Why does an invalid `?status=` value return `400` instead of an empty list?
3. Why doesn't `GET /alerts/:alert_id` need a "dark alert" fallback the way the entity by-id endpoints needed a dark-entity one?
4. Why is the by-id alert read unrestricted by status when the list read isn't?
5. What's the one piece of authorization logic every by-id endpoint in this phase (entities and alerts both) now shares, and where does it live?

---

## Completion checklist

- [ ] I can explain why both new query params default to today's exact behavior, and how that was verified rather than assumed
- [ ] I can explain why an unknown status value is a loud `400`, not a silent empty result
- [ ] I can explain the difference in "dark" handling between an entity by-id lookup and an alert by-id lookup
- [ ] I can trace why the by-id alert read has no status restriction while the list does
- [ ] I have run the real dev server myself and confirmed the default-unchanged claim against real seeded Postgres data, not just the test suite
