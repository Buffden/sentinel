# ADR-012: Workspace Scope and Server-Side Alert Filtering

**Status:** Accepted
**Date:** 2026-08-07

---

## Context

Sentinel surfaces alerts for every entity in the system. Without filtering, an operator opening the dashboard would immediately receive alerts for aircraft and vessels anywhere in the world - across all alert types - which is unmanageable.

The operator needs to define a scope before alerts are streamed. Scope has three dimensions:

- **Geographic region** - a named region (e.g. "France") that resolves to a bounding box
- **Entity types** - aircraft, vessels, or both
- **Alert rule types** - which anomaly categories to include (e.g. signal loss only, or all types)

This scope must be:
- Set once and saved per operator - not re-entered on every visit
- Applied on the server before alerts reach the WebSocket client
- Updatable without dropping and re-opening the connection

---

## Decision

### Scope object

The scope is a JSON object stored per-operator in a `user_workspaces` table on TimescaleDB:

```
user_workspaces (
  user_id       UUID REFERENCES users(user_id) PRIMARY KEY,
  scope         JSONB NOT NULL,
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
)
```

The `scope` JSONB field has the shape:

```json
{
  "geo_region": {
    "name": "France",
    "bounds": { "min_lat": 41.3, "max_lat": 51.1, "min_lon": -5.2, "max_lon": 9.6 }
  },
  "entity_types": ["aircraft", "vessel"],
  "alert_types": ["SIGNAL_LOSS", "ROUTE_DEVIATION", "PROXIMITY", "COMPOSITE"]
}
```

Geographic regions are selected from a predefined list with known bounding boxes. No external geocoder is required in v1. The operator can also draw a custom bounding box on the map.

### Server-side filtering

The API instance that consumes an alert from Kafka writes it to TimescaleDB, then publishes it to the `alert-events` Redis pub/sub channel. All API instances subscribe to `alert-events` and evaluate the alert against their local WebSocket connection map (see doc-gaps.md decision #10 — direct push from the consuming instance would miss users on other instances).

Scope filter on each `alert-events` message:

1. Use the **position embedded in the alert payload** (`payload.last_lat`, `payload.last_lon` for signal loss; `payload.lat`, `payload.lon` for proximity/composite) — not the current Redis position. The alert payload carries an immutable position recorded at detection time; the Redis position may have changed by delivery time.
2. Check whether the position falls within the scope's `geo_region.bounds`
3. Check whether the entity type matches the scope's `entity_types` list
4. Check whether the alert type matches the scope's `alert_types` list
5. Push only if all match

Operators with no saved workspace see the scope setup prompt and receive no alerts until a scope is saved.

### Scope updates over an active connection

An operator can update their scope while the WebSocket is open. The dashboard calls `PUT /users/me/workspace` with the new scope. The API updates the `user_workspaces` table, then the **dashboard reconnects the WebSocket**. On reconnect, the API loads the updated scope from `user_workspaces` and applies it to the new connection. This is simpler than a `scope_updated` in-band control message and avoids the complexity of hot-swapping a filter on a live connection. The reconnect is fast and transparent to the operator.

---

## Reasoning

**Server-side filtering is the only approach that scales.** Client-side filtering requires the server to push every alert to every connected operator and let the client discard most of them. Alert volume grows with the number of tracked entities. Filtering on the server means bandwidth and client rendering cost are proportional to what the operator actually cares about, not to the total system alert rate.

**One scope object per operator is the right granularity.** An operator has one active view at a time. Storing the full scope as JSONB in a single row per user keeps the query simple (`SELECT scope FROM user_workspaces WHERE user_id = $1`) and makes updates atomic (a single `UPDATE` replaces the whole scope).

**Predefined regions eliminate geocoder dependency in v1.** An external geocoder (Google Maps Geocoding API, Nominatim) adds a network dependency, rate limits, and cost. A curated list of maritime/aviation regions with known bounding boxes covers the practical use cases. Custom bounding boxes via map draw cover the rest.

**No scope = no stream.** Starting with a clean map and a scope prompt is intentional UX. It prevents alert flood on login and forces the operator to declare intent before receiving data. This is better than showing all alerts and expecting the operator to filter down.

---

## Alternatives Considered

### Client-side filtering (rejected)

- Server pushes all alerts to all clients regardless of scope; client discards non-matching ones
- Simple to implement but wastes bandwidth proportionally to how narrow the operator's scope is
- Does not scale: a system tracking thousands of entities producing hundreds of alerts per minute pushes all of that to every connected dashboard client
- Violates the principle of not doing work that can be avoided upstream

### Per-topic Kafka consumer per scope (rejected)

- Creating a dedicated Kafka consumer group per operator scope would allow the broker to do the filtering
- Kafka is not designed for per-user consumer groups; this would create thousands of consumer groups as operators connect
- The filter logic (geo bounds, entity type, alert type) cannot be expressed as a Kafka topic partition key - it requires application-level evaluation against Redis state
- The current approach (single consumer, in-process fan-out to WebSocket connections) is correct for this scale

### Saving scope in Redis (rejected)

- Redis is already used for live entity state; it is the right store for high-frequency ephemeral reads
- User workspace is low-frequency (read once at login, updated occasionally) and must survive restarts
- TimescaleDB already holds the `users` table from ADR-011; co-locating `user_workspaces` avoids a cross-store JOIN for the login flow

### External geocoder for region resolution (rejected)

- Adds a network dependency for a feature that a static lookup table handles in v1
- A predefined region list is explainable and testable; a geocoder response is not
- Custom bounding box draw on the map covers cases the region list does not

---

## Consequences

- A `user_workspaces` table is added to TimescaleDB as described above
- The API maintains an in-memory map of `{ connection_id -> scope }` for active WebSocket connections
- On WebSocket upgrade, the API loads the operator's saved scope from `user_workspaces` into this map
- The API publishes each consumed alert to `alert-events` Redis pub/sub; all instances receive it and evaluate against their local connection scope maps
- Alert scope filtering uses the position in the alert payload, not the current Redis position
- `GET /users/me/workspace` returns the saved scope (used by the dashboard on load to decide whether to show the scope prompt or restore the previous view)
- `PUT /users/me/workspace` updates the saved scope; the dashboard then reconnects the WebSocket to pick up the new scope server-side
- A predefined region list (name + bounding box) is maintained as a static JSON file in the API service - no database table needed for regions in v1
