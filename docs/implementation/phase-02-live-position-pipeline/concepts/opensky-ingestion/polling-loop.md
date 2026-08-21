# Polling Loop Design

## Responsibility boundary

The ingestion poller has one job: fetch and forward faithfully. From `ARCHITECTURE.md`:

> The poller may unwrap the provider response envelope and split it into per-entity records. Field coercion, canonical naming, validation, persistence, and DLQ handling belong to the Position Consumer.

This means the poller does three things and nothing more:

1. Unwrap the OpenSky `states` array into individual per-aircraft records.
2. Map positional array indices to named fields so the Kafka log is readable.
3. Publish one message per record, keyed by `icao24`.

It does not rename fields to the canonical schema, filter null positions, validate types, or write to any store. The raw Kafka log must faithfully represent what OpenSky sent so that replaying `adsb.raw` in the future still gives the Position Consumer the original provider data to work from.

---

## Provider-fidelity field names

The `AdsbRawEvent` type in `poller.ts` uses OpenSky's own field names: `icao24`, `baro_altitude`, `true_track`, `time_position`, and so on. The Position Consumer maps these to canonical names (`entity_id`, `altitude`, `timestamp_ms`, etc.) during normalization.

Keeping provider names in the raw topic decouples the ingestion boundary from the normalization boundary. If OpenSky changes a field name, only the poller and the type definition change — the canonical schema is untouched.

---

## Message key: icao24

Every `adsb.raw` message is keyed by `icao24`. This is the ICAO 24-bit aircraft address, which becomes `entity_id` in Sentinel's canonical schema.

With one partition the key has no routing effect. When `adsb.raw` is scaled to multiple partitions, kafkajs hashes the key with murmur2 so all events for the same aircraft land in the same partition and are consumed in the order they were produced. Without a key, records would be round-robined and per-entity ordering would be lost.

---

## Bounding box

The poller defaults to a bounding box covering UK + Western Europe:

```
lamin=49  lomin=-8  lamax=61  lomax=10
```

All four values are configurable via environment variables: `OPENSKY_LAMIN`, `OPENSKY_LOMIN`, `OPENSKY_LAMAX`, `OPENSKY_LOMAX`.

The bounding box serves two purposes: it keeps response sizes manageable in development and it reduces credit consumption for anonymous OpenSky accounts.

---

## Fetch timeout

The HTTP fetch uses `AbortSignal.timeout(8_000)` — an 8-second hard deadline. This leaves 2 seconds of headroom inside the 10-second poll interval. If OpenSky takes longer than 8 seconds to respond, the cycle is aborted, a warning is logged, and the next cycle is scheduled normally.

The producer stays connected between cycles. There is no reconnect overhead on a slow OpenSky response.

---

## Error handling

Two failure modes and how the poller handles them:

**OpenSky fetch failure** (network error, timeout, non-2xx response): log a `warn` and return from `pollOnce` without publishing. The Kafka producer is unaffected. The next cycle retries automatically.

**Kafka publish failure** (broker unavailable): the `producer.send()` call throws, which is caught by the `.catch()` handler in `scheduleNextPoll` and logged as an `error`. The next cycle retries. The poller does not attempt to buffer unsent messages locally — at-least-once delivery is a Kafka guarantee, not a poller guarantee.

---

## Poll loop structure

The loop uses `setTimeout` recursively rather than `setInterval`. This means each poll does not begin until the previous one finishes:

```
run()
  └─ pollOnce()        ← fires immediately on startup
  └─ scheduleNextPoll()
       └─ setTimeout(POLL_INTERVAL_MS)
            └─ pollOnce()
            └─ scheduleNextPoll()   ← recurse
```

If a poll cycle takes longer than `POLL_INTERVAL_MS` (for example, a slow OpenSky response close to the timeout), the next cycle is simply delayed rather than overlapping. This prevents concurrent produce calls from the same process.

---

## Graceful shutdown

On `SIGINT` or `SIGTERM`:

1. Set `stopping = true` so `scheduleNextPoll` does not schedule another cycle.
2. Clear the pending `setTimeout` so the in-progress wait is cancelled.
3. Disconnect the Kafka producer cleanly.
4. Exit.

Any in-flight `pollOnce` call at the moment of the signal will complete or be abandoned by the process exit. Records that were partially published are at-least-once safe — the Position Consumer's idempotency handling absorbs any duplicates on restart.
