# adsb.fi Primary Ingestion Debrief

Evidence from verifying CP1 on 2026-09-23, against the local Docker Compose infrastructure (Redpanda, TimescaleDB, Redis) and the live adsb.fi API. The design is in [adsbfi-primary-ingestion.md](adsbfi-primary-ingestion.md).

---

## Automated checks

The full Position Consumer suite passed: 68 tests across 4 files, including the integration tests that run against real Redis and TimescaleDB. The typecheck was clean.

```text
$ cd services/position-consumer && npx vitest run
 Test Files  4 passed (4)
      Tests  68 passed (68)

$ npx tsc --noEmit -p .
(no output, exit 0)
```

The ingestion poller's suite also passed (21 tests across 3 files, covering the envelope, the response split, the `now` check and the backoff delay), and its typecheck was clean.

The three adsb.fi altitude tests go through `normalizeAdsbfiRecord` using a real captured aircraft (EIN960). They check the feet-to-metres conversion, that `altitude_m` prefers geometric altitude and falls back to barometric when geometric is missing, and that `alt_baro: "ground"` gives `on_ground` true with a null barometric altitude while keeping the geometric one.

---

## Live run

The Position Consumer and the adsb.fi poller (`npm run poll:adsbfi`) ran together from 06:18:59 to 06:20:04 UTC with the default settings: a 48 NM circle around the SF Bay box, polled every 2 seconds. The OpenSky poller was not running.

### What the poller saw

Each cycle fetched about 50 aircraft and published 39. It skipped 2 with non-ICAO (`~`) addresses and 9 outside the box, and none for missing positions. A typical log line:

```text
{"level":"info","service":"ingestion-poller","provider":"adsbfi","message":"poll cycle complete",
 "aircraft_in_response":50,"published":39,"skipped_non_icao":2,"skipped_no_position":0,
 "skipped_outside_box":9,"topic":"adsb.raw","first_offset":"439067"}
```

### What the consumer did

The consumer processed 1,060 positions with 0 errors and nothing routed to the DLQ. Counts of its log messages over the run:

| Log message | Count |
| --- | ---: |
| `position persisted` | 1,060 |
| `live state not updated`, stale event | 259 |
| `signal loss episode cleared` | 2 |

The 259 stale-event lines are the Redis monotonic guard rejecting live-state updates that were not newer than what was stored. The likely cause is adsb.fi returning the same position (and so the same source timestamp) on consecutive polls 2 seconds apart. That was not investigated further in CP1. History rows are unaffected, since each `(entity_id, observed_at)` is written once.

---

## Inspected state

### TimescaleDB: altitudes converted from feet to metres

```text
SELECT entity_id, altitude_m, baro_altitude_m, geo_altitude_m, on_ground, observed_at
FROM position_history WHERE provider = 'adsbfi'
ORDER BY observed_at DESC LIMIT 10;

 entity_id | altitude_m | baro_altitude_m | geo_altitude_m | on_ground |        observed_at
-----------+------------+-----------------+----------------+-----------+----------------------------
 0d0e3b    |    5539.74 |         5311.14 |        5539.74 | f         | 2026-09-23 06:20:01.939+00
 a791c0    |    1684.02 |         1653.54 |        1684.02 | f         | 2026-09-23 06:20:01.936+00
 ab9276    |     1295.4 |         1272.54 |         1295.4 | f         | 2026-09-23 06:20:01.903+00
 ac41f3    |    2964.18 |         2849.88 |        2964.18 | f         | 2026-09-23 06:20:01.9+00
 a017c9    |            |                 |                | t         | 2026-09-23 06:20:01.883+00
 a5cdb3    |            |                 |                | t         | 2026-09-23 06:20:01.789+00
 a3b02e    |     -22.86 |                 |         -22.86 | t         | 2026-09-23 06:20:01.759+00
 addb2b    |    3817.62 |         3665.22 |        3817.62 | f         | 2026-09-23 06:20:01.749+00
 a2a823    |    1645.92 |         1607.82 |        1645.92 | f         | 2026-09-23 06:20:01.744+00
 a24f2b    |            |                 |                | t         | 2026-09-23 06:20:01.727+00
```

Airborne rows have both altitudes in metres, and `altitude_m` equals the geometric value.

### The ground path on real data

All adsb.fi rows written during the run, grouped by `on_ground`:

```text
 on_ground | rows | aircraft | baro_null | altitude_null
-----------+------+----------+-----------+---------------
 f         |  355 |       15 |         0 |             0
 t         |  443 |       28 |       443 |           438
```

28 real aircraft were reported on the ground. Every grounded row has a null barometric altitude, as expected for `alt_baro: "ground"`. Five grounded rows still have an `altitude_m`, taken from geometric altitude, for example `a3b02e` above. The rest had no geometric altitude either. No row from the run has `on_ground` null.

### Redis: live state matches history

```text
$ redis-cli HMGET entity:live:0d0e3b provider altitude_m on_ground last_seen_ms
adsbfi
5539.740000000001
false
1790144401939

$ redis-cli HMGET entity:live:a3b02e provider altitude_m on_ground last_seen_ms
adsbfi
-22.86
true
1790144401759
```

`last_seen_ms` 1790144401939 is 06:20:01.939 UTC, matching `observed_at` for `0d0e3b` in TimescaleDB to the millisecond. The same holds for `a3b02e` (06:20:01.759). Both stores got the same source event time, derived from `response_now_ms - seen_pos`.

---

## Boundaries observed

**Idempotent history does not rewrite old rows.** 1,545 adsb.fi rows written before the altitude mapping existed still have null altitude and null `on_ground`:

```text
 before_run | ground_null | count
------------+-------------+-------
 f          | f           |   798
 t          | f           |     3
 t          | t           |  1545
```

This is the intended behaviour, not a defect. History inserts do nothing when `(entity_id, observed_at)` already exists, so reprocessing the same messages with a better mapping leaves the old rows alone. Filling them in would need a deliberate backfill.

---

## Observations kept as they are

- **Negative geometric altitude.** `a3b02e` reads -22.86 m on the ground. adsb.fi's geometric altitude is measured from the GPS reference ellipsoid, not sea level, and around SF Bay the ellipsoid sits roughly 30 m above sea level, so an aircraft at a sea-level airport reads negative. The value is kept as reported. Anything that later shows `altitude_m` for grounded aircraft should expect it.
- **Float formatting in Redis.** Redis stores `5539.740000000001` where TimescaleDB shows `5539.74`. That is how JavaScript writes the number as a string. It was left unchanged. Rounding would be a separate decision.

---

## Failure boundary: live `429` and recovery

**Method.** The real adsb.fi poller ran unchanged with its default settings (2 second interval, backoff from 2 to 60 seconds) from 06:27:20 to 06:28:13 UTC. To push adsb.fi over its one-request-a-second limit without changing any code or configuration, a throwaway shell loop in a scratch directory sent back-to-back requests to the same adsb.fi URL from the same machine for 15 seconds (06:27:28 to 06:27:43). It made 59 requests: adsb.fi answered 29 with `200` and 30 with `429`. The loop was then stopped, and the poller kept running for another 30 seconds. The Position Consumer was not running. The poller's records stayed on `adsb.raw` for it to process later.

**What the poller logged** (the `url`, `box` and offset fields are trimmed):

```text
06:27:29.297 info  poll cycle complete                           published 48
06:27:31.466 warn  adsb.fi rate limited (429, no retry time given)
06:27:31.467 warn  backing off before next request               consecutive_failures 1, delay_ms 2000
06:27:33.636 warn  adsb.fi rate limited (429, no retry time given)
06:27:33.636 warn  backing off before next request               consecutive_failures 2, delay_ms 2000
06:27:35.797 warn  adsb.fi rate limited (429, no retry time given)
06:27:35.797 warn  backing off before next request               consecutive_failures 3, delay_ms 5029
06:27:41.050 warn  adsb.fi rate limited (429, no retry time given)
06:27:41.050 warn  backing off before next request               consecutive_failures 4, delay_ms 2000
06:27:43.238 info  poll cycle complete                           published 48
06:27:43.238 info  adsb.fi recovered                             after_failures 4
06:27:45.418 info  poll cycle complete                           published 48
```

**What it shows:**

- **Every `429` was detected** and counted as a failed cycle: four in a row.
- **Backoff was applied, with jitter, and never below the floor.** The delays were 2,000, 2,000, 5,029 and 2,000 ms. Each one is a random value under a ceiling that doubles with each failure (2, 4, 8, then 16 seconds), raised to the 2 second poll interval when the random value is lower. That is why the fourth delay is shorter than the third: jitter is random under the ceiling, not a fixed growing wait. No delay was below 2,000 ms.
- **Recovery was clean.** The first request after the loop stopped succeeded, and the poller logged `adsb.fi recovered` once, with the failure count.
- **Normal polling resumed.** The 13 cycles after it all succeeded about every 2.2 seconds (the 2 second interval plus request time) until shutdown. No errors were logged and nothing was retried at full rate during the burst.

The poller sent one request per cycle throughout, so it added at most one request every 2 seconds to the load. Backoff reduced that further while adsb.fi was refusing requests.

---

## Not exercised in this run

- **A rejected envelope on real infrastructure.** Classification rejections are covered by unit tests, and the pre-change experiment in ADR-021 showed the DLQ path for an unidentified record. No envelope was deliberately rejected during this run.

---

## Engineering debrief

**Data flow:** the adsb.fi poller fetches a circle, drops aircraft outside the box and `~` tracks, copies the response's `now` into each aircraft as `response_now_ms`, and publishes one `{ provider: 'adsbfi', payload }` message per aircraft to `adsb.raw`, keyed by lowercase ICAO24. The Position Consumer classifies the message, archives only the payload in `raw_events` with provider `adsbfi`, maps it into a canonical position, and writes history, live state and `position.normalized` exactly as it does for OpenSky.

**Trade-off:** one shared topic with an envelope, rather than a topic per provider. It costs a classification step and a legacy path for old bare records. It keeps one aircraft's records on one partition across providers, which a future failover needs.

**Failure behaviour:** unidentifiable records go to the DLQ with the full envelope and are never guessed as OpenSky. Event time comes only from the message, so replays are idempotent. Old history rows are never rewritten.

## Manual inspection commands

```bash
# Run the consumer and the adsb.fi poller (separate terminals)
cd services/position-consumer && npm run consumer
cd services/ingestion-poller && npm run poll:adsbfi

# Latest adsb.fi history rows
docker compose exec -T timescaledb psql -U sentinel -d sentinel -c \
  "SELECT entity_id, altitude_m, baro_altitude_m, geo_altitude_m, on_ground, observed_at
   FROM position_history WHERE provider = 'adsbfi' ORDER BY observed_at DESC LIMIT 10;"

# One aircraft's live state
docker compose exec -T redis redis-cli HMGET entity:live:<entity_id> provider altitude_m on_ground last_seen_ms

# Records the consumer refused
docker compose exec -T redpanda rpk topic consume adsb.dlq -n 5
```

## Knowledge-check questions

1. `0d0e3b` has the same timestamp in `position_history` and Redis to the millisecond. What makes that true, and what would break if the consumer used its own clock instead?
2. Why do 1,545 older adsb.fi rows still have null altitudes after the mapping was added, and why is that correct?
3. The consumer logged 259 stale-event rejections but no errors. Why is a rejection here not a failure?
4. `a3b02e` is on the ground with an altitude of -22.86 m. Where does that number come from, and why wasn't it replaced with zero?

## Optional manual tweak

Set `ADSBFI_POLL_INTERVAL_MS=5000` when starting the poller and rerun for a minute. Compare the number of stale-event rejections with the 2-second run. If the guess above is right (repeated positions between close polls), the number should drop.

## Next

CP2: harden the OpenSky poller as the fallback, so it runs within its credit budget and honours OpenSky's retry time, before CP3 connects the two through provider health.
