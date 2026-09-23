# adsb.fi Primary Ingestion: Design and Learning Reference

---

## What this checkpoint does

Before this checkpoint, Sentinel had one live aviation source, OpenSky, and every part of the ingestion path assumed it. ADR-020 made adsb.fi the regional primary and kept OpenSky as the fallback. This checkpoint adds adsb.fi as a working source: a new poller fetches it, and the Position Consumer turns its records into the same canonical positions it already produces for OpenSky, tagged with provider `adsbfi`.

Everything after the Position Consumer is unchanged. `position_history`, Redis live state and `position.normalized` keep their schemas. They simply start seeing a new provider value.

---

## In plain language

Two different companies now send aircraft positions into the same Kafka topic. They describe the same aircraft in different words and different units: one says `icao24`, the other `hex`; one reports altitude in metres, the other in feet. The consumer has to know who wrote a message before it can read it.

So every message now travels in a small envelope that says who sent it, with the provider's own record inside. The consumer opens the envelope, looks at the sender, and hands the record to the translator for that provider. If it cannot tell who sent a message, it refuses to guess and puts the message aside for a human to inspect.

---

## Concepts

### One topic, with a `{ provider, payload }` envelope

Both providers publish to the existing `adsb.raw` topic. Each message has exactly two fields: `provider` (`opensky` or `adsbfi`) and `payload` (the provider's per-aircraft record plus the few pieces of context the poller adds, described below).

A separate topic per provider was rejected in ADR-021. The reason is ordering. Kafka only orders messages within a partition, and the partition is chosen from the message key. Both pollers key messages by the aircraft's lowercase ICAO24 address and use the same partitioner, so every record for one aircraft lands on the same partition whichever provider sent it. During a future switch from one provider to the other, one aircraft's records stay in order. Two topics would lose that.

The envelope sits outside the provider's record rather than adding a `provider` field inside it. That keeps Sentinel's transport metadata from colliding with a provider field of the same name, and it means the archive can store exactly what the provider sent.

### Classification happens before normalization

The consumer's first step for every `adsb.raw` record is classification: deciding which provider produced it. Only then does a provider's mapping run.

The order matters for the archive. `raw_events` records the provider of every record, including records that later fail normalization. If the provider were only known after a successful mapping, a rejected adsb.fi record would be archived with no provider at all, which is exactly what the pre-change experiment in ADR-021 showed happening.

Classification has three outcomes:

| Outcome | What it means | What happens next |
| --- | --- | --- |
| Enveloped | A valid envelope with a known provider | That provider's mapping runs on the payload |
| Legacy OpenSky | A bare record in the shape the old OpenSky poller wrote | The OpenSky mapping runs on it |
| Rejected | Unknown provider, malformed envelope, a shape nobody recognizes, or invalid JSON | Archived with provider null, then sent to `adsb.dlq` |

There is deliberately no default provider. A record the consumer cannot identify is never read as OpenSky "just in case", because that would quietly accept any malformed or foreign record.

### Narrow legacy OpenSky compatibility

`adsb.raw` already held bare OpenSky records from before the envelope existed, and replaying old offsets has to keep working. The consumer therefore accepts one specific bare shape as OpenSky: a record with no envelope keys that has a non-empty `icao24`, a numeric `fetched_at_ms`, and `lat`, `lon` and `time_position` keys.

This check only asks "does this look like Sentinel's own old OpenSky poller wrote it?" It does not validate the fields. The OpenSky mapping still makes every decision it made before (no position, wrong types, and so on), so a replayed old record ends up exactly where it would have gone before this change. Anything bare that does not match is rejected as `unidentified_provider`.

No producer writes bare records any more. The legacy path exists only for replay, and a later decision can remove it once no bare records remain in the topic's retention.

### Provider-specific normalization

Each provider has its own mapping into the canonical position. The consumer picks the mapping from the classified provider and nothing else.

The adsb.fi mapping handles the differences that matter:

- **Identity:** the entity ID is the lowercase `hex` address.
- **Units:** altitudes arrive in feet and are converted to metres; ground speed arrives in knots and vertical rate in feet per minute, both converted to metres per second.
- **Ground state:** adsb.fi reports `alt_baro` as the string `"ground"` when an aircraft is on the ground. That becomes `on_ground` true with no barometric altitude. A numeric `alt_baro` becomes `on_ground` false. The geometric altitude is still kept when present, and `altitude_m` prefers it over barometric altitude, the same rule the OpenSky mapping uses.
- **Category:** adsb.fi's emitter category codes (such as `A5` or `A7`) map to the same subtypes the OpenSky integer codes do.

`on_ground` is not just a display field. The Alert Evaluator skips signal-loss checks for aircraft whose live state says they are on the ground, so this mapping changes which aircraft can raise signal-loss alerts.

### ICAO-only identity in CP1

adsb.fi also reports tracks that have no real ICAO address. Their `hex` starts with `~`. Sentinel's canonical identity model is built on ICAO addresses, and deciding how non-ICAO tracks should be identified is a separate decision this checkpoint does not make.

So the poller skips them and counts them in every poll log line (`skipped_non_icao`). They are never published and never reach the DLQ, because they are not malformed, just out of scope. The consumer's adsb.fi mapping also rejects a `~` address, so a hand-published record cannot slip one into the identity model.

### Deterministic event time from `response_now_ms - seen_pos`

adsb.fi does not give each aircraft an absolute timestamp. It gives one timestamp for the whole response (`now`) and, per aircraft, how many seconds before that moment the position was received (`seen_pos`).

Once the poller splits a response into one message per aircraft, that relative time is useless without `now`. So the poller copies the response's `now` into every payload as `response_now_ms`. The consumer computes the position's source event time as `response_now_ms` minus `seen_pos`, rounded to whole milliseconds.

This matters because source event time feeds `position_history`'s identity `(entity_id, observed_at)` and the Redis monotonic guard. Both values come from the message itself, so replaying the same message always produces the same event time. Using processing time would give a different answer on every replay and break idempotency.

The poller also checks that `now` really is epoch milliseconds. If it is not, the whole response is rejected rather than published with wrong times.

### 2-second regional poll with bounded 429 backoff

The adsb.fi poller queries a circle (adsb.fi has no box query) that contains the monitored box, then drops aircraft outside the box. It polls every 2 seconds by default. That matches how often adsb.fi positions changed per aircraft in the provider experiment, and stays under adsb.fi's public limit of one request per second. Configuration refuses any interval below one second.

adsb.fi's `429` responses carry no retry time and no rate-limit headers. So after a failed cycle (a `429`, another HTTP error, a network failure, or a failed Kafka publish), the poller waits using bounded exponential backoff with full jitter: the wait ceiling doubles with each consecutive failure up to a maximum, the actual wait is random below that ceiling, and it is never shorter than the normal poll interval. The first successful cycle logs a recovery and returns to the normal interval.

The poller also sends a descriptive user agent, because adsb.fi's front end rejects default client user agents.

### No automatic failover yet

Only one provider is authoritative at a time, and in this checkpoint the operator chooses which poller runs. Nothing detects that adsb.fi has stopped and switches to OpenSky. That is CP3, which needs its own design and ADR, because a provider outage currently looks to the Alert Evaluator like every aircraft going silent at once.

---

## Ownership

| Part | Owner | Reads | Writes |
| --- | --- | --- | --- |
| adsb.fi poller | Ingestion Poller | adsb.fi circle endpoint | `adsb.raw` (enveloped) |
| OpenSky poller | Ingestion Poller | OpenSky REST API | `adsb.raw` (enveloped, same inner record as before) |
| Classification and dispatch | Position Consumer | `adsb.raw` | `raw_events`, `adsb.dlq` for rejected records |
| adsb.fi mapping | Position Consumer | classified adsb.fi payload | the canonical position, then the existing history, Redis and `position.normalized` writes |

The pollers still only fetch, split, filter by area and wrap. Units, field names, validation and DLQ handling stay in the Position Consumer, as `ARCHITECTURE.md` requires.

---

## Failure modes

**A record with no recognizable provider.** Archived with provider null and sent to `adsb.dlq` with reason `unknown_provider`, `invalid_envelope` or `unidentified_provider`. The DLQ keeps the exact Kafka value, envelope included, so it can be corrected and republished. It is never normalized as OpenSky.

**A replay of old bare OpenSky records.** Recognized by the legacy shape check and normalized exactly as before. Records already in `position_history` are skipped by the existing idempotent insert.

**Rows written before a mapping existed.** `position_history` inserts do nothing when `(entity_id, observed_at)` already exists. Replaying the topic after a mapping improves does not rewrite older rows. Correcting them would need a deliberate backfill.

**An unusable response time.** If adsb.fi's `now` is missing or not epoch milliseconds, the poller rejects the whole response and counts it as a failed cycle, rather than publishing records whose event times would be wrong.

**Rate limiting.** A `429` counts as a failed cycle and the poller backs off with jitter instead of retrying at full rate.

**adsb.fi stops answering entirely.** Not handled yet. The poller keeps backing off, positions stop arriving, and after the signal-loss timeout every tracked aircraft looks lost. This is CP3's problem to solve.

**A record in a compression codec kafkajs cannot decode.** Unchanged by this checkpoint and still latent. It is recorded in the Phase 10 plan's Starting State and scheduled for the failure lab.

---

## Map to code

| Concept | Where |
| --- | --- |
| Envelope builder | `adsbRawEnvelope`, `services/ingestion-poller/src/envelope.ts` |
| adsb.fi poller, response split, `~` skip, box filter | `splitAdsbfiResponse`, `services/ingestion-poller/src/adsbfiPoller.ts` |
| Response time check | `toResponseNowMs`, same file |
| Backoff with jitter | `nextDelayMs`, same file |
| adsb.fi settings (circle, box, interval, backoff) | `services/ingestion-poller/src/config.ts` |
| OpenSky poller wrapping its records | `services/ingestion-poller/src/poller.ts` |
| Classification and legacy shape check | `classifyAdsbRaw`, `services/position-consumer/src/classify.ts` |
| Dispatch by provider | `normalizeByProvider`, `services/position-consumer/src/normalize.ts` |
| adsb.fi mapping | `normalizeAdsbfiRecord` and `mapAdsbfiAltitudeAndGround`, same file |
| Event time derivation | `adsbfiEventTimeMs`, same file |
| Archive and DLQ routing for rejections | `handleMessage` and `routeToDlq`, `services/position-consumer/src/consumer.ts` |
| Contract | ADR-021; `adsb.raw` section of `docs/DATA_MODEL.md` |

---

## Retention questions

1. Why does the consumer classify a record before normalizing it, instead of trying each provider's mapping until one works?
2. Why do both providers share one topic and one key, rather than having a topic each?
3. Why does the legacy OpenSky check look only at the record's shape and not at whether its fields are valid?
4. Why does the adsb.fi poller copy the response's `now` into every per-aircraft message?
5. Why are `~` tracks skipped with a count rather than sent to the DLQ?
6. After improving a mapping, why doesn't replaying `adsb.raw` fix the rows already in `position_history`?
7. What happens today if adsb.fi goes down for ten minutes, and which checkpoint changes that?

---

## Completion checklist

- [ ] I can explain the envelope and why it is outside the provider's record
- [ ] I can trace one adsb.fi aircraft from the HTTP response to `position_history` and `entity:live`
- [ ] I can explain each classification outcome and where its record ends up
- [ ] I can explain how source event time is derived and why it is replay-safe
- [ ] I can explain what `on_ground` changes downstream
- [ ] I can explain why there is no automatic failover yet
