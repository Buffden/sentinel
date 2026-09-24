# ADR-021: Provider Envelope for `adsb.raw`

**Status:** Accepted (2026-09-23)
**Date:** 2026-09-23
**Depends on:** ADR-001 (Kafka over HTTP ingestion), ADR-013 (Node.js ingestion poller), ADR-020 (aviation data provider strategy)

---

## Context

ADR-020 makes adsb.fi the regional primary live source and keeps OpenSky as the fallback. Both are ADS-B providers, so both belong on `adsb.raw`, but their raw records have different shapes: OpenSky's poller emits a flat object keyed by `icao24`, while adsb.fi returns objects keyed by `hex` with different field names and units.

Today the Position Consumer assumes every `adsb.raw` record is OpenSky's shape. Normalization looks for `icao24` and hard-codes `provider: 'opensky'`. A pre-change experiment on 2026-09-23 published one real adsb.fi record onto `adsb.raw`: it was archived in `raw_events` with `provider` and `entity_id` both null, routed to `adsb.dlq` as `missing_entity_id: icao24 field missing or empty`, and never reached `position_history` or Redis. Every adsb.fi record would take that path.

The consumer needs to know which provider produced a record before it parses it, and the archive needs to record that provider even when parsing fails.

---

## Decision

### Envelope

Every new message on `adsb.raw` is a transport envelope with exactly two fields:

| Field | Value |
| --- | --- |
| `provider` | `opensky` or `adsbfi`. New providers are added by a decision, not by a producer choosing a new string. |
| `payload` | The provider's per-aircraft record plus documented response/operational context required after splitting a provider response. OpenSky adds `fetched_at_ms`; adsb.fi preserves the response-level `now` as `response_now_ms` and may add `fetched_at_ms`. |

- All producers emit the envelope, including the existing OpenSky poller.
- For ICAO-addressed aircraft, the Kafka key is the lowercase ICAO24 address, whichever provider sent it, so every record for one aircraft stays on the same partition and in order across a provider switch. adsb.fi non-ICAO track addresses (those starting with `~`) are outside the CP1 canonical identity model and are skipped by the poller with an observable count per poll; they are never published to `adsb.raw` or sent to the DLQ. Supporting non-ICAO identities is a separate identity-model decision.
- adsb.fi's `seen_pos` is measured in seconds before the response's top-level `now`, so a per-aircraft record split out of a response is not time-complete without it. Keeping `now` as `response_now_ms` lets the mapping derive source event time deterministically as `response_now_ms` minus `seen_pos`, never from processing time, which preserves replay behaviour.
- The topic is unchanged, and `source` stays `adsb`, derived from the topic.

### Consumer handling

The Position Consumer classifies each record before normalizing it:

| Record | Classification | `raw_events.provider` | `raw_events.payload` | Next step |
| --- | --- | --- | --- | --- |
| Envelope with a known `provider` and an object `payload` | Enveloped | the envelope's `provider` | the `payload` only, envelope stripped | Normalize with that provider's mapping |
| No `provider` or `payload` key, matching the legacy OpenSky shape (below) | Legacy OpenSky | `opensky` | the record as received | Normalize with the OpenSky mapping |
| Envelope with an unknown `provider` value | Rejected: `unknown_provider` | null | the record as received | DLQ |
| `provider` or `payload` present but malformed | Rejected: `invalid_envelope` | null | the record as received | DLQ |
| Valid JSON matching neither shape | Rejected: `unidentified_provider` | null | the record as received | DLQ |
| Not valid JSON | Rejected: `parse_error` (unchanged) | null | JSONB string scalar (unchanged) | DLQ |

Normalization dispatches only on the classified provider. There is no default provider: a record that cannot be identified is archived and sent to the DLQ, never normalized as OpenSky.

### Legacy OpenSky compatibility

`adsb.raw` already holds bare OpenSky records, and replay from committed or older offsets must keep working. A record is classified as legacy OpenSky if it has no `provider` key and no `payload` key, `icao24` is a non-empty string, `fetched_at_ms` is a number, and the keys `lat`, `lon` and `time_position` are present (their values may be null). These identify Sentinel's own OpenSky producer without re-validating OpenSky's fields: the classified record is handed to the existing OpenSky normalizer, which still decides `no_position`, malformed field types and every other normalization outcome exactly as before. Any other bare record is `unidentified_provider`.

This path exists for replay compatibility only. No producer may emit bare records after this change. The path can be removed by a later decision once no bare records remain within `adsb.raw`'s retention and no replay of older offsets is needed.

### DLQ records

`adsb.dlq` records keep their existing fields. `raw_payload` preserves the exact rejected Kafka value, envelope included, for diagnosis and for reconstruction or republication after correction. The rejection reasons `unknown_provider`, `invalid_envelope` and `unidentified_provider` are added.

### Rollout order

The consumer that understands both envelopes and legacy records must be running before any producer emits the envelope. Producers change second.

---

## Scope

- **In scope:** the `adsb.raw` record contract, `raw_events.provider` and `raw_events.payload` derivation, consumer classification and dispatch, the new DLQ reasons.
- **No downstream canonical change.** `position_history`, Redis live state and `position.normalized` keep their schemas. Their existing `provider` field gains the value `adsbfi`.
- **No automatic provider switching.** Which poller runs is an operator choice until Phase 10's provider health checkpoint.
- **Not decided here:** how adsb.fi's own fields map to canonical fields (for example altitude units and the `"ground"` altitude value). That belongs to the adsb.fi mapping itself.
- **Deferred, unchanged:** records in a compression codec the consumer cannot decode (the snappy poison-pill finding in the Phase 10 plan). This ADR does not address it.

---

## Alternatives Considered

### A separate topic per provider (rejected)

Gives each topic one raw shape with no envelope, but adds a canonical topic per provider, and records for the same aircraft from two providers would no longer share a partition, so their relative order during a provider switch would not be preserved.

### A `provider` field added beside the provider's own fields (rejected)

Smaller change, but it mixes Sentinel's transport metadata into the provider's record, risks colliding with a provider field of the same name, and makes "what did the provider actually send" harder to archive faithfully.

### Treat any record without a provider as OpenSky (rejected)

Keeps old offsets working with no special code, but also silently accepts any malformed or foreign record as OpenSky. The narrow legacy shape check keeps replay working without that risk.

---

## Consequences

- The OpenSky poller wraps its records in the envelope; its raw record shape inside `payload` is unchanged.
- The adsb.fi poller emits enveloped records from its first version, keeps each response's `now` as `response_now_ms`, and skips non-ICAO tracks with a logged count.
- `raw_events.provider` is populated for every identified record, including ones that later fail normalization, so the archive shows which provider sent each rejected record.
- `raw_events.payload` holds only what the provider sent for identified records, so archived OpenSky payloads look the same before and after this change.
- The consumer gains a classification step and three DLQ reasons; the existing OpenSky normalization runs unchanged on the unwrapped payload.
- `DATA_MODEL.md` documents the envelope, the classification, and the `adsbfi` provider value.
