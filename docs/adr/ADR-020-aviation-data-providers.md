# ADR-020: Aviation Data Provider Strategy

**Status:** Accepted (2026-09-23). Adopting FlightAware for filed routes still needs its own decision against ADR-015 when Phase 04 resumes.
**Date:** 2026-09-22, revised 2026-09-23 with a measured provider comparison
**Depends on:** ADR-007 (idempotency key schema), ADR-013 (Node.js ingestion poller), ADR-015 (v1 reference route model)

---

## Context

The ingestion poller (ADR-013) pulls live aircraft positions from the OpenSky Network REST API and has been hitting OpenSky's rate limits. This raised the question of whether to switch to a paid provider or add more providers, and whether a different source could also supply the real filed routes that Phase 04 route deviation has been waiting on since it stopped at CP1.

This ADR records the research done on 2026-09-22, a measured comparison of OpenSky and adsb.fi run on 2026-09-23, and the provider strategy decided from that evidence. Nothing in it is implemented yet. The experiment's method, results and reproduction steps are in `docs/implementation/phase-10-production-hardening/concepts/provider-experiment/README.md`.

### Why the poller runs out of credits today

OpenSky does not limit requests per second. It gives each account a daily budget of credits, and each `/states/all` call spends credits based on the size of the bounding box. The `/states` endpoints have their own budget, separate from tracks and flights.

| Account type | Daily credits for `/states` |
| --- | --- |
| Anonymous | 400 |
| Logged in (OAuth2 client credentials) | 4,000 |
| Active feeder (own receiver, at least 30% uptime) | 8,000 |

| Bounding box area | Credits per call |
| --- | --- |
| 25 square degrees or less | 1 |
| 25 to 100 | 2 |
| 100 to 400 | 3 |
| Over 400, or global | 4 |

The poller's defaults are a 10 second interval and a box of latitude 49 to 61, longitude -8 to 10 (UK and Western Europe). That box is 12 by 18 degrees, which is 216 square degrees, so each call costs 3 credits. Polling every 10 seconds is 8,640 calls a day, or about 26,000 credits. That is more than six times the logged-in budget and about 65 times the anonymous one. A logged-in account can afford one call about every 65 seconds over that box.

The comment in the poller's config says the anonymous limit is "approximately one request per 10 s." That is not how OpenSky limits access. Ten seconds is the data resolution for anonymous users (five seconds when logged in), not an allowed request rate.

OpenSky returns two headers the poller does not use yet: `X-Rate-Limit-Remaining`, which is the credit balance left, and `X-Rate-Limit-Retry-After-Seconds`, sent with a `429` response when the budget is gone.

### Why this matters beyond ingestion

When the budget runs out, every aircraft stops updating at the same moment. The Alert Evaluator declares signal loss after 5 minutes of silence, and OpenSky's budget does not come back until the daily refill. So an exhausted budget does not look like "the provider is unavailable." It looks like every tracked aircraft went dark, which produces a signal-loss alert for all of them. Sentinel currently has no way to tell those two situations apart. This is true with one provider and gets more important with several.

---

## Providers Compared

| | OpenSky (current) | Flightradar24 API | FlightAware AeroAPI | adsb.fi | adsb.lol |
| --- | --- | --- | --- | --- | --- |
| Live positions by area | Bounding box, or global | Bounding box | Yes, but priced per result set | Circle up to 250 NM only; global only through the feeder snapshot | Not verified |
| Filed routes | No | No | Yes | No | Plausible routes only |
| How it is limited | Daily credit budget | Monthly credit budget plus queries per minute | Per result set (15 records) plus result sets per minute | 1 request per second | Dynamic, depends on load |
| Cost | Free | $9, $90 or $900 a month | Personal tier has no minimum and includes $5 of usage a month | Free | Free |
| Terms | Non-profit research and education. Commercial or operational live use needs a written license | Personal, non-commercial. Any public display needs the Business plan | Personal tier is non-commercial | Personal, non-commercial, attribution required | ODbL 1.0, share-alike |

### OpenSky

Still the best fit for live positions. Logged in with a box of 25 square degrees or less, the budget allows one call about every 22 seconds. Feeding a receiver doubles the budget to about one call every 11 seconds.

The terms allow non-profit research and education, which fits a portfolio project. They also say that operational use of the REST API in any live product, service or automated system needs a written license, even for non-profit users. Sentinel's live pipeline arguably counts as an automated system, so this should be confirmed with OpenSky by email rather than assumed.

### Flightradar24

Not viable for continuous ingestion. The live positions endpoint costs 6 credits for each aircraft it returns, so one poll that returns 100 aircraft costs 600 credits.

| Plan | Price | Credits a month | Queries a minute | Polls of 100 aircraft |
| --- | --- | --- | --- | --- |
| Explorer | $9 | 30,000 | 10 | About 50 a month |
| Essential | $90 | 333,000 | 30 | About 555 a month |
| Advanced | $900 | 4,050,000 | 90 | About 225 a day, one every 6 minutes |

The terms also restrict any public display of the data, commercial or not, to the Business plan. A sandbox with free test requests exists, which is useful for trying the API but not for running it.

### FlightAware AeroAPI

Too expensive for bulk live positions, since continuous polling is priced per 15 records returned. It is a good fit for routes. The filed route endpoint (`/flights/{id}/route`) costs $0.01 per result set on every tier, so the $5 of monthly usage included in the Personal tier covers about 500 route lookups. That is enough to run route deviation against a small set of real flights. Historical routes need the Standard tier ($100 monthly minimum). The Personal tier allows 10 result sets a minute and excludes commercial use.

### Community ADS-B feeds

adsb.fi's public API has no bounding box or global query. It answers by aircraft identity (hex, callsign, registration, squawk) or by a circle up to 250 nautical miles around a point, limited to one request per second, with no API key. A separate feeder-only endpoint, `/v2/snapshot`, returns every aircraft, refreshes twice a minute and may be called every 30 seconds, but only from an IP registered as an adsb.fi feeder. Terms: personal, non-commercial use, with attribution and a link to adsb.fi; the data may not be licensed, sold, rented or leased.

adsb.lol publishes its data under ODbL 1.0. ODbL's share-alike clause may apply to a database built from its data, such as Sentinel's position history, if that database is used publicly. adsb.lol was not measured.

### Measured basis (2026-09-23)

A 15-minute side-by-side experiment polled OpenSky and adsb.fi over the same SF Bay area. Its method, results and caveats are in `docs/implementation/phase-10-production-hardening/concepts/provider-experiment/README.md`. Only what this decision rests on is summarised here.

- **Measured conclusion.** In the 15-minute SF Bay experiment, adsb.fi outperformed OpenSky on coverage, freshness, latency, field richness, and observed update frequency. OpenSky provided better explicit rate-limit signalling. This is not generalized to other geographies or times.
- **Rate-limit behaviour that shapes the adapters.** adsb.fi returns no rate-limit headers, and a `429` from exceeding one request per second carries no retry time; recovery was observed once after about 2.4 seconds, which is an observation, not a contract. adsb.fi also rejects default client user agents. OpenSky reports its remaining credits on every response, and its `429` carries a retry time.
- **Proximity, supporting evidence only.** A lower sampling rate reduced how many close pairs the correlation worker's rule detected. The reference truth came from adsb.fi and only two clearly airborne pairs came within 300 m, so no conclusion about airborne proximity detection is drawn.
- **Outside this decision.** Most close pairs involved airport surface traffic. That is a separate Phase 10 investigation; this ADR does not change the correlation worker.

---

## Decision

Each source gets one job, and at any moment exactly one provider is the authoritative source of live positions.

| Need | Source | Status |
| --- | --- | --- |
| Regional, high-frequency positions | adsb.fi public regional API | **Primary.** Measured |
| Global positions | adsb.fi feeder `/v2/snapshot` | Preferred **if** feeder access is obtained. **Not yet measured** |
| Global positions | OpenSky | The currently proven global-capable fallback |
| Filed routes | FlightAware AeroAPI | When Phase 04 resumes, subject to its own decision against ADR-015 |
| Live failover | OpenSky | Takes over only when the primary is down, through explicit failover |

adsb.fi is not the global primary. It becomes the preferred global source only once feeder access exists and its snapshot has been measured the same way as the regional API.

1. **adsb.fi is the primary regional source.** The Position Consumer gets a second raw mapping for it. Its poller sends a descriptive user agent and stays under one request per second. Because adsb.fi gives no retry time, its adapter backs off after a `429` with bounded exponential backoff and jitter, not a fixed wait based on the 2.4 seconds observed here.
2. **OpenSky is the live failover and the proven global-capable fallback.** Its existing poller is hardened to stay inside its daily budget and honour its retry-after header, so it is healthy when it has to take over.
3. **Provider health comes before failover.** Failover needs a reliable signal that the primary is down, and signal loss must not fire for every aircraft during an outage. The two providers did not see identical aircraft in the experiment, so failover changes which aircraft are visible. How to detect and represent provider health, and when to fail over and fail back, is designed in Phase 10 with its own ADR. That ADR must decide failback hysteresis as well as failover, so an unstable recovery cannot make Sentinel bounce between adsb.fi and OpenSky.
4. **FlightAware Personal only for filed routes, when Phase 04 resumes.** This changes ADR-015, which limits v1 reference routes to synthetic entities, so it needs its own decision at that point.
5. **No Flightradar24.** Its per-aircraft pricing and public display terms rule it out for continuous ingestion.

**The rule this decision preserves:** one authoritative live provider at a time, explicit failover between them, and no merging of positions from several providers at once.

### Why not merge several live sources

Merging live positions from several providers does not deduplicate the way it first appears. ADR-007's position identity is `(entity_id, observed_at)`, which only matches when two sources report exactly the same timestamp. Different providers use different receivers and have different delays, so they almost never do. Both copies would be stored, and Redis live state, which accepts whichever report is newer, would jump between sources. That noise would feed straight into proximity and deviation checks.

With a second live source, the safer model is one authoritative provider at a time, with the source recorded on each position, and an explicit failover rule rather than a merge. Reconciling positions from several providers at once would need its own design. A shared provider adapter layer should wait until a second live source actually exists, not be built ahead of it.

---

## Open Questions

- Does OpenSky treat Sentinel's live pipeline as operational use that needs a written license?
- Is a public portfolio demo compatible with FlightAware's Personal tier, and with adsb.fi's terms (personal, non-commercial, attribution)?
- What backoff bounds and jitter suit adsb.fi, given that it gives no retry time?
- Does adsb.fi's advantage hold at other times of day, over longer windows and in other regions, and for airborne encounters specifically?
- How does the adsb.fi feeder snapshot compare, once feeder access exists?
- How should Sentinel signal provider health so the Alert Evaluator can suppress signal loss during a provider outage, without hiding a real aircraft going dark?
- What failover and failback rules, including hysteresis, keep Sentinel from bouncing between providers during an unstable recovery?
- What circle size and poll interval give an acceptable picture over the region Sentinel actually demonstrates?

---

## Consequences

- ADR-013's provider scope changes: adsb.fi is the primary regional live aviation source, and OpenSky the live failover and global-capable fallback.
- The Position Consumer gains a second raw mapping (adsb.fi's ADS-B Exchange-compatible format) alongside OpenSky's. Each normalized position records its provider in the existing canonical `provider` field (`adsbfi` alongside `opensky`).
- Whether raw adsb.fi messages can travel on the existing `adsb.raw` topic or need a new one is not decided here. The canonical `provider` field existing does not show that the current raw Kafka contract can carry adsb.fi unchanged. This is settled at Phase 10's first checkpoint; a new or changed canonical topic needs its own ADR.
- One authoritative live provider at a time, explicit failover, and no simultaneous merging of positions, unless source reconciliation is designed separately.
- The OpenSky poller's defaults (box size, interval) and rate-limit handling change so it is ready to act as the fallback. The config comment about anonymous limits is corrected.
- Phase 10's checkpoints are reordered: adsb.fi regional primary ingestion first, then hardening OpenSky as the fallback, then provider health with failover and failback between them.
- The ground-traffic proximity finding is investigated separately in Phase 10. The correlation worker is not changed by this decision.
- ADR-015 is revisited when Phase 04 resumes, with FlightAware filed routes as the candidate real route source.
- This decision by itself changes no canonical schema or downstream service. The raw topic question above, and any change provider health needs (for example in the Alert Evaluator's signal-loss rule), are decided in Phase 10.

---

## Sources

Provider research checked 2026-09-22; measured comparison run 2026-09-23 (method, results and capture checksums in the experiment README). OpenSky limits and FlightAware pricing were read from the providers' own pages. Flightradar24 plan figures and terms, and OpenSky's terms, came from search results because those pages did not render when fetched. Re-check them before spending money or relying on the terms.

- OpenSky REST API documentation (limits, credits, headers): https://openskynetwork.github.io/opensky-api/rest.html
- OpenSky terms of use: https://opensky-network.org/about/terms-of-use
- Flightradar24 API subscriptions and credits: https://fr24api.flightradar24.com/subscriptions-and-credits
- Flightradar24 API credit overview: https://fr24api.flightradar24.com/docs/credit-overview
- Flightradar24 terms and conditions: https://www.flightradar24.com/terms-and-conditions
- FlightAware AeroAPI pricing: https://www.flightaware.com/commercial/aeroapi/
- adsb.fi open data API: https://github.com/adsbfi/opendata/blob/main/README.md
- adsb.lol API documentation: https://www.adsb.lol/docs/open-data/api/
