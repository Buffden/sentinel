# ADR-020: Aviation Data Providers: OpenSky, Flightradar24, FlightAware

**Status:** Proposed
**Date:** 2026-09-22
**Depends on:** ADR-007 (idempotency key schema), ADR-013 (Node.js ingestion poller), ADR-015 (v1 reference route model)

---

## Context

The ingestion poller (ADR-013) pulls live aircraft positions from the OpenSky Network REST API and has been hitting OpenSky's rate limits. This raised the question of whether to switch to a paid provider or add more providers, and whether a different source could also supply the real filed routes that Phase 04 route deviation has been waiting on since it stopped at CP1.

This ADR records the research done on 2026-09-22. Nothing here is decided or implemented yet. It exists so that later decisions (tuning OpenSky, adding a route source) can point to the evidence instead of re-deriving it.

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
| Live positions by bounding box | Yes | Yes | Yes, but priced per result set | Yes | Yes |
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

adsb.fi allows one request a second on its public endpoints, far more headroom than OpenSky, but only for personal non-commercial use with attribution. adsb.lol publishes its data under ODbL 1.0. ODbL's share-alike clause may apply to a database built from its data, such as Sentinel's position history, if that database is used publicly. Both are reasonable fallbacks for development, not obvious primary sources for a public demo.

---

## Proposed Direction

Each source gets one job instead of all of them streaming live positions.

1. **Keep OpenSky as the only live position source.** Make sure the poller logs in, shrink the box to 25 square degrees or less, poll about every 25 seconds, and read the rate-limit headers so the poller slows down before it gets a `429`. Fix the misleading config comment. Consider feeding a receiver to OpenSky to double the budget. Email OpenSky to confirm how its terms apply to Sentinel.
2. **Handle "provider down" separately from "aircraft dark."** Exhausting the budget or losing the provider should not produce a signal-loss alert for every aircraft. This is a real gap today regardless of how many providers Sentinel ends up using. How to detect and represent provider health is an open design question, not settled here.
3. **Use FlightAware Personal only for filed routes, when Phase 04 resumes.** This changes ADR-015, which limits v1 reference routes to synthetic entities, so it needs its own decision at that point.
4. **Do not adopt Flightradar24.** Its per-aircraft pricing and public display terms rule it out for continuous ingestion.

### Why not merge several live sources

Merging live positions from several providers does not deduplicate the way it first appears. ADR-007's position identity is `(entity_id, observed_at)`, which only matches when two sources report exactly the same timestamp. Different providers use different receivers and have different delays, so they almost never do. Both copies would be stored, and Redis live state, which accepts whichever report is newer, would jump between sources. That noise would feed straight into proximity and deviation checks.

If a second live source is ever needed, the safer model is one active source per aircraft at a time, with the source recorded on each position, and a failover rule rather than a merge. A shared provider adapter layer should wait until a second live source actually exists, not be built ahead of it.

---

## Open Questions

- Does OpenSky treat Sentinel's live pipeline as operational use that needs a written license?
- Is a public portfolio demo compatible with FlightAware's Personal tier, and with adsb.fi's or adsb.lol's terms?
- How should Sentinel signal provider health so the Alert Evaluator can suppress signal loss during a provider outage, without hiding a real aircraft going dark?
- What box size and poll interval give an acceptable picture over the region Sentinel actually demonstrates?

---

## Consequences If Accepted

- ADR-013's provider scope narrows to OpenSky for live aviation positions, with the credit model documented.
- The poller's defaults (box size, interval) and rate-limit handling change. The config comment about anonymous limits is corrected.
- Provider health becomes a design item before or alongside any failover work.
- ADR-015 is revisited when Phase 04 resumes, with FlightAware filed routes as the candidate real route source.
- No change to Kafka topics, the canonical position schema, or any downstream service.

---

## Sources

Checked 2026-09-22. OpenSky limits and FlightAware pricing were read from the providers' own pages. Flightradar24 plan figures and terms, and OpenSky's terms, came from search results because those pages did not render when fetched. Re-check them before spending money or relying on the terms.

- OpenSky REST API documentation (limits, credits, headers): https://openskynetwork.github.io/opensky-api/rest.html
- OpenSky terms of use: https://opensky-network.org/about/terms-of-use
- Flightradar24 API subscriptions and credits: https://fr24api.flightradar24.com/subscriptions-and-credits
- Flightradar24 API credit overview: https://fr24api.flightradar24.com/docs/credit-overview
- Flightradar24 terms and conditions: https://www.flightradar24.com/terms-and-conditions
- FlightAware AeroAPI pricing: https://www.flightaware.com/commercial/aeroapi/
- adsb.fi open data API: https://github.com/adsbfi/opendata/blob/main/README.md
- adsb.lol API documentation: https://www.adsb.lol/docs/open-data/api/
