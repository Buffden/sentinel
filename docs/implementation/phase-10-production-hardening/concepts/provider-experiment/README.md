# OpenSky vs adsb.fi Provider Experiment

## Motivation

The ingestion poller kept exhausting OpenSky's daily credit budget, and adsb.fi was a free alternative with a much higher request limit. Rather than choose from documentation, both were measured side by side to decide which should be Sentinel's primary live-position source.

## Method

- **When:** 2026-09-23, 01:33 to 01:48 UTC (15 minutes), both providers polled at the same time.
- **Where:** the ingestion poller's SF Bay box (latitude 36.9 to 38.1, longitude -122.8 to -121.5). adsb.fi only supports circle queries, so it was queried with a 48 NM circle containing the box and filtered back to it.
- **Rates:** OpenSky every 5 s, logged in (its resolution). adsb.fi every 1 s (its public limit). 25 s results are simulated from every 25th snapshot.
- **Rate-limit test:** 20 back-to-back adsb.fi requests after collection.
- **Proximity:** the correlation worker's 300 m rule replayed on each stream and scored against closest approaches from adsb.fi's 1 s tracks, over the 125 ICAO aircraft both providers saw.

Scripts: [collect.py](scripts/collect.py), [burst.py](scripts/burst.py), [analyze.py](scripts/analyze.py). Full output: [analysis.txt](results/analysis.txt).

## Results

| Metric | OpenSky | adsb.fi |
| --- | ---: | ---: |
| Median latency | 580 ms | 375 ms |
| Median aircraft per snapshot | 90 | 110 |
| Unique aircraft | 132 | 173 |
| Median / P90 position age | 7.1 s / 50 s | 2.1 s / 6.3 s |
| Median update gap per aircraft | 5.0 s | 2.0 s |
| Returned fields | 18 | 48 |
| Rate-limit headers | Yes | No |
| `429` retry time given | Yes | No |
| Close pairs missed at 25 s (of 167) | 45 | 34 |

adsb.fi refused 6 of the 20 burst requests with an empty `429` and accepted the next request about 2.4 s later. Its feeder-only global snapshot returned `403` and was not measured.

## Decision

adsb.fi becomes the regional primary and OpenSky the fallback. See ADR-020.

## Caveats

- One 15-minute window, one region, one time of day.
- The proximity reference truth came from adsb.fi, and only 2 clearly airborne pairs came within 300 m; most close pairs were airport surface traffic.
- The 2.4 s recovery is a single observation, not an API contract.
- adsb.fi rejects Python's default user agent; the scripts send a descriptive one.

## Running the experiment

The raw provider responses (about 10 MB) are intentionally not versioned, so the recorded run cannot be reproduced from Git alone. Their SHA-256 hashes identify the inputs used for the recorded experiment:

```text
d0860a5a2e6c7d451c5f1bc5f7a82c22c35755b9394cdd1f0c700b9db32bc368  opensky.ndjson.gz
b713183cc4b6e1abb7074caa976339994092d992929f5f260d9d1d36903eff82  adsbfi.ndjson.gz
da25ddc85e2719130b96bebb5b1e64d2eac3ca4a59ee1e461a1fdc1f65459c85  burst.ndjson.gz
```

`results/analysis.txt` preserves the generated output of that historical run.

To perform and analyze a fresh experiment, load the poller's OpenSky credentials into the environment and, from this directory, run `python3 scripts/collect.py` (about 180 OpenSky credits) and optionally `python3 scripts/burst.py`, then `python3 scripts/analyze.py`. The scripts read and write their captures in `scripts/`.
