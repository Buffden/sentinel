# OpenSky Fallback Hardening Debrief

Evidence from verifying CP2 on 2026-09-23, against local Redpanda and the live OpenSky API. The design is in [opensky-fallback-hardening.md](opensky-fallback-hardening.md).

---

## Before any code: a real `429`

The provider experiment had recorded OpenSky's credit balance header but never a real `429`. The claim that a `429` carries a retry time came from OpenSky's documentation. The pause design depended on that header, so a real `429` was produced first.

**Method.** First, one anonymous request over the SF Bay box. Then more anonymous requests with a 1 second pause between them, until OpenSky refused one, capped at 450 attempts. This spent only the anonymous budget for this machine's IP address. The logged-in account was not touched.

**What came back.** The first request returned `200` with `x-rate-limit-remaining: 399`. The last lines of the loop:

```text
20:42:58 #396 HTTP/2 200  x-rate-limit-remaining: 3
20:42:59 #397 HTTP/2 200  x-rate-limit-remaining: 2
20:43:01 #398 HTTP/2 200  x-rate-limit-remaining: 1
20:43:03 #399 HTTP/2 200  x-rate-limit-remaining: 0
20:43:04 #400 HTTP/2 429  x-rate-limit-retry-after-seconds: 31952
```

One more request at 20:43:17 UTC captured the full `429`:

```text
HTTP/2 429
date: Wed, 23 Sep 2026 20:43:18 GMT
content-type: text/plain;charset=UTF-8
content-length: 17
x-rate-limit-retry-after-seconds: 31939
cache-control: no-cache, no-store, max-age=0, must-revalidate
(standard CORS and security headers omitted)

Too many requests
```

**What it showed:**

- The budget was exactly 400 one-credit calls. OpenSky refused only after the balance reached 0, and did not refuse early.
- The `429` carries `X-Rate-Limit-Retry-After-Seconds` as a whole number of seconds.
- The `429` carries no `X-Rate-Limit-Remaining`.
- Both retry values point to the same moment: 20:43:04 + 31,952 s and 20:43:18 + 31,939 s both land on 2026-09-24 05:35:36/37 UTC. The retry time is a countdown to a fixed refill moment, not a fresh wait from each request.

These results set the design: the retry header is the wait, the balance is never expected on a `429`, and a guessed backoff is only for when the header is missing.

---

## Automated checks

```text
$ cd services/ingestion-poller
$ npx tsc --noEmit -p .
(no output, exit 0)

$ npx prettier --check src
All matched files use Prettier code style!

$ npm test
 Test Files  3 passed (3)
      Tests  45 passed (45)
```

The suite had 21 tests before this checkpoint and has 45 now. The 24 new tests are in `src/poller.test.ts`:

- **Credit bands:** the band edges.
- **Budget projection:** the SF Bay box at 25 seconds projects 3,456 credits, inside the 4,000 logged-in budget and over the 400 anonymous budget. The old Europe box at 10 seconds reproduces the 25,920 credits a day overspend.
- **Header parsing:** the real `31952`, surrounding whitespace, `0`, and rejection of empty, negative, fractional, text and overlong values.
- **Fallback backoff:** the 60 second floor, the doubling ceiling, the 15 minute cap, and a reset after one success.
- **Pause lifecycle:**
  - the real `429` pauses for exactly 31,952,000 ms;
  - a retry of 0 waits one interval;
  - an absurd value is capped at the timer limit;
  - a second `429` extends the pause rather than starting a new one;
  - a network error or `5xx` changes nothing;
  - only a success resumes, once.
- **Log lines:** the pause, extension and resume lines. The pause line uses the real run's refill time, `2026-09-24T05:35:36.000Z`.

The tests cover the pure decision functions. The wiring around them (the timer, storing the state, the fetch) needs a real HTTP call, so the two live runs below cover it.

---

## Live run 1: anonymous, budget already spent

**Method.** Redpanda was started (it had been stopped). The poller then ran with both credentials cleared in the shell, overriding `.env`:

```bash
cd services/ingestion-poller
OPENSKY_CLIENT_ID= OPENSKY_CLIENT_SECRET= npm run poll
```

The anonymous budget was still spent from the experiment above. The poller ran from 21:30:35 to 21:32:12 UTC and was then stopped with SIGINT.

**Log** (the kafkajs warning lines are discussed separately below):

```text
{"timestamp":"2026-09-23T21:30:35.011Z","level":"info","service":"ingestion-poller","message":"poller starting","url":"https://opensky-network.org/api/states/all?extended=1&lamin=36.9&lomin=-122.8&lamax=38.1&lomax=-121.5","poll_interval_ms":25000,"fetch_timeout_ms":8000,"batch_max_messages":"unlimited","authenticated":false,"backoff_base_ms":60000,"backoff_max_ms":900000,"area_square_degrees":1.56,"credits_per_call":1,"calls_per_day":3456,"projected_daily_credits":3456,"daily_budget":400,"within_budget":false,"min_sustainable_interval_ms":216000}
{"timestamp":"2026-09-23T21:30:35.011Z","level":"warn","service":"ingestion-poller","message":"projected daily credits exceed the budget, expect a 429 pause","projected_daily_credits":3456,"daily_budget":400,"min_sustainable_interval_ms":216000}
{"timestamp":"2026-09-23T21:30:35.554Z","level":"warn","service":"ingestion-poller","message":"opensky rate limited, pausing requests","http_status":429,"delay_source":"retry_header","retry_after_header":"29101","delay_ms":29101000,"resume_at":"2026-09-24T05:35:36.554Z","rate_limited_responses":1}
{"timestamp":"2026-09-23T21:32:12.860Z","level":"info","service":"ingestion-poller","message":"shutdown initiated","signal":"SIGINT"}
{"timestamp":"2026-09-23T21:32:12.861Z","level":"info","service":"ingestion-poller","message":"producer disconnected"}
```

**What it shows:**

- **Anonymous mode was selected from the shell.** `authenticated: false` even though `.env` holds credentials.
- **The projection is right.** 3,456 credits against 400, `within_budget: false`, and exactly one over-budget warning.
- **The first request got a `429`,** 543 ms after startup.
- **One pause warning,** using the retry header. The raw value was `29101`, so the delay was 29,101 × 1000 = 29,101,000 ms. That is well above the 25 second floor and below the timer cap.
- **`resume_at` came from this response,** not from a stored value. 21:30:35.554 + 29,101 s = 2026-09-24T05:35:36.554Z. This third `429` pointed to the same refill moment as the two in the experiment.
- **No requests during the pause.** For the 97 seconds until shutdown, almost four poll intervals, there were no new log lines. The `adsb.raw` high watermark was 439967 before the run and still 439967 at 21:32:06:

```text
$ docker compose exec -T redpanda rpk topic describe adsb.raw -p
PARTITION  LEADER  EPOCH  REPLICAS  LOG-START-OFFSET  HIGH-WATERMARK
0          0       19     [0]       369078            439967
```

- **Clean shutdown during the long timer.** SIGINT cleared the pending 8-hour timer, and the producer disconnected 1 ms later.

---

## Live run 2: logged in

**Method.** The poller ran with credentials loaded from `.env`, from 21:33:36 to 21:35:26 UTC, then was stopped with SIGINT:

```bash
cd services/ingestion-poller
npm run poll
```

**Log:**

```text
{"timestamp":"2026-09-23T21:33:35.990Z","level":"info","service":"ingestion-poller","message":"poller starting","url":"https://opensky-network.org/api/states/all?extended=1&lamin=36.9&lomin=-122.8&lamax=38.1&lomax=-121.5","poll_interval_ms":25000,"fetch_timeout_ms":8000,"batch_max_messages":"unlimited","authenticated":true,"backoff_base_ms":60000,"backoff_max_ms":900000,"area_square_degrees":1.56,"credits_per_call":1,"calls_per_day":3456,"projected_daily_credits":3456,"daily_budget":4000,"within_budget":true,"min_sustainable_interval_ms":21600}
{"timestamp":"2026-09-23T21:33:37.686Z","level":"info","service":"ingestion-poller","message":"poll cycle complete","state_vectors":118,"credits_remaining":3817,"topic":"adsb.raw","first_offset":"439967"}
{"timestamp":"2026-09-23T21:34:03.500Z","level":"info","service":"ingestion-poller","message":"poll cycle complete","state_vectors":117,"credits_remaining":3816,"topic":"adsb.raw","first_offset":"440085"}
{"timestamp":"2026-09-23T21:34:29.208Z","level":"info","service":"ingestion-poller","message":"poll cycle complete","state_vectors":117,"credits_remaining":3815,"topic":"adsb.raw","first_offset":"440202"}
{"timestamp":"2026-09-23T21:34:54.852Z","level":"info","service":"ingestion-poller","message":"poll cycle complete","state_vectors":117,"credits_remaining":3814,"topic":"adsb.raw","first_offset":"440319"}
{"timestamp":"2026-09-23T21:35:20.478Z","level":"info","service":"ingestion-poller","message":"poll cycle complete","state_vectors":116,"credits_remaining":3813,"topic":"adsb.raw","first_offset":"440436"}
{"timestamp":"2026-09-23T21:35:26.710Z","level":"info","service":"ingestion-poller","message":"shutdown initiated","signal":"SIGINT"}
{"timestamp":"2026-09-23T21:35:26.711Z","level":"info","service":"ingestion-poller","message":"producer disconnected"}
```

**What it shows:**

- **Credentials loaded from `.env`.** `authenticated: true`, a budget of 4,000, a projection of 3,456, `within_budget: true`, and no warning.
- **The first poll was immediate:** 1.7 seconds after startup, including the OAuth token request.
- **Five successful cycles, about 25 seconds plus request time apart:** 25.81, 25.71, 25.64 and 25.63 seconds. The next timer starts only after a cycle finishes, so the spacing is the interval plus the fetch and publish time. Requests never overlap, and the real daily spend is slightly under the projection.
- **One credit per call:** `credits_remaining` went 3817, 3816, 3815, 3814, 3813.
- **Kafka offsets account for every message.** Each cycle's first offset is the previous one plus that cycle's state-vector count (439967 + 118 = 440085, and so on). The final high watermark, 440552, is 440436 + 116.
- **No OAuth error and no `429`.**
- **Clean shutdown.**

The account started this run at 3,817 credits, not 4,000. About 183 had been spent earlier that day, most of them by the provider experiment, which uses about 180. The projection assumes a fresh day, which is why the `429` pause remains the backstop.

---

## Not tested live

- **Resuming after a real multi-hour pause.** OpenSky's retry window during both runs was about 8 hours (29,101 seconds in run 1), and nothing kept the poller running through it. The resume is covered by unit tests: `planNextPoll` ends the pause on the first success and logs it once with the pause length, and `rateLimitLogLine` produces the resume line.
- **The fallback for a missing or unusable retry header.** OpenSky sent a valid header every time. The fallback is covered by unit tests: the 60 second floor, the doubling ceiling, the 15 minute cap, and the reset after a success.
- **A successful response with no state vectors.** Every live cycle returned aircraft. This path's `credits_remaining` field is covered only by the typecheck.

No extra live requests were made just to create these conditions. Doing so would have meant keeping the poller running for about 8 hours, or spending requests against an empty budget.

---

## Separate observation: kafkajs `TimeoutNegativeWarning`

Both runs printed this to stderr during `producer.connect()`:

```text
(node:90692) TimeoutNegativeWarning: -1790199035011 is a negative number.
Timeout duration was set to 1.
```

It is not caused by CP2:

- **It fires before the poller's own code runs.** It appears during `producer.connect()`, before the first OpenSky request.
- **Bare kafkajs reproduces it.** A short script that only connects and disconnects a kafkajs producer, with no OpenSky code, printed the same warning. With `--trace-warnings`, the stack points into kafkajs 2.2.4: `RequestQueue.scheduleCheckPendingRequests` in `src/network/requestQueue/index.js`, running on Node 23.11.0.
- **It is harmless here.** Node replaces the negative delay with 1 ms, and both runs connected and published normally.

It was not fixed in this checkpoint. It matters for CP4 (consistent structured logs), because these lines are not JSON. Every service using kafkajs 2.2.4 on Node 23 probably prints it at connect, but only the ingestion poller was checked. It is recorded in the Phase 10 plan's Starting State.

---

## Engineering debrief

**Data flow.** The poller sends one request per cycle:
- **`200`:** it publishes the state vectors to `adsb.raw` and logs the remaining balance, then waits the normal interval.
- **`429`:** it reads the retry time, logs one pause warning, and sets one timer for that long. With no usable retry time, it backs off instead.
- **Any other failure:** it waits the normal interval without changing the pause state.

The first `200` after a pause logs the resume and returns to normal polling.

**Trade-off.** Waiting for the provider's retry time can mean hours with no OpenSky data. It is still the right choice, because nothing sent before the refill can succeed. The cost is that OpenSky goes quiet during a pause, and signal loss treats that as every aircraft going dark. CP3 has to address that.

**Failure behaviour.** OpenSky refused at exactly 0 credits. The refusal carried an exact retry time and no balance. The poller then made no requests and stayed silent until shutdown.

---

## Manual inspection commands

```bash
# Logged-in run: check authenticated, the projection and credits_remaining per cycle
cd services/ingestion-poller && npm run poll

# Force anonymous mode even when .env has credentials
OPENSKY_CLIENT_ID= OPENSKY_CLIENT_SECRET= npm run poll

# See OpenSky's rate-limit headers directly (anonymous, costs 1 credit if any remain)
curl -s -o /dev/null -D - 'https://opensky-network.org/api/states/all?lamin=36.9&lomin=-122.8&lamax=38.1&lomax=-121.5' | grep -i -E '^HTTP|x-rate-limit'

# Check nothing was published during a pause
docker compose exec -T redpanda rpk topic describe adsb.raw -p
```

## Knowledge-check questions

1. The anonymous run projected 3,456 credits against 400 but still only made one request. What stopped it, and what would the old poller have done instead?
2. `credits_remaining` fell by exactly 1 per cycle. What would it fall by with the old Europe box, and why?
3. Three `429`s at different times gave different retry values but the same refill moment. How can you tell that from the log alone?
4. Cycles were 25.6 to 25.8 seconds apart, not 25.0. Why is that the intended behaviour?

## Optional manual tweak

Set `POLL_INTERVAL_MS=21600` for a logged-in run and read the startup line. The projection should show exactly 4,000 credits and `within_budget: true`, the edge of what this box can sustain. Then try 21,000 and watch the warning appear. The poller sends its first request immediately, so each try costs 1 credit. Stop it after the startup lines.

## Next

CP3: provider health, failover and failback. It starts with evidence and a design discussion for its own ADR, not with code.
