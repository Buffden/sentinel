"""Deliberately exceed adsb.fi's 1 req/s public limit, briefly, and record the
real response; then measure recovery. Also probe the feeder-only snapshot
endpoint once as a separate case (expected to be refused for a non-feeder IP).

Runs after collect.py so it cannot contaminate the 1 req/s measurements.
"""

import gzip
import json
import os
import time

from collect import ADSBFI_URL, fetch

DIR = os.path.dirname(os.path.abspath(__file__))
BURST_REQUESTS = 20
RECOVERY_PROBE_EVERY_S = 2.0
RECOVERY_MAX_S = 180.0
SNAPSHOT_URL = "https://opendata.adsb.fi/api/v2/snapshot"


def main():
    out = gzip.open(os.path.join(DIR, "burst.ndjson.gz"), "wt")

    def record(phase, r):
        r["phase"] = phase
        out.write(json.dumps(r) + "\n")
        print(f"{phase}: HTTP {r['status']} {r['req_end_ms'] - r['req_start_ms']}ms "
              f"headers={ {k: v for k, v in r['headers'].items() if 'rate' in k.lower() or 'retry' in k.lower()} } "
              f"body={r['body'][:80]!r}", flush=True)
        return r

    # Back-to-back, no pause: roughly 2-3 requests per second given latency.
    for _ in range(BURST_REQUESTS):
        record("burst", fetch(ADSBFI_URL))

    # Recovery: how long until a normal request succeeds again.
    t0 = time.time()
    while time.time() - t0 < RECOVERY_MAX_S:
        time.sleep(RECOVERY_PROBE_EVERY_S)
        r = record("recovery", fetch(ADSBFI_URL))
        if r["status"] == 200:
            print(f"recovered after {time.time() - t0:.1f}s", flush=True)
            break

    time.sleep(2)
    record("feeder_snapshot", fetch(SNAPSHOT_URL))
    out.close()


if __name__ == "__main__":
    main()
