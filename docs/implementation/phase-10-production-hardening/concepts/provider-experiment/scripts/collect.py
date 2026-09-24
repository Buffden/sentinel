"""Collect raw OpenSky and adsb.fi responses over the same SF Bay geography.

Both providers are polled concurrently for DURATION_S. Every response is kept
raw (status, headers, body, request start/end time) in gzipped NDJSON so the
analysis can be re-run or audited without re-collecting.

Geography: the ingestion poller's local OpenSky box (lat 36.9..38.1,
lon -122.8..-121.5). adsb.fi has no box query, so it is queried with a circle
centred on the box that fully contains it (48 NM > the box's 47.5 NM
half-diagonal); the analysis filters adsb.fi aircraft back to the box.
"""

import gzip
import json
import os
import ssl
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

DURATION_S = int(os.environ.get("DURATION_S", "900"))
OUT_DIR = os.path.dirname(os.path.abspath(__file__))

BOX = dict(lamin=36.9, lomin=-122.8, lamax=38.1, lomax=-121.5)
OPENSKY_URL = (
    "https://opensky-network.org/api/states/all?extended=1&"
    + urllib.parse.urlencode(BOX)
)
OPENSKY_TOKEN_URL = (
    "https://auth.opensky-network.org/auth/realms/opensky-network/"
    "protocol/openid-connect/token"
)
# Authenticated OpenSky resolution is 5 s; polling faster returns the same data.
OPENSKY_INTERVAL_S = 5.0

ADSBFI_URL = "https://opendata.adsb.fi/api/v3/lat/37.5/lon/-122.15/dist/48"
# Documented public limit: 1 request per second.
ADSBFI_INTERVAL_S = 1.0


# This python.org build ships without CA certificates; use macOS's system
# bundle so TLS verification stays on.
SSL_CTX = ssl.create_default_context(cafile="/etc/ssl/cert.pem")
USER_AGENT = "sentinel-provider-experiment/0.1 (portfolio project)"


def fetch(url, headers=None, data=None):
    # adsb.fi's Cloudflare front end rejects Python's default user agent with
    # error 1010, so identify the client honestly instead.
    hdrs_out = {"User-Agent": USER_AGENT, **(headers or {})}
    req = urllib.request.Request(url, headers=hdrs_out, data=data)
    start = time.time()
    try:
        with urllib.request.urlopen(req, timeout=15, context=SSL_CTX) as resp:
            body = resp.read().decode("utf-8", "replace")
            status, hdrs = resp.status, dict(resp.headers)
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", "replace")
        status, hdrs = e.code, dict(e.headers)
    except Exception as e:  # timeouts, DNS, TLS
        body, status, hdrs = repr(e), None, {}
    end = time.time()
    return dict(req_start_ms=int(start * 1000), req_end_ms=int(end * 1000),
                status=status, headers=hdrs, body=body)


class Token:
    def __init__(self):
        self.value, self.expires = None, 0

    def get(self):
        if time.time() < self.expires:
            return self.value
        data = urllib.parse.urlencode(dict(
            grant_type="client_credentials",
            client_id=os.environ["OPENSKY_CLIENT_ID"],
            client_secret=os.environ["OPENSKY_CLIENT_SECRET"],
        )).encode()
        r = fetch(OPENSKY_TOKEN_URL, data=data)
        tok = json.loads(r["body"])
        self.value = tok["access_token"]
        self.expires = time.time() + tok["expires_in"] - 60
        return self.value


def run(name, interval_s, do_request, deadline):
    path = os.path.join(OUT_DIR, f"{name}.ndjson.gz")
    n = 0
    with gzip.open(path, "wt") as out:
        next_at = time.time()
        while time.time() < deadline:
            rec = do_request()
            rec["provider"] = name
            out.write(json.dumps(rec) + "\n")
            n += 1
            if rec["status"] != 200:
                print(f"{name}: HTTP {rec['status']} at {rec['req_start_ms']}", flush=True)
            # Fixed-rate schedule: next request starts interval_s after the
            # previous one started, not after it finished.
            next_at += interval_s
            time.sleep(max(0.0, next_at - time.time()))
    print(f"{name}: {n} requests written to {path}", flush=True)


def main():
    token = Token()
    token.get()
    deadline = time.time() + DURATION_S

    def opensky():
        return fetch(OPENSKY_URL, headers={"Authorization": f"Bearer {token.get()}"})

    def adsbfi():
        return fetch(ADSBFI_URL)

    threads = [
        threading.Thread(target=run, args=("opensky", OPENSKY_INTERVAL_S, opensky, deadline)),
        threading.Thread(target=run, args=("adsbfi", ADSBFI_INTERVAL_S, adsbfi, deadline)),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()


if __name__ == "__main__":
    sys.exit(main())
