"""Analyze raw OpenSky vs adsb.fi responses collected by collect.py.

Everything here is derived from opensky.ndjson.gz and adsbfi.ndjson.gz.
Both providers are filtered to the same box before any comparison.
"""

import collections
import gzip
import json
import math
import os
import statistics

DIR = os.path.dirname(os.path.abspath(__file__))
BOX = dict(lamin=36.9, lomin=-122.8, lamax=38.1, lomax=-121.5)

# Mirrors the correlation worker: on each new position of A, compare against
# every other entity whose latest position is at most CANDIDATE_FRESHNESS_S
# old (relative to A's position time), using 2D great-circle distance, no
# altitude and no on-ground filter.
CANDIDATE_FRESHNESS_S = 60.0
THRESHOLDS_M = (300, 1000)
# Ground truth interpolation refuses to bridge gaps longer than this.
MAX_INTERP_GAP_S = 10.0


def in_box(lat, lon):
    return (lat is not None and lon is not None
            and BOX["lamin"] <= lat <= BOX["lamax"]
            and BOX["lomin"] <= lon <= BOX["lomax"])


def haversine_m(lat1, lon1, lat2, lon2):
    r = 6371008.8
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def pct(values, q):
    if not values:
        return None
    s = sorted(values)
    k = (len(s) - 1) * q
    f, c = math.floor(k), math.ceil(k)
    return s[f] if f == c else s[f] + (s[c] - s[f]) * (k - f)


def summary(values, unit=""):
    if not values:
        return "n/a"
    return (f"n={len(values)} min={min(values):.2f}{unit} p50={pct(values, .5):.2f}{unit} "
            f"p90={pct(values, .9):.2f}{unit} p99={pct(values, .99):.2f}{unit} max={max(values):.2f}{unit}")


def load(name):
    with gzip.open(os.path.join(DIR, f"{name}.ndjson.gz"), "rt") as f:
        return [json.loads(line) for line in f]


# ---- Normalize each snapshot to a list of aircraft observations ----------

def opensky_snapshots(records):
    snaps = []
    for r in records:
        if r["status"] != 200:
            continue
        body = json.loads(r["body"])
        obs = []
        for s in body.get("states") or []:
            alt_ft = s[7] * 3.28084 if s[7] is not None else None
            gs_kt = s[9] * 1.94384 if s[9] is not None else None
            obs.append(dict(
                id=s[0].lower(), lat=s[6], lon=s[5], pos_t=s[3],
                on_ground=bool(s[8]), alt_ft=alt_ft, gs_kt=gs_kt, raw=s,
            ))
        snaps.append(dict(req=r, resp_t=body["time"], obs=obs))
    return snaps


def adsbfi_snapshots(records):
    snaps = []
    for r in records:
        if r["status"] != 200:
            continue
        body = json.loads(r["body"])
        now = body["now"] / 1000.0
        obs = []
        for a in body.get("ac") or []:
            lat, lon = a.get("lat"), a.get("lon")
            seen_pos = a.get("seen_pos")
            pos_t = now - seen_pos if (lat is not None and seen_pos is not None) else None
            alt = a.get("alt_baro")
            obs.append(dict(
                id=a["hex"].lower(), lat=lat, lon=lon, pos_t=pos_t,
                on_ground=alt == "ground",
                alt_ft=alt if isinstance(alt, (int, float)) else None,
                gs_kt=a.get("gs"), raw=a,
            ))
        snaps.append(dict(req=r, resp_t=now, obs=obs))
    return snaps


def restrict_to_box(snaps):
    """Keep aircraft whose position is inside the box, plus position-less ones
    (kept separately so the null-position rate can be reported). Position-less
    adsb.fi aircraft cannot be placed, so the null rate for adsb.fi is over its
    whole 48 NM circle, which is stated in the report."""
    out = []
    for s in snaps:
        inside = [o for o in s["obs"] if in_box(o["lat"], o["lon"])]
        nullpos = [o for o in s["obs"] if o["lat"] is None or o["lon"] is None]
        out.append(dict(s, obs=inside, nullpos=nullpos, total=len(s["obs"])))
    return out


AIRBORNE_MIN_ALT_FT = 300
AIRBORNE_MIN_GS_KT = 80


def airborne(o):
    return (not o["on_ground"] and o["alt_ft"] is not None and o["alt_ft"] >= AIRBORNE_MIN_ALT_FT
            and o["gs_kt"] is not None and o["gs_kt"] >= AIRBORNE_MIN_GS_KT)


# ---- Position event streams ----------------------------------------------

def events_from(snaps, every_n=1):
    """Distinct new positions as a consumer would see them if it polled every
    `every_n`-th snapshot. A position is new when its (pos_t, lat, lon)
    differs from the last one seen for that aircraft."""
    last = {}
    events = []
    for i, s in enumerate(snaps):
        if i % every_n:
            continue
        for o in s["obs"]:
            if o["pos_t"] is None:
                continue
            key = (round(o["pos_t"], 1), o["lat"], o["lon"])
            if last.get(o["id"]) == key:
                continue
            last[o["id"]] = key
            events.append((o["pos_t"], o["id"], o["lat"], o["lon"], airborne(o)))
    events.sort()
    return events


def update_intervals(events):
    per = collections.defaultdict(list)
    for t, i, *_ in events:
        per[i].append(t)
    gaps = []
    for ts in per.values():
        ts.sort()
        gaps.extend(b - a for a, b in zip(ts, ts[1:]) if b > a)
    return gaps, per


# ---- Proximity: simulated worker vs interpolated ground truth -------------

def simulate_worker(events, threshold_m):
    latest = {}
    detected = {}
    for t, i, lat, lon, air in events:
        for j, (tj, latj, lonj, aj) in latest.items():
            if j == i or tj < t - CANDIDATE_FRESHNESS_S:
                continue
            d = haversine_m(lat, lon, latj, lonj)
            if d <= threshold_m:
                pair = tuple(sorted((i, j)))
                detected.setdefault(pair, (t, d, air and aj))
        latest[i] = (t, lat, lon, air)
    return detected


def ground_truth(events, max_threshold_m):
    """True minimum separation per pair from ~1 s tracks, interpolated on a
    1 s grid, only across gaps <= MAX_INTERP_GAP_S. Returns pairs whose true
    minimum separation is <= max_threshold_m, with that minimum and whether
    both were on the ground at that moment."""
    tracks = collections.defaultdict(list)
    for t, i, lat, lon, air in events:
        tracks[i].append((t, lat, lon, air))
    t0 = math.floor(min(e[0] for e in events))
    t1 = math.ceil(max(e[0] for e in events))
    idx = {i: 0 for i in tracks}
    best = {}
    cell_deg = 0.02  # about 2 km; neighbours of a cell cover > max threshold
    for t in range(t0, t1 + 1):
        pos = {}
        for i, tr in tracks.items():
            k = idx[i]
            while k + 1 < len(tr) and tr[k + 1][0] <= t:
                k += 1
            idx[i] = k
            if k + 1 >= len(tr) or tr[k][0] > t:
                continue
            a, b = tr[k], tr[k + 1]
            if b[0] - a[0] > MAX_INTERP_GAP_S:
                continue
            f = (t - a[0]) / (b[0] - a[0])
            pos[i] = (a[1] + (b[1] - a[1]) * f, a[2] + (b[2] - a[2]) * f, a[3] and b[3])  # airborne at both ends
        grid = collections.defaultdict(list)
        for i, (lat, lon, g) in pos.items():
            grid[(int(lat // cell_deg), int(lon // cell_deg))].append(i)
        for (cy, cx), members in grid.items():
            near = []
            for dy in (-1, 0, 1):
                for dx in (-1, 0, 1):
                    near.extend(grid.get((cy + dy, cx + dx), []))
            for i in members:
                for j in near:
                    if j <= i:
                        continue
                    d = haversine_m(pos[i][0], pos[i][1], pos[j][0], pos[j][1])
                    if d <= max_threshold_m and d < best.get((i, j), (math.inf,))[0]:
                        best[(i, j)] = (d, t, pos[i][2] and pos[j][2])  # both airborne
    return best


def score(detected, truth, threshold_m, airborne_only, ids=None):
    def ok(p):
        return ids is None or (p[0] in ids and p[1] in ids)
    true_pairs = {p for p, v in truth.items() if v[0] <= threshold_m and (not airborne_only or v[2]) and ok(p)}
    det_pairs = {p for p, v in detected.items() if (not airborne_only or v[2]) and ok(p)}
    tp = det_pairs & true_pairs
    fp = det_pairs - true_pairs
    fn = true_pairs - det_pairs
    return dict(truth=len(true_pairs), detected=len(det_pairs), tp=len(tp), fp=len(fp), fn=len(fn),
                fp_pairs=sorted(fp), fn_pairs=sorted(fn))


# ---- Report ----------------------------------------------------------------

def main():
    os_raw, fi_raw = load("opensky"), load("adsbfi")
    report = {}

    for name, raw in (("opensky", os_raw), ("adsbfi", fi_raw)):
        statuses = collections.Counter(r["status"] for r in raw)
        lat_ms = [r["req_end_ms"] - r["req_start_ms"] for r in raw if r["status"] == 200]
        rl_headers = sorted({h for r in raw for h in r["headers"] if "rate" in h.lower() or "retry" in h.lower()})
        span = (raw[-1]["req_start_ms"] - raw[0]["req_start_ms"]) / 1000
        print(f"\n=== {name}: HTTP")
        print(f"requests={len(raw)} over {span:.0f}s, statuses={dict(statuses)}")
        print(f"latency ms: {summary(lat_ms)}")
        print(f"rate-limit related headers seen: {rl_headers}")
        if name == "opensky":
            rem = [int(r['headers'].get('X-Rate-Limit-Remaining') or r['headers'].get('x-rate-limit-remaining'))
                   for r in raw if (r['headers'].get('X-Rate-Limit-Remaining') or r['headers'].get('x-rate-limit-remaining'))]
            if rem:
                print(f"x-rate-limit-remaining: first={rem[0]} last={rem[-1]} spent={rem[0]-rem[-1]}")
        non200 = [r for r in raw if r["status"] != 200]
        for r in non200[:5]:
            print(f"  non-200 sample: status={r['status']} headers={r['headers']} body={r['body'][:200]!r}")

    os_snaps = restrict_to_box(opensky_snapshots(os_raw))
    fi_snaps = restrict_to_box(adsbfi_snapshots(fi_raw))

    for name, snaps in (("opensky", os_snaps), ("adsbfi", fi_snaps)):
        counts = [len(s["obs"]) for s in snaps]
        ages = [s["resp_t"] - o["pos_t"] for s in snaps for o in s["obs"] if o["pos_t"] is not None]
        wall_ages = [s["req"]["req_end_ms"] / 1000 - o["pos_t"] for s in snaps for o in s["obs"] if o["pos_t"] is not None]
        nulls = sum(len(s["nullpos"]) for s in snaps)
        totals = sum(s["total"] for s in snaps)
        unique = {o["id"] for s in snaps for o in s["obs"]}
        ground = sum(1 for s in snaps for o in s["obs"] if o["on_ground"])
        n_obs = sum(counts)
        print(f"\n=== {name}: coverage and freshness (inside the box)")
        print(f"snapshots={len(snaps)} aircraft per snapshot: {summary(counts)}")
        print(f"unique aircraft over the run: {len(unique)}  on-ground share of observations: {ground / max(n_obs, 1):.1%}")
        print(f"position age at provider response time (s): {summary(ages)}")
        print(f"position age at our receipt, local clock (s): {summary(wall_ages)}")
        scope = "whole response" if name == "opensky" else "whole 48 NM circle (position-less aircraft cannot be placed in the box)"
        print(f"null-position rate over {scope}: {nulls}/{totals} = {nulls / max(totals, 1):.2%}")

    # Fields
    print("\n=== fields")
    os_fields = ["icao24", "callsign", "origin_country", "time_position", "last_contact", "longitude",
                 "latitude", "baro_altitude", "on_ground", "velocity", "true_track", "vertical_rate",
                 "sensors", "geo_altitude", "squawk", "spi", "position_source", "category"]
    os_obs = [o["raw"] for s in os_snaps for o in s["obs"]]
    print("opensky (fixed 18-field array; share non-null):")
    print("  " + ", ".join(f"{f}={sum(1 for r in os_obs if len(r) > k and r[k] is not None) / max(len(os_obs), 1):.0%}"
                           for k, f in enumerate(os_fields)))
    fi_obs = [o["raw"] for s in fi_snaps for o in s["obs"]]
    keys = collections.Counter(k for a in fi_obs for k in a)
    print(f"adsbfi ({len(keys)} distinct keys; share present):")
    print("  " + ", ".join(f"{k}={c / max(len(fi_obs), 1):.0%}" for k, c in keys.most_common()))
    types = collections.Counter(a.get("type") for a in fi_obs)
    print(f"adsbfi position source types: {dict(types)}")
    nonicao = sum(1 for a in fi_obs if a["hex"].startswith("~"))
    print(f"adsbfi non-ICAO (~) addresses: {nonicao}/{len(fi_obs)}")

    # Overlap
    os_ids = {o["id"] for s in os_snaps for o in s["obs"]}
    fi_ids = {o["id"] for s in fi_snaps for o in s["obs"]}
    print("\n=== identity overlap over the run (same box)")
    print(f"both={len(os_ids & fi_ids)} opensky_only={len(os_ids - fi_ids)} adsbfi_only={len(fi_ids - os_ids)}")

    # Update frequency and proximity streams
    streams = {
        "adsbfi @1s (as polled)": events_from(fi_snaps, 1),
        "adsbfi @5s (subsampled)": events_from(fi_snaps, 5),
        "adsbfi @25s (subsampled)": events_from(fi_snaps, 25),
        "opensky @5s (as polled)": events_from(os_snaps, 1),
        "opensky @25s (subsampled)": events_from(os_snaps, 5),
    }
    print("\n=== per-aircraft interval between distinct new positions (s)")
    for name, ev in streams.items():
        gaps, per = update_intervals(ev)
        med_per_ac = [statistics.median([b - a for a, b in zip(ts, ts[1:])]) for ts in per.values() if len(ts) > 2]
        print(f"{name}: events={len(ev)} all gaps {summary(gaps)}")
        print(f"    median gap per aircraft: {summary(med_per_ac)}")

    truth_events = streams["adsbfi @1s (as polled)"]
    truth = ground_truth(truth_events, max(THRESHOLDS_M))
    print("\n=== proximity: simulated worker vs ground truth")
    print("ground truth = adsb.fi ~1 s tracks, linearly interpolated on a 1 s grid (gaps <= 10 s only).")
    print("OpenSky streams are scored against adsb.fi-derived truth, so they mix sampling effects with provider differences;")
    print("the adsb.fi subsampled streams isolate the sampling-rate effect alone.")
    common = {i for i in (os_ids & fi_ids) if not i.startswith("~")}
    print(f"fair set: ICAO addresses seen by both providers = {len(common)}")
    tilde_truth = sum(1 for p, v in truth.items() if v[0] <= 300 and (p[0].startswith('~') or p[1].startswith('~')))
    print(f"truth pairs <= 300 m involving a non-ICAO (~) track: {tilde_truth}")
    for scope_name, ids in (("all adsb.fi aircraft (as the worker would see them)", None),
                            ("fair: ICAO aircraft seen by both providers", common)):
        for thr in THRESHOLDS_M:
            for airborne_only in (False, True):
                label = f"[{scope_name}] threshold {thr} m, {'airborne pairs only' if airborne_only else 'all pairs incl. ground'}"
                print(f"\n-- {label}")
                for name, ev in streams.items():
                    det = simulate_worker(ev, thr)
                    sc = score(det, truth, thr, airborne_only, ids)
                    print(f"{name:28s} truth={sc['truth']:4d} detected={sc['detected']:4d} tp={sc['tp']:4d} fp={sc['fp']:4d} fn={sc['fn']:4d}")
    close = sorted(((v[0], p) for p, v in truth.items() if v[0] <= 300 and v[2] and p[0] in common and p[1] in common))
    print(f"\nfair-set airborne pairs with true min separation <= 300 m: {len(close)}")
    for d, p in close[:20]:
        print(f"  {p} min={d:.0f} m")


if __name__ == "__main__":
    main()
