"""B10 hook for loadtests/k6/analyze.py (passed with --hook).

The k6 summary has totals only. B10 is also about what the SERVER holds in memory and whether it keeps
working for clients that already gave up, so this hook asks Prometheus (server metrics are scraped every
few seconds) for the run window, and uses numbers run.sh passes in environment variables:

  PROM_URL          Prometheus, default http://localhost:9090
  B10_START_EPOCH   when k6 started
  B10_END_EPOCH     when k6 finished
  B10_ROWS          rows in the events table after the server had drained
  B10_DRAIN_S       seconds after k6 finished until the server had nothing queued or running

Columns added to every summary row:
  hung_pct      share of requests that got no answer before the k6 timeout (status 0)
  stale_rows    rows in the table minus requests that got a 202 = events stored for clients that gave up
                (or got a 503/error). Only a hint for "work done for nobody"; see the Explanation.
  peak_waiting  highest ep_pg_pool_waiting seen in the run window (requests queued for a pool connection)
  peak_inflight highest http_requests_in_flight in the run window
  rss_growth_mb highest process_resident_memory_bytes in the window minus its value at the start
  drain_s       seconds after k6 finished until the server had nothing left to do
Prometheus columns are "n/a" if Prometheus has no data for the window.
"""

import json
import os
import sys
import urllib.parse
import urllib.request


def _num(metric, *keys):
    if not metric:
        return None
    for k in keys:
        v = metric.get(k)
        if v is None:
            v = (metric.get("values") or {}).get(k)
        if v is not None:
            return v
    return None


def _range(prom, expr, start, end):
    q = urllib.parse.urlencode({"query": expr, "start": start, "end": end, "step": 1})
    with urllib.request.urlopen(f"{prom}/api/v1/query_range?{q}", timeout=10) as r:
        data = json.load(r)["data"]["result"]
    out = []
    for series in data:
        out += [float(v) for _, v in series["values"]]
    return out


def extra_columns(m, args):
    cols = []
    it_n = _num(m.get("iterations"), "count")
    hung = _num(m.get("b10_resp_hung"), "count") or 0
    ok = _num(m.get("b10_resp_202"), "count") or 0
    cols.append(("hung_pct", "n/a" if not it_n else f"{100 * hung / it_n:.1f}"))

    rows = os.environ.get("B10_ROWS")
    cols.append(("stale_rows", "n/a" if rows is None else str(int(rows) - int(ok))))

    peak_w = peak_f = rss = "n/a"
    try:
        prom = os.environ.get("PROM_URL", "http://localhost:9090")
        t0 = float(os.environ["B10_START_EPOCH"])
        t1 = float(os.environ["B10_END_EPOCH"]) + 5
        job = 'job="eventpulse-nodejs"'
        w = _range(prom, f"max(ep_pg_pool_waiting{{{job}}})", t0, t1)
        f = _range(prom, f'sum(http_requests_in_flight{{{job}}})', t0, t1)
        r = _range(prom, f"max(process_resident_memory_bytes{{{job}}})", t0, t1)
        if w:
            peak_w = f"{max(w):.0f}"
        if f:
            peak_f = f"{max(f):.0f}"
        if r:
            rss = f"{(max(r) - r[0]) / 1e6:.0f}"
    except Exception as e:  # keep the row even if Prometheus is down
        print(f"hook.py: Prometheus columns skipped ({e})", file=sys.stderr)
    cols += [("peak_waiting", peak_w), ("peak_inflight", peak_f), ("rss_growth_mb", rss)]
    cols.append(("drain_s", os.environ.get("B10_DRAIN_S", "n/a")))
    return cols
