"""B3 hook for loadtests/k6/scripts/analyze.py (passed with --hook).

Adds two columns to every summary row:
  cap_pred_rps  prediction: pool / (h0 + latency). h0 = the hold time of one request with no added
                latency (INSERT + commit wait, 4.5 to 5.4 ms in B2). H0_MS env var, default 5.0.
  little_L      Little's Law: mean requests in the system = achieved rate x mean latency.
                Compare it with the in-flight gauge in Grafana.
"""

import os


def _var(args, name):
    for item in args.var or []:
        k, _, v = item.partition("=")
        if k == name:
            return float(v)
    return None


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


def extra_columns(m, args):
    lat = _var(args, "latency_ms")
    pool = _var(args, "pool")
    h0 = float(os.environ.get("H0_MS", "5.0"))
    cap = (
        "n/a" if lat is None or pool is None else f"{pool / ((h0 + lat) / 1000.0):.0f}"
    )
    rate = _num(m.get("http_reqs"), "rate")
    avg = _num(m.get("http_req_duration"), "avg")
    little = "n/a" if rate is None or avg is None else f"{rate * avg / 1000.0:.1f}"
    return [("cap_pred_rps", cap), ("little_L", little)]
