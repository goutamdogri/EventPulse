"""B6 hook for loadtests/k6/analyze.py (passed with --hook).

Adds columns to every summary row:
  cap_pred_rps   my prediction of the max accepted rps for the sync mode of the row.
                 on : 4300 (B3 measured 25 / 5.8 ms hold time at pool 25).
                 off: 5500 (Node CPU limit, about 0.17 ms per request; the pool and Postgres are no longer the limit).
                 Only meaningful for the "over" phase; "at" and the crash probes just offer a fixed rate.
  little_L       Little's Law: mean requests in the system = achieved rate x mean latency.
"""

import os

CAP_BY_SYNC = {"on": 4300, "off": 5500}


def _var(args, name):
    for item in args.var or []:
        k, _, v = item.partition("=")
        if k == name:
            return v
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
    sync = _var(args, "sync")
    cap = CAP_BY_SYNC.get(sync)
    cap = "n/a" if cap is None else str(int(os.environ.get("CAP_" + sync.upper(), cap)))
    rate = _num(m.get("http_reqs"), "rate")
    avg = _num(m.get("http_req_duration"), "avg")
    little = "n/a" if rate is None or avg is None else f"{rate * avg / 1000.0:.1f}"
    return [("cap_pred_rps", cap), ("little_L", little)]
