"""B7 hook for loadtests/k6/analyze.py (passed with --hook).

Adds columns to every summary row:
  cap_pred_rps   my prediction (written before the run, see Explanation.md) of the max accepted rps for the body size of the row.
                 Only meaningful for the "over" phase; "at" just offers a fixed rate.
  little_L       Little's Law: mean requests in the system = achieved rate x mean latency.
"""

import os

CAP_BY_SIZE = {"200": 4400, "500": 4300, "2048": 4200, "10240": 4000, "102400": 1700}


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
    size = _var(args, "size")
    cap = CAP_BY_SIZE.get(size)
    cap = "n/a" if cap is None else str(int(os.environ.get("CAP_" + str(size), cap)))
    rate = _num(m.get("http_reqs"), "rate")
    avg = _num(m.get("http_req_duration"), "avg")
    little = "n/a" if rate is None or avg is None else f"{rate * avg / 1000.0:.1f}"
    return [("cap_pred_rps", cap), ("little_L", little)]
