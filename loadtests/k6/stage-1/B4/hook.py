"""B4 hook for loadtests/k6/analyze.py (passed with --hook).

Adds two columns to every summary row:
  cap_pred_rps  prediction. hot: min(tenants x 1000 / HOLD_MS, CEIL_RPS): one tenant can finish one
                transaction per lock-hold time, tenants work in parallel, the server/pool ceiling caps it.
                off: just CEIL_RPS. HOLD_MS env var (default 3.0 ms = UPDATE to end of COMMIT), CEIL_RPS (default 3800).
  little_L      Little's Law: mean requests in the system = achieved rate x mean latency.
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
    tenants = _var(args, "tenants")
    hot = _var(args, "hot")
    hold = float(os.environ.get("HOLD_MS", "3.0"))
    ceil = float(os.environ.get("CEIL_RPS", "3800"))
    if tenants is None or hot is None:
        cap = "n/a"
    elif hot == 1:
        cap = f"{min(tenants * 1000.0 / hold, ceil):.0f}"
    else:
        cap = f"{ceil:.0f}"
    rate = _num(m.get("http_reqs"), "rate")
    avg = _num(m.get("http_req_duration"), "avg")
    little = "n/a" if rate is None or avg is None else f"{rate * avg / 1000.0:.1f}"
    return [("cap_pred_rps", cap), ("little_L", little)]
