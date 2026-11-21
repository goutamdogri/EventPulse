"""B5 hook for loadtests/k6/analyze.py (passed with --hook).

Adds columns to every summary row:
  cap_pred_rps   prediction of max accepted rps. B3 measured 4300 req/s at pool 25 WITH one secondary index
                 (events_tenant_time_idx already existed then), so with the primary key only I take 4300 * 1.12 = 4820.
                 Cost model: every b-tree index adds 12 % CPU per row, a GIN index counts as 3 b-trees.
                 capacity = BASE_RPS / (1 + 0.12 * k).  k per level: 0 -> 0, 1 -> 1, 3 -> 3,
                 5 -> 4 b-trees + GIN = 7, 1g (idx1 + GIN probe) -> 1 + 3 = 4.
                 Only meaningful for the "over" phase; the "under" phase just offers a fixed rate.
  wal_pred_B     prediction of WAL bytes per row: 0 -> 700, 1 -> 790, 3 -> 970, 5 -> 1260, 1g -> 990.
  little_L       Little's Law: mean requests in the system = achieved rate x mean latency.
"""

import os

K_BY_LEVEL = {"0": 0, "1": 1, "3": 3, "5": 7, "1g": 4}
WAL_BY_LEVEL = {"0": 700, "1": 790, "3": 970, "5": 1260, "1g": 990}


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
    level = _var(args, "idx")
    base = float(os.environ.get("BASE_RPS", "4820"))
    k = K_BY_LEVEL.get(level)
    cap = "n/a" if k is None else f"{base / (1 + 0.12 * k):.0f}"
    wal = WAL_BY_LEVEL.get(level)
    wal = "n/a" if wal is None else str(wal)
    rate = _num(m.get("http_reqs"), "rate")
    avg = _num(m.get("http_req_duration"), "avg")
    little = "n/a" if rate is None or avg is None else f"{rate * avg / 1000.0:.1f}"
    return [("cap_pred_rps", cap), ("wal_pred_B", wal), ("little_L", little)]
