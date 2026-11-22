"""B9 hook for loadtests/k6/analyze.py (passed with --hook).

The k6 summary only has totals for the whole run. B9 is about what happens over time, so this hook also asks
Prometheus (the k6 metrics are pushed there every second) for the time series of ONE run.

run.sh passes the run's facts through environment variables:
  PROM_URL            Prometheus, default http://localhost:9090
  B9_BASE_RATE        original rate (new events per second)
  B9_ADDED_EPOCH      when the latency toxic was added
  B9_REMOVED_EPOCH    when it was deleted
  B9_END_EPOCH        when k6 finished
  B9_ROWS             rows in the events table after the run

Columns added to every summary row:
  amplification   attempts / iterations over the whole run (1.00 = no retries sent)
  peak_offered_x  highest attempts/s after the toxic was added, divided by the original rate
  recovery_s      seconds from toxic removal until attempts/s is back within 10 % of the original rate AND at least
                  99 % of the attempts get a 202, for 10 seconds in a row. "never" = did not happen before the run ended.
  tail_ok_pct     share of attempts that got a 202 in the last 20 s of the run
  stale_rows      rows in the table minus iterations that ended with a 202 = events the database stored although the
                  client gave up on them (work whose answer nobody read)
Every time-series column is "n/a" if Prometheus has no data for the run (series are filtered by testid).
"""

import json
import os
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
    out = {}
    for series in data:  # sum() already makes it one series; merge anyway
        for t, v in series["values"]:
            out[int(float(t))] = out.get(int(float(t)), 0.0) + float(v)
    return out


def _series(testid):
    prom = os.environ.get("PROM_URL", "http://localhost:9090")
    added = float(os.environ["B9_ADDED_EPOCH"])
    end = float(os.environ["B9_END_EPOCH"]) + 5
    sel = f'{{testid="{testid}"}}'
    att = _range(prom, f"sum(rate(k6_b9_attempts_total{sel}[5s]))", added - 5, end)
    ok = _range(prom, f"sum(rate(k6_b9_resp_202_total{sel}[5s]))", added - 5, end)
    return att, ok


def extra_columns(m, args):
    cols = []
    att_n = _num(m.get("b9_attempts"), "count")
    it_n = _num(m.get("iterations"), "count")
    cols.append(("amplification", "n/a" if not att_n or not it_n else f"{att_n / it_n:.2f}"))

    peak = rec = tail = "n/a"
    try:
        base = float(os.environ["B9_BASE_RATE"])
        added = int(float(os.environ["B9_ADDED_EPOCH"]))
        removed = int(float(os.environ["B9_REMOVED_EPOCH"]))
        end = int(float(os.environ["B9_END_EPOCH"]))
        att, ok = _series(args.testid)
        if att:
            peak = f"{max(v for t, v in att.items() if t >= added) / base:.2f}"
            good = {}
            for t in range(removed, end):
                a, o = att.get(t), ok.get(t, 0.0)
                good[t] = a is not None and a > 0 and a <= 1.1 * base and o / a >= 0.99
            rec = "never"
            for t in range(removed, end - 9):
                if all(good[t + i] for i in range(10)):
                    rec = str(t - removed)
                    break
            a20 = sum(att.get(t, 0.0) for t in range(end - 20, end))
            o20 = sum(ok.get(t, 0.0) for t in range(end - 20, end))
            tail = "n/a" if a20 <= 0 else f"{100 * o20 / a20:.1f}"
    except Exception as e:  # keep the row even if Prometheus is down; the reason goes to stderr
        import sys

        print(f"hook.py: time-series columns skipped ({e})", file=sys.stderr)
    cols += [("peak_offered_x", peak), ("recovery_s", rec), ("tail_ok_pct", tail)]

    stale = "n/a"
    rows = os.environ.get("B9_ROWS")
    ok_n = _num(m.get("b9_unique_ok"), "count")
    if rows and ok_n is not None:
        stale = str(int(rows) - int(ok_n))
    cols.append(("stale_rows", stale))
    return cols
