#!/usr/bin/env python3
"""B1 helper: turns one k6 --summary-export file into one summary row, and
(after the coarse pass) suggests the rates for the fine pass.

  analyze.py row     --json FILE --testid T --phase measured --rate 500 --rows 29980
  analyze.py table   summary.tsv
  analyze.py suggest summary.tsv suggestion.env

I wrote this in python because parsing JSON in bash is painful.
Not verified: the exact layout of k6's summary-export differs between k6
versions, so val() looks for the number in both places I know about.
"""

import json
import sys

COLS = [
    "testid",
    "phase",
    "rate",
    "achieved_rps",
    "reqs",
    "n202",
    "n503",
    "n_other",
    "err_pct",
    "p50_ms",
    "p95_ms",
    "p99_ms",
    "max_ms",
    "dropped",
    "rows",
    "audit",
    "verdict",
]

P99_LIMIT_MS = 1000.0  # my definition of "collapsed": p99 above 1 s
ERR_LIMIT_PCT = 1.0  # ... or more than 1 % of requests not answered with 202


def val(metric, *keys):
    """Return the first key found, looking at the flat layout and at metric['values']."""
    if not metric:
        return None
    for k in keys:
        if k in metric:
            return metric[k]
        v = (metric.get("values") or {}).get(k)
        if v is not None:
            return v
    return None


def row(a):
    with open(a.json) as f:
        d = json.load(f)
    m = d.get("metrics", d)
    reqs = val(m.get("http_reqs"), "count") or 0
    rps = val(m.get("http_reqs"), "rate") or 0
    n202 = val(m.get("b1_resp_202"), "count") or 0
    n503 = val(m.get("b1_resp_503"), "count") or 0
    other = val(m.get("b1_resp_other"), "count") or 0
    dropped = val(m.get("dropped_iterations"), "count") or 0
    dur = m.get("http_req_duration")
    p50 = val(dur, "p(50)", "med")
    p95 = val(dur, "p(95)")
    p99 = val(dur, "p(99)")
    mx = val(dur, "max")
    err = 100.0 * (reqs - n202) / reqs if reqs else 100.0
    rows = int(a.rows)

    # Audit: every 202 must be a row. More rows than 202s = committed but the client never heard.
    if rows == n202:
        audit = "ok"
    elif rows > n202:
        audit = f"+{rows - n202} committed-not-acked"
    else:
        audit = f"LOST {n202 - rows}"

    why = []
    if err > ERR_LIMIT_PCT:
        why.append(f"err>{ERR_LIMIT_PCT:g}%")
    judge = p99 if p99 is not None else p95  # fall back to p95 if p99 is not exported
    if judge is not None and judge > P99_LIMIT_MS:
        why.append(("p99" if p99 is not None else "p95") + ">1s")
    if audit.startswith("LOST"):
        why.append("LOST-ROWS")
    flags = []
    if dropped > 0:
        flags.append(f"dropped={int(dropped)} (check k6 CPU: generator or VU cap?)")
    verdict = "OK" if not why else "COLLAPSED:" + ",".join(why)
    if flags:
        verdict += " | " + "; ".join(flags)

    def f1(x):
        return "n/a" if x is None else f"{x:.1f}"

    vals = [
        a.testid,
        a.phase,
        a.rate,
        f"{rps:.0f}",
        int(reqs),
        int(n202),
        int(n503),
        int(other),
        f"{err:.2f}",
        f1(p50),
        f1(p95),
        f1(p99),
        f1(mx),
        int(dropped),
        rows,
        audit,
        verdict,
    ]
    print("\t".join(str(v) for v in vals))


def read_rows(path):
    out = []
    with open(path) as f:
        header = f.readline().rstrip("\n").split("\t")
        for line in f:
            parts = line.rstrip("\n").split("\t")
            if len(parts) == len(header):
                out.append(dict(zip(header, parts)))
    return out


def table(a):
    rows = read_rows(a.tsv)
    show = [
        "testid",
        "rate",
        "achieved_rps",
        "err_pct",
        "p50_ms",
        "p99_ms",
        "dropped",
        "audit",
        "verdict",
    ]
    widths = {
        c: max(len(c), *(len(r[c]) for r in rows)) if rows else len(c) for c in show
    }
    print("  ".join(c.ljust(widths[c]) for c in show))
    for r in rows:
        print("  ".join(r[c].ljust(widths[c]) for c in show))


def suggest(a):
    rows = [r for r in read_rows(a.tsv) if r["phase"] == "measured"]
    rates = sorted({int(r["rate"]) for r in rows})
    if not rates:
        sys.exit("no measured rows in " + a.tsv)
    ok = {}
    for r in rows:
        ok.setdefault(int(r["rate"]), []).append(r["verdict"].startswith("OK"))
    knee = None
    for rt in rates:  # highest rate before the first collapse
        if all(ok[rt]):
            knee = rt
        else:
            break
    note = ""
    if knee is None:
        knee = max(rates[0] // 2, 10)
        note = "# WARNING: even the lowest rate collapsed; fine pass starts lower. Look at the coarse results first.\n"
    elif knee == rates[-1]:
        note = "# WARNING: no collapse found in the coarse pass; extend the coarse rates upward first.\n"
    # Even steps (a multiple of 50 rps) from 50% of the last good rate upward, 9 points.
    lo = max(50, int(round(0.5 * knee / 50.0)) * 50)
    step = max(50, int(round((knee / 8.0) / 50.0)) * 50)
    fine = [lo + i * step for i in range(9)]
    probes = sorted(
        {int(round(0.7 * knee / 50.0) * 50) or 50, int(round(knee / 50.0) * 50) or 50}
    )
    with open(a.out, "w") as f:
        f.write(note)
        f.write(f"# last coarse rate before the first collapse: {knee}\n")
        f.write(f'RATES="{" ".join(map(str, fine))}"\n')
        f.write(f'PROBE_RATES="{" ".join(map(str, probes))}"\n')
    print(open(a.out).read())


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("row")
    for k in ("json", "testid", "phase", "rate", "rows"):
        r.add_argument("--" + k, required=True)
    r.set_defaults(fn=row)
    t = sub.add_parser("table")
    t.add_argument("tsv")
    t.set_defaults(fn=table)
    s = sub.add_parser("suggest")
    s.add_argument("tsv")
    s.add_argument("out")
    s.set_defaults(fn=suggest)
    args = ap.parse_args()
    args.fn(args)
