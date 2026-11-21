#!/usr/bin/env python3
"""analyze.py - ONE shared analyzer for every EventPulse k6 experiment.

WHAT IT DOES
------------
A k6 run (started by loadtests/k6/runner.sh) writes a --summary-export json file. Reading those
json files by hand is slow and eats tokens, so this script turns each one into ONE ROW of a
tab-separated file (summary.tsv) and prints the whole file as a compact table:

    row       one json  ->  one line (appended to --out, or printed)
    table     summary.tsv -> aligned table for the terminal
    suggest   coarse-pass summary.tsv -> the rates for a finer pass (RATES=...)

Every row has the same fixed columns, so experiments can be compared:

    testid phase [--var columns] rate achieved_rps accepted_rps reqs n<ok> n<fail> n_other
    err_pct p50_ms p95_ms p99_ms max_ms dropped rows audit [--extra / hook columns] verdict

  achieved_rps  all answers per second (k6 http_reqs rate). It also counts the error answers.
  accepted_rps  only the answers with the OK status code per second. When a server sheds load
                with 503 this is the real capacity; achieved_rps would hide it.
  verdict       OK, or COLLAPSED:<why> (error % above a limit, p99 above a limit, lost rows),
                plus "dropped=N" if k6 could not start every planned request.

WHAT THE k6 SCRIPT MUST PROVIDE
-------------------------------
Three response-class counters (the experiment's script.js creates them):

    <prefix>resp_<ok-code>   e.g. b3_resp_202
    <prefix>resp_<fail-code> e.g. b3_resp_503
    <prefix>resp_other       everything else (4xx, 500, status 0 = timeout)

Use --prefix b3 for the "b3_" prefix, --ok-code / --fail-code for other status codes.

HOW TO REUSE IT FOR A NEW EXPERIMENT (do not copy this file, do not write a new analyzer)
---------------------------------------------------------------------------------------
Level 1 - flags only (covers most experiments):
    --var NAME=VALUE      the thing being compared (pool=25, latency_ms=50, ...). Repeatable.
                          Each one becomes a column right after "phase".
    --extra NAME=VALUE    a column computed OUTSIDE this script (e.g. err_types="pool_timeout=12")
                          placed before "verdict". Repeatable.
    --rows N              rows the DB really holds for this run -> fills the audit column
                          (ok == rows is "ok", more rows = committed but not acknowledged,
                          fewer = LOST). Leave it out when the experiment has no database audit.
    --p99-limit-ms / --err-limit-pct   what "collapsed" means for this experiment.

Level 2 - a hook file, when a column needs the k6 numbers themselves:
    --hook my_hook.py     a python file that may define either or both functions
        extra_columns(metrics, args) -> list of (name, value)
            metrics = the k6 summary "metrics" dict, args = the parsed command line.
            The returned columns are added before "verdict".
            Example: [("little_inflight", f"{rate * p50 / 1000:.1f}")]
        verdict(metrics, args, default_verdict) -> str
            return your own verdict text (default_verdict is the built-in one).
    Keep hook files next to the experiment (loadtests/k6/stage-N/<ID>/), not in this folder.

Level 3 - a capability every experiment could use: add a NEW OPTIONAL flag or subcommand here.
    Old command lines must keep working (existing experiments depend on this file), and every
    run of one summary.tsv must use the same columns: row refuses to append if the header differs.

Typical call from a run.sh:
    python3 loadtests/k6/analyze.py row --json "$lap_json" --testid "$testid" \
        --phase measured --rate "$RATE" --prefix b3 --var latency_ms=50 --rows "$rows" \
        --extra "err_types=$err_types" --out "$OUT/summary.tsv"
    python3 loadtests/k6/analyze.py table "$OUT/summary.tsv"

NOTES
-----
- Not verified for every k6 version: the layout of the summary export differs between versions,
  so val() looks for each number in both places known to exist (flat, and under "values").
- B1 and B2 keep their own older analyze.py; they were not migrated.
"""

import argparse
import importlib.util
import json
import os
import sys

DEFAULT_P99_LIMIT_MS = 1000.0  # "collapsed": p99 above 1 s
DEFAULT_ERR_LIMIT_PCT = (
    1.0  # ... or more than 1 % of requests not answered with the OK code
)


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


def pairs(items, what):
    """['a=1', 'b=2'] -> [('a','1'), ('b','2')]; an empty value becomes 'none'."""
    out = []
    for it in items or []:
        if "=" not in it:
            sys.exit(f"--{what} expects NAME=VALUE, got {it!r}")
        k, v = it.split("=", 1)
        out.append((k.strip(), v.strip() or "none"))
    return out


def f1(x):
    return "n/a" if x is None else f"{x:.1f}"


def load_hook(path):
    if not path:
        return None
    spec = importlib.util.spec_from_file_location("analyze_hook", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def build_row(a):
    """Return (columns, values) for one run."""
    with open(a.json) as f:
        d = json.load(f)
    m = d.get("metrics", d)
    pre = (a.prefix + "_") if a.prefix else ""
    ok, fail = str(a.ok_code), str(a.fail_code)

    reqs = val(m.get("http_reqs"), "count") or 0
    rps = val(m.get("http_reqs"), "rate") or 0
    n_ok = val(m.get(f"{pre}resp_{ok}"), "count") or 0
    n_fail = val(m.get(f"{pre}resp_{fail}"), "count") or 0
    other = val(m.get(f"{pre}resp_other"), "count") or 0
    dropped = val(m.get("dropped_iterations"), "count") or 0
    dur = m.get("http_req_duration")
    p50 = val(dur, "p(50)", "med")
    p95 = val(dur, "p(95)")
    p99 = val(dur, "p(99)")
    mx = val(dur, "max")
    err = 100.0 * (reqs - n_ok) / reqs if reqs else 100.0
    # Real test length = requests / rate. accepted_rps = OK answers per second of that length.
    seconds = reqs / rps if rps else 0
    accepted = n_ok / seconds if seconds else 0

    # Audit: every OK response must be a committed row. More rows than OKs = committed but
    # the client never heard. No --rows given = no audit for this experiment.
    if a.rows is None:
        rows, audit = "n/a", "n/a"
    else:
        rows = int(a.rows)
        if rows == n_ok:
            audit = "ok"
        elif rows > n_ok:
            audit = f"+{rows - n_ok} committed-not-acked"
        else:
            audit = f"LOST {n_ok - rows}"

    why = []
    if err > a.err_limit_pct:
        why.append(f"err>{a.err_limit_pct:g}%")
    judge = p99 if p99 is not None else p95  # fall back to p95 if p99 is not exported
    if judge is not None and judge > a.p99_limit_ms:
        why.append(
            ("p99" if p99 is not None else "p95") + f">{a.p99_limit_ms / 1000:g}s"
        )
    if audit.startswith("LOST"):
        why.append("LOST-ROWS")
    verdict = "OK" if not why else "COLLAPSED:" + ",".join(why)
    if dropped > 0:
        verdict += f" | dropped={int(dropped)} (check k6 CPU: generator or VU cap?)"

    hook = load_hook(a.hook)
    if hook is not None and hasattr(hook, "verdict"):
        verdict = hook.verdict(m, a, verdict)

    vars_ = pairs(a.var, "var")
    extras = pairs(a.extra, "extra")
    if hook is not None and hasattr(hook, "extra_columns"):
        extras += [(str(k), str(v)) for k, v in hook.extra_columns(m, a)]
    cols = (
        ["testid", "phase"]
        + [k for k, _ in vars_]
        + [
            "rate",
            "achieved_rps",
            "accepted_rps",
            "reqs",
            f"n{ok}",
            f"n{fail}",
            "n_other",
            "err_pct",
            "p50_ms",
            "p95_ms",
            "p99_ms",
            "max_ms",
            "dropped",
            "rows",
            "audit",
        ]
        + [k for k, _ in extras]
        + ["verdict"]
    )
    values = (
        [a.testid, a.phase]
        + [v for _, v in vars_]
        + [
            a.rate,
            f"{rps:.0f}",
            f"{accepted:.0f}",
            int(reqs),
            int(n_ok),
            int(n_fail),
            int(other),
            f"{err:.2f}",
            f1(p50),
            f1(p95),
            f1(p99),
            f1(mx),
            int(dropped),
            rows,
            audit,
        ]
        + [v for _, v in extras]
        + [verdict]
    )
    return cols, [str(v) for v in values]


def cmd_row(a):
    cols, values = build_row(a)
    if not a.out:
        print("\t".join(values))
        return
    header = "\t".join(cols)
    if os.path.exists(a.out) and os.path.getsize(a.out) > 0:
        with open(a.out) as f:
            if f.readline().rstrip("\n") != header:
                sys.exit(
                    f"{a.out}: existing header differs from this row's columns. "
                    "Use the same --var/--extra/--hook/--ok-code/--fail-code for every run of one summary."
                )
        mode = "a"
    else:
        mode = "w"
    with open(a.out, mode) as f:
        if mode == "w":
            f.write(header + "\n")
        f.write("\t".join(values) + "\n")


def read_rows(path):
    out = []
    with open(path) as f:
        header = f.readline().rstrip("\n").split("\t")
        for line in f:
            parts = line.rstrip("\n").split("\t")
            if len(parts) == len(header):
                out.append(dict(zip(header, parts)))
    return out


def header_of(path):
    with open(path) as f:
        return f.readline().rstrip("\n").split("\t")


def cmd_table(a):
    rows = read_rows(a.tsv)
    hdr = header_of(a.tsv)
    vars_ = (
        hdr[hdr.index("phase") + 1 : hdr.index("rate")]
        if "phase" in hdr and "rate" in hdr
        else []
    )
    extras = (
        hdr[hdr.index("audit") + 1 : hdr.index("verdict")]
        if "audit" in hdr and "verdict" in hdr
        else []
    )
    show = (
        ["testid"]
        + vars_
        + [
            "rate",
            "achieved_rps",
            "accepted_rps",
            "err_pct",
            "p50_ms",
            "p99_ms",
            "dropped",
            "audit",
        ]
        + extras
        + ["verdict"]
    )
    show = [c for c in show if c in hdr]
    widths = {c: max([len(c)] + [len(r[c]) for r in rows]) for c in show}
    print("  ".join(c.ljust(widths[c]) for c in show))
    for r in rows:
        print("  ".join(r[c].ljust(widths[c]) for c in show))


def cmd_suggest(a):
    rows = [r for r in read_rows(a.tsv) if r["phase"] == "measured"]
    rates = sorted({int(r["rate"]) for r in rows})
    if not rates:
        sys.exit("no measured rows in " + a.tsv)
    ok = {}
    for r in rows:
        ok.setdefault(int(r["rate"]), []).append(r["verdict"].startswith("OK"))
    knee = None
    for (
        rt
    ) in (
        rates
    ):  # highest rate before the first collapse (any run at that rate collapsing counts)
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


def main():
    ap = argparse.ArgumentParser(
        description="Shared k6 summary analyzer. Read the docstring at the top of this file."
    )
    sub = ap.add_subparsers(dest="cmd", required=True)

    r = sub.add_parser("row")
    for k in ("json", "testid", "phase", "rate"):
        r.add_argument("--" + k, required=True)
    r.add_argument(
        "--rows", default=None, help="rows committed by this run (omit = no audit)"
    )
    r.add_argument(
        "--var",
        action="append",
        help="NAME=VALUE, the thing being compared (repeatable)",
    )
    r.add_argument(
        "--extra",
        action="append",
        help="NAME=VALUE, extra column before verdict (repeatable)",
    )
    r.add_argument(
        "--hook", default=None, help="python file with extra_columns() and/or verdict()"
    )
    r.add_argument(
        "--prefix", default="", help="counter prefix, e.g. b3 -> b3_resp_202"
    )
    r.add_argument("--ok-code", type=int, default=202)
    r.add_argument("--fail-code", type=int, default=503)
    r.add_argument("--p99-limit-ms", type=float, default=DEFAULT_P99_LIMIT_MS)
    r.add_argument("--err-limit-pct", type=float, default=DEFAULT_ERR_LIMIT_PCT)
    r.add_argument(
        "--out", default=None, help="append to this summary.tsv instead of printing"
    )
    r.set_defaults(fn=cmd_row)

    t = sub.add_parser("table")
    t.add_argument("tsv")
    t.set_defaults(fn=cmd_table)

    s = sub.add_parser("suggest")
    s.add_argument("tsv")
    s.add_argument("out")
    s.set_defaults(fn=cmd_suggest)

    args = ap.parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()
