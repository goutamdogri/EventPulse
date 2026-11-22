"""B8 hook for loadtests/k6/analyze.py (passed with --hook).

Adds the recovery timeline of one run to its summary row. The numbers come from two probe files that run.sh writes
while k6 is running (one line every 0.25 s each, columns: epoch_ms <TAB> ok(1/0) [<TAB> ms]):

  <prefix>.db.tsv    can Postgres itself answer?  (docker exec pg_isready, straight into the container, NOT through
                     the proxy, so a network pause does not show here: Postgres is fine, only the path to it is cut)
  <prefix>.api.tsv   can the API answer GET /readyz (which runs SELECT 1 through the pool) within 1 s?

run.sh gives the file prefix and the fault times in the environment (B8_PROBE_PREFIX, B8_FAULT_START, B8_FAULT_END,
epoch seconds). All times below are seconds after B8_FAULT_START.

  db_down_s     first failed DB probe after the fault started          (n/a if the DB never failed: pauses, control)
  db_up_s       first good DB probe after that                         (for pauses: when the path was restored)
  api_down_s    first failed API probe after the fault started
  api_up_s      first good API probe after api_down_s
  api_stable_s  start of the last unbroken stretch of good API probes that lasts until the end of the run
  lag_s         api_up_s - db_up_s: how long the API needed after the database (or path) was back. Negative = the
                API probe happened to succeed before the first DB probe.
  api_end       ok | DOWN: was the API still failing in the last 10 s of the run?
"""

import os


def _read(path):
    rows = []
    try:
        with open(path) as f:
            for line in f:
                p = line.rstrip("\n").split("\t")
                if len(p) >= 2:
                    try:
                        rows.append((int(p[0]) / 1000.0, p[1] == "1"))
                    except ValueError:
                        pass
    except OSError:
        pass
    return rows


def _first(rows, after, want):
    for t, ok in rows:
        if t >= after and ok == want:
            return t
    return None


def _fmt(t, f0):
    return "n/a" if t is None else f"{t - f0:.1f}"


def extra_columns(m, args):
    prefix = os.environ.get("B8_PROBE_PREFIX")
    f0 = float(os.environ.get("B8_FAULT_START", "0") or 0)
    f1 = float(os.environ.get("B8_FAULT_END", "0") or 0)
    db = _read(prefix + ".db.tsv") if prefix else []
    api = _read(prefix + ".api.tsv") if prefix else []
    if not api or not f0:
        return [(k, "n/a") for k in ("db_down_s", "db_up_s", "api_down_s", "api_up_s", "api_stable_s", "lag_s", "api_end")]

    db_down = _first(db, f0, False)
    db_up = _first(db, db_down, True) if db_down is not None else None
    if db_up is None and f1 and db_down is None:
        db_up = f1  # a pause: the database never failed, the path came back at the end of the fault
    api_down = _first(api, f0, False)
    api_up = _first(api, api_down, True) if api_down is not None else None

    stable = None
    if api_down is not None:
        # stable = the start of the last unbroken run of good probes that reaches the end of the run
        i = len(api)
        while i > 0 and api[i - 1][1] and api[i - 1][0] >= api_down:
            i -= 1
        if i < len(api):
            stable = api[i][0]
    lag = "n/a" if api_up is None or db_up is None else f"{api_up - db_up:.1f}"

    tail = [ok for t, ok in api if t >= api[-1][0] - 10]
    api_end = "ok" if tail and all(tail) else "DOWN"
    return [
        ("db_down_s", _fmt(db_down, f0)),
        ("db_up_s", _fmt(db_up, f0)),
        ("api_down_s", _fmt(api_down, f0)),
        ("api_up_s", _fmt(api_up, f0)),
        ("api_stable_s", _fmt(stable, f0)),
        ("lag_s", lag),
        ("api_end", api_end),
    ]
