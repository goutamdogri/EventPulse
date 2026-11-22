"""B11 hook for loadtests/k6/analyze.py (passed with --hook).

Adds the "how did the run change over time" columns to its summary row. k6's summary json only has numbers for the
whole run, so the time dimension comes from the sampler file that run.sh writes during the run:

  captures/<testid>.samples.tsv   header line, then one line per sample (about one a minute, plus one right after the run).
                                  Counters are cumulative since Postgres / the server started, so a column of the summary
                                  is always a DIFFERENCE between two samples, divided by the seconds between them.

run.sh gives the file path in the environment (B11_SAMPLES). "first" is the first W minutes of the run and "last" is the
last W minutes, with W = 5 samples, or fewer if the run is short (a third of the samples at most), so a 3 minute control
run still gives numbers. All first/last pairs are what I compare: a soak that did not decay has first about equal to last.

  rps_first / rps_last     events the server inserted per second (server counter ep_events_inserted_total)
  rps_min_min              the slowest single sample interval (about one minute) of the run
  q_ms_first / q_ms_last   mean INSERT time on the pool connection, ms (ep_pg_query_duration_seconds sum / count)
  hit_first / hit_last     buffer cache hit ratio in %, hit / (hit + read) of the database counters (pg_stat_database)
  walB_row_first / _last   WAL bytes written per inserted row (pg_stat_wal wal_bytes / rows)
  fpi_row_first / _last    full-page images per inserted row (pg_stat_wal wal_fpi / rows)
  ckpt_timed / ckpt_req    checkpoints during the run, started by the timer / because max_wal_size was close
  ckpt_per_h               all checkpoints scaled to one hour
  table_mb / index_mb      size of table events and its indexes at the end of the run
  free_gb_end              free disk space at the end of the run
"""

import os

COLS = ("epoch n_live table_bytes index_bytes ckpt_timed ckpt_req buf_ckpt buf_backend buf_clean ckpt_write_ms "
        "ckpt_sync_ms db_hit db_read wal_bytes wal_fpi autovac inserted qdur_sum_s qdur_count free_gb").split()
NAMES = ("rps_first rps_last rps_min_min q_ms_first q_ms_last hit_first hit_last walB_row_first walB_row_last "
         "fpi_row_first fpi_row_last ckpt_timed ckpt_req ckpt_per_h table_mb index_mb free_gb_end").split()


def _num(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _read(path):
    rows = []
    try:
        with open(path) as f:
            next(f, None)  # header
            for line in f:
                p = line.rstrip("\n").split("\t")
                if len(p) == len(COLS):
                    rows.append({c: _num(v) for c, v in zip(COLS, p)})
    except OSError:
        pass
    return [r for r in rows if r["epoch"] is not None]


def _d(a, b, col):
    """difference of a cumulative column between two samples (None if either is missing)"""
    x, y = a.get(col), b.get(col)
    return None if x is None or y is None else y - x


def _window(a, b):
    """rates and ratios between sample a and sample b"""
    dt = b["epoch"] - a["epoch"]
    ins = _d(a, b, "inserted")
    qs, qc = _d(a, b, "qdur_sum_s"), _d(a, b, "qdur_count")
    hit, rd = _d(a, b, "db_hit"), _d(a, b, "db_read")
    wal, fpi = _d(a, b, "wal_bytes"), _d(a, b, "wal_fpi")
    out = {}
    out["rps"] = ins / dt if ins is not None and dt > 0 else None
    out["q_ms"] = qs / qc * 1000 if qs is not None and qc else None
    out["hit"] = hit / (hit + rd) * 100 if hit is not None and rd is not None and hit + rd > 0 else None
    out["walB_row"] = wal / ins if wal is not None and ins else None
    out["fpi_row"] = fpi / ins if fpi is not None and ins else None
    return out


def _fmt(x, nd=1):
    return "n/a" if x is None else f"{x:.{nd}f}"


def extra_columns(m, args):
    s = _read(os.environ.get("B11_SAMPLES", "")) if os.environ.get("B11_SAMPLES") else []
    if len(s) < 4:
        return [(k, "n/a") for k in NAMES]

    # The LAST sample is taken a few seconds after k6 ended (run.sh), so an interval that ends there contains idle
    # time. It is used only for sizes and counters; the rate windows use the samples up to the one before it.
    final = s[-1]
    s = s[:-1]
    n = len(s) - 1                      # number of intervals
    w = max(1, min(5, n // 3))          # window in samples
    first = _window(s[0], s[w])
    last = _window(s[-1 - w], s[-1])

    mins = [r for r in (_window(s[i], s[i + 1])["rps"] for i in range(n)) if r is not None]
    dt_total = s[-1]["epoch"] - s[0]["epoch"]
    ct, cr = _d(s[0], s[-1], "ckpt_timed"), _d(s[0], s[-1], "ckpt_req")
    per_h = (ct + cr) / dt_total * 3600 if ct is not None and cr is not None and dt_total > 0 else None
    tb, ib = final.get("table_bytes"), final.get("index_bytes")

    return [
        ("rps_first", _fmt(first["rps"], 0)),
        ("rps_last", _fmt(last["rps"], 0)),
        ("rps_min_min", _fmt(min(mins) if mins else None, 0)),
        ("q_ms_first", _fmt(first["q_ms"], 2)),
        ("q_ms_last", _fmt(last["q_ms"], 2)),
        ("hit_first", _fmt(first["hit"], 2)),
        ("hit_last", _fmt(last["hit"], 2)),
        ("walB_row_first", _fmt(first["walB_row"], 0)),
        ("walB_row_last", _fmt(last["walB_row"], 0)),
        ("fpi_row_first", _fmt(first["fpi_row"], 3)),
        ("fpi_row_last", _fmt(last["fpi_row"], 3)),
        ("ckpt_timed", _fmt(ct, 0)),
        ("ckpt_req", _fmt(cr, 0)),
        ("ckpt_per_h", _fmt(per_h, 1)),
        ("table_mb", _fmt(tb / 1048576 if tb is not None else None, 0)),
        ("index_mb", _fmt(ib / 1048576 if ib is not None else None, 0)),
        ("free_gb_end", _fmt(final.get("free_gb"), 0)),
    ]
