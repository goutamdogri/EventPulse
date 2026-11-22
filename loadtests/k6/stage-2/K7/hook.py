"""K7 hook for the shared analyze.py (--hook): p99 of the request time in each window of the run.
The k6 script sends lat_healthy / lat_slow / lat_after trends; the plain summary only has one p99 for the whole run."""


def extra_columns(metrics, args):
    cols = []
    for w in ("healthy", "slow", "after"):
        t = metrics.get(f"lat_{w}") or {}
        v = t.get("p(99)", (t.get("values") or {}).get("p(99)"))
        cols.append((f"p99_{w}_ms", "n/a" if v is None else f"{v:.1f}"))
    return cols
