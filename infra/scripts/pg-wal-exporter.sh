#!/usr/bin/env bash
# pg-wal-exporter.sh - how much WAL Postgres writes, for Prometheus (B5)
#
# Why: the postgres-exporter container has no WAL volume metric. Its xlog_location collector only works on
# Postgres < 10 (I tried it on 15: it logs "not available" and exports nothing), and pg_wal_size_bytes is only the size
# of the pg_wal folder, not how much was written. Postgres itself counts it in the view pg_stat_wal
# (since version 14), so I read that view once a second and write it in node_exporter's textfile format,
# the same way pg-container-exporter.sh does.
#
# Metrics (all counters since the last stats reset; use rate() or increase() in Grafana):
#   ep_pg_wal_bytes_total          bytes of WAL generated
#   ep_pg_wal_records_total        WAL records generated
#   ep_pg_wal_fpi_total            full-page images (whole 8 KB pages written to WAL, the first time a page is changed after a checkpoint)
#   ep_pg_wal_buffers_full_total   times a backend had to write WAL itself because the WAL buffers were full
#
# Usage:  PG_CONTAINER=ep-postgres ./infra/scripts/pg-wal-exporter.sh &
# Env:    PG_CONTAINER   container name (required)
#         PGUSER, PGDB   default eventpulse / eventpulse
#         TEXTFILE_DIR   default <repo>/infra/compose/textfile
set -u
PG_CONTAINER="${PG_CONTAINER:?set PG_CONTAINER to the postgres container name}"
PGUSER="${PGUSER:-eventpulse}"; PGDB="${PGDB:-eventpulse}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEXTFILE_DIR="${TEXTFILE_DIR:-$HERE/../compose/textfile}"
OUT="$TEXTFILE_DIR/ep_pg_wal.prom"

mkdir -p "$TEXTFILE_DIR"
# Stop every child (docker exec, the read loop) when this script is told to stop. The pipeline below runs in the
# BACKGROUND and I "wait" for it: bash only runs a trap after the foreground command ends, and a pipeline that never
# ends would never let the trap run (my first version left docker exec and the loop running after the script was killed).
kill_children() { local c; for c in $(pgrep -P "$1"); do kill_children "$c"; kill "$c" 2>/dev/null; done; }
trap 'trap - EXIT; kill_children $$; rm -f "$OUT" "$OUT.tmp"; exit 0' INT TERM EXIT
echo "pg-wal-exporter: $PG_CONTAINER -> $OUT every 1 s" >&2

# ONE psql session for the whole time, so I do not pay for a new docker exec + new connection each second (that would add
# its own noise to the experiment). A loop feeds one query per second into psql's standard input.
# I do not use psql's "\watch 1" for this: when the docker exec client is killed, the psql inside the container kept
# running its \watch for ever (an orphan session). With a feeder loop, killing this script closes psql's stdin and psql exits.
# -A -t -F' ' = no alignment, no header/footer, space between the columns, so `read` can split the line.
# wal_bytes is numeric in pg_stat_wal; ::bigint is fine for many years of WAL.
{ while :; do echo "select wal_records, wal_fpi, wal_bytes::bigint, wal_buffers_full from pg_stat_wal;"; sleep 1; done; } \
  | docker exec -i "$PG_CONTAINER" psql -U "$PGUSER" -d "$PGDB" -X -A -t -F' ' 2>/dev/null \
  | while read -r rec fpi bytes full; do
      # only lines that start with a digit and have 4 fields are data (skips any error text)
      [[ "$rec" =~ ^[0-9]+$ && -n "${full:-}" ]] || continue
      {
        echo '# TYPE ep_pg_wal_bytes_total counter'
        echo "ep_pg_wal_bytes_total $bytes"
        echo '# TYPE ep_pg_wal_records_total counter'
        echo "ep_pg_wal_records_total $rec"
        echo '# TYPE ep_pg_wal_fpi_total counter'
        echo "ep_pg_wal_fpi_total $fpi"
        echo '# TYPE ep_pg_wal_buffers_full_total counter'
        echo "ep_pg_wal_buffers_full_total $full"
      } > "$OUT.tmp" && mv "$OUT.tmp" "$OUT"     # write then rename, so node_exporter never reads half a file
    done &
wait
