#!/usr/bin/env bash
# Writes the accept-queue length of the listening socket as Prometheus metrics for node_exporter's
# textfile collector.   Output file: <textfile dir>/ep_listen.prom
#
# Usage:   scripts/listen-queue-exporter.sh [TEXTFILE_DIR]        (stop with Ctrl+C)
#   or:    TEXTFILE_DIR=infra/compose/textfile PORT=8000 scripts/listen-queue-exporter.sh
# The benchmark runner starts and stops this for you.
set -euo pipefail

PORT="${PORT:-8000}"
INTERVAL="${INTERVAL:-0.25}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INFRA_DIR="${INFRA_DIR:-$(cd "$SCRIPT_DIR/.." && pwd)}"
DIR="${1:-${TEXTFILE_DIR:-$INFRA_DIR/compose/textfile}}"
OUT="$DIR/ep_listen.prom"
TMP="$OUT.tmp"

[[ -d "$DIR" ]] || { echo "textfile directory not found: $DIR" >&2; exit 1; }
command -v ss >/dev/null || { echo "ss not found (install iproute2)" >&2; exit 1; }

# On exit remove the file so Grafana shows a gap instead of a stale (frozen) value.
trap 'rm -f "$OUT" "$TMP"' EXIT
trap 'exit 0' INT TERM

echo "listen-queue-exporter: port=$PORT interval=${INTERVAL}s -> $OUT" >&2
while true; do
  # Recv-Q summed, Send-Q max across the listener sockets (IPv4 + IPv6 may both appear).
  read -r recvq sendq < <(ss -H -ltn "sport = :${PORT}" | awk '{r+=$2; if ($3>s) s=$3} END{ if (NR>0) print r, s }') || true
  if [[ -n "${recvq:-}" ]]; then
    {
      echo "# HELP ep_listen_recvq Connections waiting in the accept queue."
      echo "# TYPE ep_listen_recvq gauge"
      echo "ep_listen_recvq{port=\"$PORT\"} $recvq"
      echo "# HELP ep_listen_sendq Max accept queue length (backlog)."
      echo "# TYPE ep_listen_sendq gauge"
      echo "ep_listen_sendq{port=\"$PORT\"} $sendq"
    } > "$TMP"
    mv "$TMP" "$OUT"          # atomic: node_exporter never reads a half-written file
  fi
  recvq=""; sendq=""
  sleep "$INTERVAL"
done
