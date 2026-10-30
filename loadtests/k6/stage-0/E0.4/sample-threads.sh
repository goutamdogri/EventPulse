#!/usr/bin/env bash
# Per-thread CPU and memory of the server, 1 sample/second. Run in a second terminal during a test.
# Shows main thread vs worker threads (isolation proof) and RSS growth (Experiment C).
# usage: ./sample-threads.sh <label> [seconds]      (needs sysstat's pidstat)
set -euo pipefail

LABEL="${1:?usage: sample-threads.sh <label> [seconds]}"
SECS="${2:-120}"
PID="${PID:-$(pgrep -f 'stage-0-foundations/src/server.js' | head -1 || true)}"
[[ -n "$PID" ]] || { echo "server pid not found; run with PID=<pid>" >&2; exit 1; }

OUT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/results"
OUT="$OUT_DIR/${LABEL}-threads.txt"
mkdir -p "$OUT_DIR"

echo "PID=$PID  samples=${SECS}s  -> $OUT"
pidstat -t -u -r -p "$PID" 1 "$SECS" | tee "$OUT"
