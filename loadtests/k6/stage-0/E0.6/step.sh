#!/usr/bin/env bash
# E0.6 step test: runs script.js for every rate, repeat and keep-alive mode.
#
# Usage:  ./step.sh                       (full matrix)
#         DRY_RUN=1 ./step.sh             (print the plan, run nothing)
#         RATES="500 1000" REPEATS=1 ./step.sh   (smaller trial)
#
# How it talks to runner.sh (one folder up, loadtests/k6/stage-0/runner.sh):
#   RESULTS_NAME  set here, so runner.sh does not prompt
#   LABEL         set to the run name; runner builds the k6 testid from it:
#                 <LABEL>_<RESULTS_NAME>-<timestamp>-lap-1
#   BASE_URL      runner's preflight target; BASE_URL is what script.js uses.
#                 Both are set from the same value, so they cannot disagree.
#   ROUTE         used by the preflight and forwarded to script.js
#   RATE, KEEPALIVE, DURATION   read by script.js through __ENV
set -euo pipefail

# ---- Settings (override with env vars) --------------------------------------
RATES="${RATES:-500 1000 2000 4000 8000}"   # rps steps
REPEATS="${REPEATS:-3}"                     # runs per rate and mode
DURATION="${DURATION:-60s}"                 # length of each run
MODES="${MODES:-1 0}"                       # 1 = keep-alive on, 0 = off
TW_MAX="${TW_MAX:-200}"                     # start a run only when TIME_WAIT <= this
DRY_RUN="${DRY_RUN:-0}"
BASE_URL="${BASE_URL:-http://localhost:8000}"   # server under test
ROUTE="${ROUTE:-/health}"
RESULTS_NAME="${RESULTS_NAME:-e06}"            # folder under results/ in stage-0/

# runner.sh lives one folder up (loadtests/k6/stage-0/), so we cd there to call it.
HERE="$(cd "$(dirname "$0")" && pwd)"
STAGE_DIR="$(dirname "$HERE")"

[ -x "$STAGE_DIR/runner.sh" ] || { echo "ERROR: $STAGE_DIR/runner.sh not found or not executable" >&2; exit 1; }

# ---- Record the sysctls that decide port exhaustion ---------------
echo "== sysctls =="
sysctl net.ipv4.ip_local_port_range net.ipv4.tcp_tw_reuse \
       net.ipv4.tcp_max_tw_buckets net.core.somaxconn || true
echo "== target: ${BASE_URL}${ROUTE}   results: results/${RESULTS_NAME}/ =="

# ---- Wait until the previous run's TIME_WAIT has drained ----------
# TIME_WAIT lasts ~60 s on Linux. Without this wait, run N pollutes run N+1.
wait_for_drain() {
  local n waited=0
  while :; do
    n=$(ss -Htan state time-wait | wc -l)
    [ "$n" -le "$TW_MAX" ] && return 0
    [ "$waited" -ge 180 ] && { echo "WARN: still $n TIME_WAIT after 180s, continuing"; return 0; }
    echo "  waiting for TIME_WAIT to drain ($n sockets)..."
    sleep 5
    waited=$((waited + 5))
  done
}

# ---- The matrix: rate (outer) > repeat > mode (inner) --------------
# Modes alternate on/off/on/off inside each repeat, so slow drift on the machine
# does not favour one mode. Rate is the outer loop so you can Ctrl+C at the rate
# where keep-alive OFF starts failing (dropped_iterations > 0) and skip higher ones.
total=$(( $(wc -w <<<"$RATES") * REPEATS * $(wc -w <<<"$MODES") ))
i=0

for rate in $RATES; do
  for rep in $(seq 1 "$REPEATS"); do
    for ka in $MODES; do
      i=$((i + 1))
      [ "$ka" = "1" ] && name=on || name=off
      testid="e06-r${rate}-ka-${name}-n${rep}"
      echo
      echo "== [$i/$total] rate=$rate keepalive=$name repeat=$rep testid=$testid =="

      if [ "$DRY_RUN" = "1" ]; then continue; fi

      wait_for_drain 

      # A failed run (including a dropped_iterations threshold failure) must not
      # stop the matrix. runner.sh records it as FAIL in results/<name>/<ts>/manifest.md.
      ( cd "$STAGE_DIR" && \
        RESULTS_NAME="$RESULTS_NAME" LABEL="$testid" \
        SCRIPT="E0.6/script.js" RATE="$rate" KEEPALIVE="$ka" DURATION="$DURATION" \
        BASE_URL="$BASE_URL" ROUTE="$ROUTE" \
        ./runner.sh ) \
        || echo "!! run $testid exited non-zero (see its manifest.md; likely dropped_iterations > 0, discard it)"
    done
  done
done

echo
echo "Done. Next: read results from the dashboard."