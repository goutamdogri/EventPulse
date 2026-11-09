#!/usr/bin/env bash
# E0.6 step test: runs script.js for every rate, repeat and keep-alive mode,
# and captures TIME_WAIT / nstat / strace snapshots in the middle of each run.
#
# Usage:  SERVER_PID=12345 ./step.sh              (full matrix)
#         DRY_RUN=1 ./step.sh                     (print the plan, run nothing)
#         RATES="500 1000" REPEATS=1 ./step.sh    (smaller trial)
#         PORT=3000 SERVER_PID=$(pgrep -f server.js | head -1) ./step.sh
#
# Env vars for captures:
#   PORT         server port. Default: taken from BASE_URL.
#   SERVER_PID   pid of the server under test. Default: auto-detected from the
#                listening socket on PORT (needs permission to see the pid).
#                If neither works, the strace capture is skipped with a warning.
#   STRACE_SECS  how long strace attaches (default 10)
#   NO_STRACE=1  skip strace (it slows the server a lot while attached)
#
# How it talks to runner.sh (one folder up, loadtests/k6/stage-0/runner.sh):
#   RESULTS_NAME  set here, so runner.sh does not prompt
#   LABEL         the run name; runner builds the k6 testid from it
#   BASE_URL      runner's preflight target and what script.js uses
#   ROUTE         used by the preflight and forwarded to script.js
#   RATE, KEEPALIVE, DURATION   read by script.js through __ENV
set -euo pipefail

# ---- Settings (override with env vars) --------------------------------------
RATES="${RATES:-500 1000 2000 4000 8000}"
REPEATS="${REPEATS:-3}"
DURATION="${DURATION:-60s}"
MODES="${MODES:-1 0}"                       # 1 = keep-alive on, 0 = off
TW_MAX="${TW_MAX:-200}"
DRY_RUN="${DRY_RUN:-0}"
BASE_URL="${BASE_URL:-http://localhost:8000}"
ROUTE="${ROUTE:-/health}"
RESULTS_NAME="${RESULTS_NAME:-e06}"
STRACE_SECS="${STRACE_SECS:-10}"
NO_STRACE="${NO_STRACE:-0}"
K6_WAIT_MAX="${K6_WAIT_MAX:-60}"            # max seconds to wait for k6 to appear

# Port: explicit PORT, else parsed from BASE_URL (http://host:PORT/...)
PORT="${PORT:-$(sed -E 's#^[a-z]+://[^:/]+:([0-9]+).*#\1#' <<<"$BASE_URL")}"
[[ "$PORT" =~ ^[0-9]+$ ]] || { echo "ERROR: cannot determine PORT; set PORT=..." >&2; exit 1; }

# Server pid: explicit SERVER_PID, else look it up from the listening socket.
if [ -z "${SERVER_PID:-}" ]; then
  SERVER_PID=$(ss -Htlnp "( sport = :${PORT} )" 2>/dev/null | grep -o 'pid=[0-9]*' | head -1 | cut -d= -f2 || true)
fi

HERE="$(cd "$(dirname "$0")" && pwd)"
STAGE_DIR="$(dirname "$HERE")"
CAPTURE_DIR="$STAGE_DIR/results/${RESULTS_NAME}/captures"

[ -x "$STAGE_DIR/runner.sh" ] || { echo "ERROR: $STAGE_DIR/runner.sh not found or not executable" >&2; exit 1; }

# ---- Helpers ----------------------------------------------------------------
to_seconds() {            # 60s -> 60, 2m -> 120, 1h -> 3600, 90 -> 90
  local v="$1"
  case "$v" in
    *h) echo $(( ${v%h} * 3600 )) ;;
    *m) echo $(( ${v%m} * 60 )) ;;
    *s) echo "${v%s}" ;;
    *)  echo "$v" ;;
  esac
}
DUR_S=$(to_seconds "$DURATION")
HALF_S=$(( DUR_S / 2 ))

STRACE_ON=1
if [ "$NO_STRACE" = "1" ]; then
  STRACE_ON=0
elif [ -z "${SERVER_PID:-}" ]; then
  echo "WARN: SERVER_PID not set and could not be auto-detected on port $PORT; strace will be skipped." >&2
  STRACE_ON=0
fi

echo "== sysctls =="
sysctl net.ipv4.ip_local_port_range net.ipv4.tcp_tw_reuse \
       net.ipv4.tcp_max_tw_buckets net.core.somaxconn || true
echo "== target: ${BASE_URL}${ROUTE}   results: results/${RESULTS_NAME}/ =="
echo "== port=$PORT pid=${SERVER_PID:-none} strace=$STRACE_ON captures: $CAPTURE_DIR =="

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

# Wait until k6 is actually generating load, so "middle of the run" is measured
# from the start of the load, not from the start of runner.sh's preflight.
wait_for_k6() {
  local waited=0
  while ! pgrep -x k6 >/dev/null 2>&1; do
    [ "$waited" -ge "$K6_WAIT_MAX" ] && return 1
    sleep 1; waited=$((waited + 1))
  done
  return 0
}

# ---- Tagged capture ---------------------------------------------------------
# Every block starts with a header line carrying rate, rep, ka, testid, phase,
# timestamp and the exact command, so you can grep a file or concatenate all runs:
#   grep -h '^=== ' results/<name>/captures/*.txt
CAP_FILE=""
tag() {  # tag <phase> <command string>
  printf '\n=== rate=%s rep=%s ka=%s testid=%s phase=%s ts=%s cmd: %s\n' \
    "$rate" "$rep" "$name" "$testid" "$1" "$(date -Is)" "$2" >>"$CAP_FILE"
}

capture_mid() {
  local c1="ss -Htan state time-wait '( sport = :${PORT} )' | wc -l   # server side"
  local c2="ss -Htan state time-wait '( dport = :${PORT} )' | wc -l   # client side"
  local c3="nstat -az TcpActiveOpens TcpPassiveOpens TcpRetransSegs TcpExtListenOverflows TcpExtListenDrops"
  local c4="sudo -n timeout ${STRACE_SECS} strace -c -f -p ${SERVER_PID:-?}"

  tag mid "$c1"; ss -Htan state time-wait "( sport = :${PORT} )" 2>&1 | wc -l >>"$CAP_FILE"
  tag mid "$c2"; ss -Htan state time-wait "( dport = :${PORT} )" 2>&1 | wc -l >>"$CAP_FILE"
  tag mid "$c3"; nstat -az TcpActiveOpens TcpPassiveOpens TcpRetransSegs TcpExtListenOverflows TcpExtListenDrops >>"$CAP_FILE" 2>&1 || true
  if [ "$STRACE_ON" = "1" ]; then
    tag mid "$c4"
    # strace prints its -c summary on stderr when timeout sends SIGTERM
    sudo -n timeout "$STRACE_SECS" strace -c -f -p "$SERVER_PID" >>"$CAP_FILE" 2>&1 || true
  else
    tag mid "$c4 (SKIPPED)"
  fi
}

# nstat counters are cumulative, so snapshot before and after to compute deltas.
nstat_snapshot() {  # nstat_snapshot <phase>
  tag "$1" "nstat -az TcpActiveOpens TcpPassiveOpens TcpRetransSegs TcpExtListenOverflows TcpExtListenDrops"
  nstat -az TcpActiveOpens TcpPassiveOpens TcpRetransSegs TcpExtListenOverflows TcpExtListenDrops >>"$CAP_FILE" 2>&1 || true
}

# ---- The matrix: rate (outer) > repeat > mode (inner) -----------------------
total=$(( $(wc -w <<<"$RATES") * REPEATS * $(wc -w <<<"$MODES") ))
i=0
[ "$DRY_RUN" = "1" ] || mkdir -p "$CAPTURE_DIR"

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
      # Refresh the sudo timestamp in the foreground so the background capture
      # never blocks on a password prompt (sudo -n fails instead of prompting).
      [ "$STRACE_ON" = "1" ] && sudo -v

      CAP_FILE="$CAPTURE_DIR/${testid}.txt"
      : >"$CAP_FILE"
      nstat_snapshot pre

      # Launch the run in the background. A failed run must not stop the matrix.
      ( cd "$STAGE_DIR" && \
        RESULTS_NAME="$RESULTS_NAME" LABEL="$testid" \
        SCRIPT="E0.6/script.js" RATE="$rate" KEEPALIVE="$ka" DURATION="$DURATION" \
        BASE_URL="$BASE_URL" ROUTE="$ROUTE" \
        ./runner.sh ) &
      run_pid=$!

      # Middle of the load: wait for k6 to start, then sleep half the duration.
      if ! wait_for_k6; then
        echo "WARN: k6 process not seen within ${K6_WAIT_MAX}s; timing mid-capture from now" >&2
      fi
      sleep "$HALF_S"
      echo "  capturing mid-run snapshots -> $CAP_FILE"
      capture_mid

      wait "$run_pid" \
        || echo "!! run $testid exited non-zero (see its manifest.md; likely dropped_iterations > 0, discard it)"

      nstat_snapshot post
    done
  done
done

echo
echo "Done. Next: read results from the dashboard."
echo "Captures: $CAPTURE_DIR/<testid>.txt   (grep -h '^=== ' $CAPTURE_DIR/*.txt)"