#!/usr/bin/env bash
# E0.9: Payload size scaling on POST /echo
# ONE command runs the whole experiment:  ./step.sh
#
# What this script does (so I can re-read it later):
#   1. preflight   - check tools, Prometheus, node_exporter, server, and that /echo really echoes a 100 KB body
#   2. env snapshot- save versions/sysctls/CPU pinning next to the results
#   3. matrix      - for every step x repeat x payload size: settle, k6 run (via runner.sh), light snapshots
#                    Each payload size has its OWN rate ladder (a 100 KB request costs far more than a 100 B one,
#                    so the same ladder would be useless for one of them). Sizes alternate inside each step
#                    and repeat, so slow drift (heat, caches) hits all sizes equally.
#   4. probes      - SEPARATE short runs where heavy tools (perf, strace, ss) may disturb the server.
#                    They run against a second server instance started with --perf-basic-prof.
#                    They are never mixed into the measured runs (E0.6 lesson: strace inside the run polluted the numbers).
#
# Usage:  ./step.sh                              (full matrix)
#         DRY_RUN=1 ./step.sh                    (print the plan, run nothing)
#         SIZES="100 100000" RATES_100="4000" RATES_100000="500" REPEATS=1 PROBES=0 ./step.sh   (small trial)
#         SHAPE=str RESULTS_NAME=e09-str ./step.sh   (second pass: one big string instead of many fields)
#         MANAGE_SERVER=0 ./step.sh              (attach to a server I started myself)
#
# EDIT markers below = the only places that change from experiment to experiment.
set -euo pipefail

# ---- 1. Settings (everything can be overridden from the environment) --------
EXP_ID="E0.9"                                          # EDIT
SHAPE="${SHAPE:-obj}"                                  # obj | str (see script.js)
if [ "$SHAPE" = "obj" ]; then DEFAULT_RESULTS="e09"; else DEFAULT_RESULTS="e09-$SHAPE"; fi
RESULTS_NAME="${RESULTS_NAME:-$DEFAULT_RESULTS}"       # EDIT  results go to stage-0/results/<RESULTS_NAME>/
SIZES="${SIZES:-100 1000 10000 100000}"                # EDIT  the variable: request body bytes (decimal: 1 KB = 1000 B here)
# Rate ladders (offered requests/s, open model), one per size, 5 steps each. These are MY PREDICTION-BASED GUESSES:
# they are built around the predicted knee of each size (see README). Same number of steps for every size.
RATES_100="${RATES_100:-4000 8000 12000 16000 20000}"      # EDIT
RATES_1000="${RATES_1000:-4000 8000 11000 14000 18000}"    # EDIT
RATES_10000="${RATES_10000:-1500 3000 4500 6000 8000}"     # EDIT
RATES_100000="${RATES_100000:-250 500 750 1000 1300}"      # EDIT
# Probe rate per size: about half of the predicted knee, so the server is busy but not saturated.
PROBE_RATE_100="${PROBE_RATE_100:-8000}"               # EDIT
PROBE_RATE_1000="${PROBE_RATE_1000:-7000}"             # EDIT
PROBE_RATE_10000="${PROBE_RATE_10000:-3000}"           # EDIT
PROBE_RATE_100000="${PROBE_RATE_100000:-500}"          # EDIT
REPEATS="${REPEATS:-3}"                                # >=3 so I can report variance
DURATION="${DURATION:-60s}"
WARMUP="${WARMUP:-10s}"                                # low-rate warm-up inside every k6 run (tagged stage=warmup)
PROBES="${PROBES:-1}"                                  # 1 = also run the separate probe runs
PROBE_DURATION="${PROBE_DURATION:-40s}"
PROBE_SECS="${PROBE_SECS:-10}"                         # how long perf and strace each observe
PERF_PROBE="${PERF_PROBE:-1}"                          # 1 = use perf in probes (needs perf + sudo), else only strace
BASE_URL="${BASE_URL:-http://localhost:8000}"
ECHO_ROUTE="${ECHO_ROUTE:-/echo}"
ROUTE="${ROUTE:-/health}"                              # only for runner.sh's GET preflight (/echo is POST)
PROM_URL="${PROM_URL:-http://localhost:9090}"
MANAGE_SERVER="${MANAGE_SERVER:-1}"                    # 1 = start/stop server-runner.sh myself
SERVER_CPUS="${SERVER_CPUS:-0,6}"                      # same default as server-runner.sh
SETTLE_MAX="${SETTLE_MAX:-200}"                        # EDIT  max TIME_WAIT sockets before a run may start
SETTLE_MIN_SECS="${SETTLE_MIN_SECS:-8}"                # EDIT  minimum pause: lets GC / heap shrink after a big-body run
DRY_RUN="${DRY_RUN:-0}"

PORT="${PORT:-$(sed -E 's#^[a-z]+://[^:/]+:([0-9]+).*#\1#' <<<"$BASE_URL")}"

# ---- 2. Paths ----------------------------------------------------------------
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"       # loadtests/k6/stage-0/E0.9
STAGE_DIR="$(dirname "$HERE")"                              # runner.sh lives here
REPO="$(cd "$STAGE_DIR/../../.." && pwd)"
SERVER_RUNNER="$REPO/labs/stage-0-foundations/script/server-runner.sh"
STATE_FILE="${E05_STATE_DIR:-/tmp/eventpulse-e0.5}/server.env"   # server-runner.sh writes the pid here
OUT="$STAGE_DIR/results/$RESULTS_NAME"
CAP="$OUT/captures"
RUNS_TSV="$OUT/runs.tsv"                                    # testid, phase, start/end epoch -> zoom Grafana to it

die() { echo "ERROR: $*" >&2; exit 1; }
to_seconds() { case "$1" in *h) echo $(( ${1%h}*3600 ));; *m) echo $(( ${1%m}*60 ));; *s) echo "${1%s}";; *) echo "$1";; esac; }

# rates_for <size>  -> prints that size's ladder;  probe_rate_for <size> -> prints its probe rate
rates_for()      { local v="RATES_$1";      [ -n "${!v:-}" ] || die "no rate ladder for size $1 (set RATES_$1)"; echo "${!v}"; }
probe_rate_for() { local v="PROBE_RATE_$1"; [ -n "${!v:-}" ] || die "no probe rate for size $1 (set PROBE_RATE_$1)"; echo "${!v}"; }

# All sizes must have the same number of steps, because the loop walks step 1..N for every size together.
N_STEPS=""
for s in $SIZES; do
  n=$(wc -w <<<"$(rates_for "$s")")
  [ -z "$N_STEPS" ] && N_STEPS="$n"
  [ "$n" = "$N_STEPS" ] || die "size $s has $n rates, expected $N_STEPS (every size needs the same number of steps)"
done

# ---- 3. Preflight -------------------------------------------------------------
# The server must really accept and echo the biggest body, otherwise the 100 KB runs would just measure errors.
echo_check() {
  local biggest=0 s; for s in $SIZES; do [ "$s" -gt "$biggest" ] && biggest="$s"; done
  local f; f="$(mktemp)"
  # {"data":"xxxx..."} with exactly $biggest bytes: 9 bytes of head ({"data":") + 2 of tail ("})
  { printf '{"data":"'; head -c $((biggest-11)) /dev/zero | tr '\0' x; printf '"}'; } >"$f"
  local out code size
  out=$(curl -sS --max-time 10 -o /dev/null -w '%{http_code} %{size_download}' \
        -H 'Content-Type: application/json' --data-binary @"$f" "${BASE_URL}${ECHO_ROUTE}") || { rm -f "$f"; die "POST $ECHO_ROUTE failed (is the server up?)"; }
  rm -f "$f"
  code=${out% *}; size=${out#* }
  [ "${code:0:1}" = "2" ] || die "POST $ECHO_ROUTE with a $biggest B body returned HTTP $code. The server may have a body size limit: raise it before running E0.9."
  echo "  echo check: sent $biggest B, got HTTP $code and $size B back"
  [ "$size" -ge $((biggest*9/10)) ] || echo "WARN: response is only $size B for a $biggest B request; /echo may not return the whole body (check src/routes/echo.js). The run still works, but the 'bytes out' panels will differ."
}

preflight() {
  for c in k6 taskset curl ss nstat awk head tr; do command -v "$c" >/dev/null || die "'$c' not found in PATH"; done
  [ -x "$STAGE_DIR/runner.sh" ] || die "$STAGE_DIR/runner.sh missing or not executable"
  [ -f "$HERE/script.js" ] || die "$HERE/script.js missing"
  curl -fsS --max-time 3 "$PROM_URL/-/ready" >/dev/null \
    || die "Prometheus not ready at $PROM_URL (start the compose stack: docker compose -f infra/compose/docker-compose.yml up -d)"
  curl -fsS --max-time 3 "$PROM_URL/api/v1/query?query=up%7Bjob%3D%22node%22%7D" | grep -q '"value":\[[^]]*"1"\]' \
    || echo "WARN: node_exporter (job=node) is not 'up' in Prometheus; host panels will be empty" >&2
  [ "$MANAGE_SERVER" = "1" ] && { [ -f "$SERVER_RUNNER" ] || die "server-runner.sh not found at $SERVER_RUNNER"; }
  # EDIT: experiment-specific checks
  if [ "$PROBES" = "1" ]; then
    command -v strace >/dev/null || echo "WARN: strace not found; probes will skip it" >&2
    if [ "$PERF_PROBE" = "1" ] && ! command -v perf >/dev/null; then
      echo "WARN: perf not found; probes will use strace only (PERF_PROBE=0 to silence)" >&2; PERF_PROBE=0
    fi
    sudo -n true 2>/dev/null || echo "WARN: 'sudo -n' does not work. strace/perf need root, so run 'sudo -v' first or the probes will skip them" >&2
  fi
}

env_snapshot() {
  {
    echo "experiment: $EXP_ID   date: $(date -Is)"
    echo "git: $(git -C "$REPO" rev-parse --short HEAD 2>/dev/null || echo n/a) $(git -C "$REPO" status --porcelain 2>/dev/null | wc -l) dirty files"
    echo "kernel: $(uname -r)   node: $(node -v 2>/dev/null)   k6: $(k6 version 2>/dev/null | head -1)"
    echo "server pinned to: $SERVER_CPUS   k6 pinned to: ${K6_CPUS:-1,2,3,7,8,9 (runner.sh default)}"
    echo "settings: SIZES=[$SIZES] SHAPE=$SHAPE REPEATS=$REPEATS DURATION=$DURATION WARMUP=$WARMUP"
    for s in $SIZES; do echo "  ladder size $s: $(rates_for "$s")"; done
    echo "--- sysctls (socket buffers matter for big bodies) ---"
    sysctl net.core.rmem_max net.core.wmem_max net.ipv4.tcp_rmem net.ipv4.tcp_wmem net.ipv4.tcp_moderate_rcvbuf \
           net.ipv4.ip_local_port_range net.core.somaxconn 2>&1 || true
    echo "--- loopback MTU (a 100 KB body is split into segments of about this size) ---"
    ip link show lo 2>/dev/null | head -1 || true
    echo "--- ulimit -n: $(ulimit -n) ---"
    echo "--- cpu ---"; lscpu 2>/dev/null | grep -E 'Model name|^CPU\(s\)|Thread|Core|MHz' || true
  } > "$OUT/env.txt"
}

# ---- 4. Server lifecycle ------------------------------------------------------
SERVER_PID=""
start_server() {   # start_server [VAR=value ...]  (extra env only for this server start)
  [ "$MANAGE_SERVER" = "1" ] || { SERVER_PID=$(ss -Htlnp "( sport = :${PORT} )" | grep -o 'pid=[0-9]*' | head -1 | cut -d= -f2 || true); return 0; }
  stop_server
  env "$@" SERVER_CPUS="$SERVER_CPUS" PORT="$PORT" bash "$SERVER_RUNNER" >"$OUT/server.out" 2>&1 &     # bash <file>: still the node pid after its exec
  for _ in $(seq 1 30); do curl -fsS --max-time 1 "${BASE_URL}${ROUTE}" >/dev/null 2>&1 && break; sleep 1; done
  curl -fsS --max-time 2 "${BASE_URL}${ROUTE}" >/dev/null || die "server did not come up (see $OUT/server.out)"
  SERVER_PID=$(. "$STATE_FILE"; echo "$SERVER_PID")      # server-runner.sh execs node, so this is the node pid
}
stop_server() {
  [ "$MANAGE_SERVER" = "1" ] || return 0
  if [ -f "$STATE_FILE" ]; then
    local p; p=$(. "$STATE_FILE"; echo "${SERVER_PID:-}")
    [ -n "$p" ] && kill -0 "$p" 2>/dev/null && { kill "$p"; sleep 1; }
  fi
}
trap stop_server EXIT

# ---- 5. Helpers used by the matrix --------------------------------------------
inflight_now() {   # requests the server is handling right now (gauge http_requests_in_flight)
  curl -fsS --max-time 2 "${BASE_URL}/metrics" 2>/dev/null | awk '/^http_requests_in_flight/ {s+=$NF} END{print s+0}'
}

settle() {         # wait until the machine is "clean" again before the next run
  local n inf waited=0
  echo "  settle: waiting at least ${SETTLE_MIN_SECS}s (GC/heap), TIME_WAIT <= ${SETTLE_MAX}, in-flight requests <= 1"
  sleep "$SETTLE_MIN_SECS"
  while :; do
    n=$(ss -Htan state time-wait | wc -l); inf=$(inflight_now)
    [ "$n" -le "$SETTLE_MAX" ] && [ "${inf:-0}" -le 1 ] && return 0
    [ "$waited" -ge 180 ] && { echo "WARN: still $n TIME_WAIT / $inf in flight after 180s, continuing"; return 0; }
    echo "  waiting ($n TIME_WAIT, $inf in flight)..."; sleep 5; waited=$((waited+5))
  done
}

CAP_FILE=""
tag() { printf '\n=== exp=%s testid=%s phase=%s ts=%s cmd: %s\n' "$EXP_ID" "$testid" "$1" "$(date -Is)" "$2" >>"$CAP_FILE"; }

# Cheap counters read from /proc: safe to take inside a measured run (before and after). They are CUMULATIVE,
# so I subtract post - pre. CPU ticks are in 1/CLK_TCK seconds (usually 100 per second).
light_snapshot() {
  local c="nstat -az TcpActiveOpens TcpPassiveOpens TcpRetransSegs TcpExtListenOverflows TcpExtListenDrops IpExtInOctets IpExtOutOctets"
  tag "$1" "$c"; $c >>"$CAP_FILE" 2>&1 || true
  if [ -n "${SERVER_PID:-}" ] && [ -r "/proc/$SERVER_PID/stat" ]; then
    tag "$1" "server CPU ticks + context switches (/proc/$SERVER_PID)"
    awk '{print "utime_ticks", $14, "stime_ticks", $15}' "/proc/$SERVER_PID/stat" >>"$CAP_FILE"
    grep -E 'ctxt_switches' "/proc/$SERVER_PID/status" >>"$CAP_FILE" 2>/dev/null || true
    echo "clk_tck $(getconf CLK_TCK)" >>"$CAP_FILE"
  fi
}

# Heavy tools: ONLY called from probe runs. They slow the server down.
heavy_probe() {
  tag probe-mid "ss -s";  ss -s >>"$CAP_FILE" 2>&1 || true
  if [ -n "${SERVER_PID:-}" ]; then
    if [ "$PERF_PROBE" = "1" ]; then
      # perf samples where the CPU time goes (99 samples/s would be too few for a 10 s window, so 499 Hz).
      # The server was started with --perf-basic-prof so JS function names show up in the report.
      local pd="$CAP/$testid.perf.data"
      tag probe-mid "sudo -n perf record -F 499 -p $SERVER_PID -o $pd -- sleep $PROBE_SECS"
      sudo -n perf record -F 499 -p "$SERVER_PID" -o "$pd" -- sleep "$PROBE_SECS" >>"$CAP_FILE" 2>&1 || echo "perf record failed (sudo? perf_event_paranoid?)" >>"$CAP_FILE"
      tag probe-mid "sudo -n perf report --stdio --no-children --sort symbol (top 40)"
      sudo -n perf report -i "$pd" --stdio --no-children --sort symbol 2>/dev/null | grep -v '^#' | grep -v '^$' | head -40 >>"$CAP_FILE" || true
      tag probe-mid "same report, only lines that mention JSON"
      sudo -n perf report -i "$pd" --stdio --no-children --sort symbol 2>/dev/null | grep -i json >>"$CAP_FILE" || echo "(no JSON symbol found)" >>"$CAP_FILE"
    fi
    if command -v strace >/dev/null; then
      tag probe-mid "sudo -n timeout $PROBE_SECS strace -c -f -p $SERVER_PID"
      sudo -n timeout "$PROBE_SECS" strace -c -f -p "$SERVER_PID" >>"$CAP_FILE" 2>&1 || true
    fi
  else
    tag probe-mid "perf/strace SKIPPED (no server pid)"
  fi
}

run_k6() {   # run_k6 <rate> <size> <duration> <testid>   -> runs runner.sh in the foreground
  local rate="$1" size="$2" dur="$3"
  ( cd "$STAGE_DIR" && env RESULTS_NAME="$RESULTS_NAME" LABEL="$4" SCRIPT="$EXP_ID/script.js" \
      RATE="$rate" DURATION="$dur" WARMUP="$WARMUP" PAYLOAD_BYTES="$size" SHAPE="$SHAPE" \
      PHASE="${PHASE:-measured}" ECHO_ROUTE="$ECHO_ROUTE" \
      BASE_URL="$BASE_URL" ROUTE="$ROUTE" ./runner.sh )
}

record() { printf '%s\t%s\t%s\t%s\t%s\n' "$testid" "$1" "$2" "$3" "$4" >>"$RUNS_TSV"; }   # phase start end status
mk_testid() { local t="${EXP_ID,,}-${SHAPE}-s${1}-r${2}-$3"; echo "${t//./}"; }            # e09-obj-s1000-r8000-n1

# ---- 6. Main ------------------------------------------------------------------
n_sizes=$(wc -w <<<"$SIZES")
measured_runs=$(( n_sizes*N_STEPS*REPEATS ))
WARM_S=$(to_seconds "$WARMUP"); [ "$WARM_S" -gt 0 ] && WARM_S=$((WARM_S+2))
run_s=$(( $(to_seconds "$DURATION") + WARM_S + 15 ))             # + ~15 s settle/gap
total_s=$(( measured_runs*run_s ))
[ "$PROBES" = 1 ] && total_s=$(( total_s + n_sizes*($(to_seconds "$PROBE_DURATION") + WARM_S + 15 + 2*PROBE_SECS) ))
echo "== $EXP_ID (shape=$SHAPE): $measured_runs measured runs + $([ "$PROBES" = 1 ] && echo "$n_sizes" || echo 0) probe runs, results in $OUT =="
echo "== expected total time: about $(( total_s/60 )) min =="
if [ "$DRY_RUN" = "1" ]; then
  for i in $(seq 1 "$N_STEPS"); do for n in $(seq 1 "$REPEATS"); do for s in $SIZES; do
    r=$(awk -v k="$i" '{print $k}' <<<"$(rates_for "$s")")
    echo "  measured: step=$i repeat=$n size=${s}B rate=$r ($WARMUP warm-up + $DURATION)"
  done; done; done
  [ "$PROBES" = 1 ] && for s in $SIZES; do echo "  probe:    size=${s}B rate=$(probe_rate_for "$s") ($PROBE_DURATION, perf=$PERF_PROBE, strace)"; done
  exit 0
fi

preflight
mkdir -p "$CAP"; env_snapshot
printf 'testid\tphase\tstart_epoch\tend_epoch\tstatus\n' >"$RUNS_TSV"
start_server
echo_check

# 6a. measured runs: nothing heavy is attached, so the numbers stay clean
for i in $(seq 1 "$N_STEPS"); do
  for rep in $(seq 1 "$REPEATS"); do
    for s in $SIZES; do
      rate=$(awk -v k="$i" '{print $k}' <<<"$(rates_for "$s")")
      testid=$(mk_testid "$s" "$rate" "n$rep")
      echo; echo "== measured: step=$i size=${s}B rate=$rate repeat=$rep ($testid) =="
      settle
      CAP_FILE="$CAP/$testid.txt"; : >"$CAP_FILE"
      light_snapshot pre; t0=$(date +%s)
      PHASE=measured; status=ok
      run_k6 "$rate" "$s" "$DURATION" "$testid" || { status=FAILED; echo "!! $testid failed (dropped iterations or a threshold?) - check its manifest.md and discard if k6 was the bottleneck"; }
      light_snapshot post; record measured "$t0" "$(date +%s)" "$status"
    done
  done
done

# 6b. probe runs: short, separate, heavy tools allowed. NOT used for headline numbers.
if [ "$PROBES" = "1" ]; then
  echo; echo "== probes: restarting the server with --perf-basic-prof (writes /tmp/perf-<pid>.map so perf can name JS functions) =="
  start_server NODE_OPTIONS="--perf-basic-prof"
  for s in $SIZES; do
    rate=$(probe_rate_for "$s")
    testid=$(mk_testid "$s" "$rate" "probe")
    echo; echo "== probe: size=${s}B rate=$rate ($testid) - heavy tools attached, do not quote latency from this run =="
    settle
    CAP_FILE="$CAP/$testid.txt"; : >"$CAP_FILE"
    { command -v sudo >/dev/null && sudo -v; } 2>/dev/null || echo "WARN: no sudo; perf/strace will be skipped in this probe" >&2
    light_snapshot pre; t0=$(date +%s)
    PHASE=probe; status=ok
    run_k6 "$rate" "$s" "$PROBE_DURATION" "$testid" & kpid=$!
    seen=0; for _ in $(seq 1 60); do pgrep -x k6 >/dev/null && { seen=1; break; }; sleep 1; done   # wait until k6 is really generating load
    [ "$seen" = 1 ] || echo "WARN: k6 process not seen within 60 s; probing anyway (timing is off)" >&2
    # skip the warm-up (+2 s gap) and 5 s of the steady part, then probe while the load is steady
    sleep $(( $(to_seconds "$WARMUP") + 2 + 5 ))
    heavy_probe
    wait "$kpid" || status=FAILED
    light_snapshot post; record probe "$t0" "$(date +%s)" "$status"
  done
fi

echo; echo "Done. Zoom Grafana to the times in $RUNS_TSV (epoch seconds)."
echo "k6 summaries: $OUT/<timestamp>/*.json   captures: $CAP/<testid>.txt   env: $OUT/env.txt"
