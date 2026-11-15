#!/usr/bin/env bash
# B1: Ramp to collapse (the baseline)
# ONE command runs a whole pass:
#     ./run.sh coarse     first pass: wide rates, 30 s, 1 repeat -> finds roughly where it collapses
#     ./run.sh fine       second pass: even steps around that point, 60 s, 3 repeats, + probe runs
#
# What a pass does (so I can re-read it later):
#   1. preflight   - tools, Prometheus, node_exporter, ingest-api build, Postgres container
#   2. pinning     - docker update --cpuset-cpus so server / postgres / k6 / monitoring use different cores
#   3. env snapshot- versions, Postgres settings, pinning, saved next to the results
#   4. for every repeat x rate: restart server, warm up, truncate table, k6 via runner.sh,
#                    light pg snapshots before/after, row-count audit, one summary row
#   5. probes (fine pass only) - SEPARATE short runs where heavy tools (psql wait events, iostat...) are allowed.
#                    Never used for headline numbers (Stage 0 lesson: strace inside a run polluted it).
#
# Usage:  DRY_RUN=1 ./run.sh coarse            (print the plan, run nothing)
#         RATES="500 1000" REPEATS=1 ./run.sh coarse     (small trial)
#         MANAGE_SERVER=0 ./run.sh coarse      (I started ingest-api myself; then no per-run restart)
#         PG_CONTAINER=my-postgres ./run.sh fine         (if auto-detection finds 0 or 2+ containers)
#
# CPU plan (12 logical CPUs = 6 cores, SMT pairs 0/6 1/7 2/8 3/9 4/10 5/11). Each component gets whole cores:
#   server   0,6         postgres 1,7,2,8 (2 cores, matches cpus: 2)
#   k6       3,9,4,10    monitoring 5,11
set -euo pipefail

MODE="${1:-coarse}"
case "$MODE" in coarse|fine) ;; *) echo "usage: $0 coarse|fine" >&2; exit 1;; esac
EXP_ID="B1"

# ---- 1. Settings (all can be overridden from the environment) ---------------
if [ "$MODE" = "coarse" ]; then
  RATES="${RATES:-100 250 500 1000 1500 2000 3000 4000 6000}"   # wide steps to find the knee
  REPEATS="${REPEATS:-1}"; DURATION="${DURATION:-30s}"; PROBES="${PROBES:-0}"
  RESULTS_NAME="${RESULTS_NAME:-b1-coarse}"
else
  REPEATS="${REPEATS:-3}"; DURATION="${DURATION:-60s}"; PROBES="${PROBES:-1}"
  RESULTS_NAME="${RESULTS_NAME:-b1-fine}"
fi
PROBE_DURATION="${PROBE_DURATION:-30s}"
BASE_URL="${BASE_URL:-http://localhost:8000}"
PROM_URL="${PROM_URL:-http://localhost:9090}"
MANAGE_SERVER="${MANAGE_SERVER:-1}"
SERVER_CPUS="${SERVER_CPUS:-0,6}"
PG_CPUS="${PG_CPUS:-1,7,2,8}"
export K6_CPUS="${K6_CPUS:-3,9,4,10}"          # runner.sh reads this (taskset for k6)
MON_CPUS="${MON_CPUS:-5,11}"
PG_CONTAINER="${PG_CONTAINER:-}"                # empty = auto-detect
MON_CONTAINERS="${MON_CONTAINERS:-}"            # empty = auto-detect (prometheus, grafana, node-exporter)
PGUSER="${PGUSER:-eventpulse}"; PGDB="${PGDB:-eventpulse}"
POOL_MAX="${POOL_MAX:-}"                        # empty = whatever services/ingest-api/.env says (default 10)
KEEP_PINNING="${KEEP_PINNING:-0}"               # 1 = do not restore the old cpusets at the end
DRY_RUN="${DRY_RUN:-0}"
OVERHEAD_S=45                                   # restart + warm-up + truncate + settle per run (rough)

# ---- 2. Paths ----------------------------------------------------------------
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"      # loadtests/k6/stage-1/B1
STAGE_DIR="$(dirname "$HERE")"                             # loadtests/k6/stage-1
RUNNER_DIR="$(dirname "$STAGE_DIR")"                       # loadtests/k6  (runner.sh lives here)
REPO="$(cd "$RUNNER_DIR/../.." && pwd)"
API_DIR="$REPO/services/ingest-api"
OUT="$HERE/results/$RESULTS_NAME"                    # runner.sh writes <timestamp>/ folders in here
CAP="$OUT/captures"; RUNS_TSV="$OUT/runs.tsv"; SUMMARY="$OUT/summary.tsv"
SUGGEST="$HERE/results/b1-coarse/suggestion.env"

if [ "$MODE" = "fine" ] && [ -z "${RATES:-}" ]; then
  [ -f "$SUGGEST" ] || { echo "ERROR: no RATES given and no $SUGGEST. Run './run.sh coarse' first (or pass RATES=...)." >&2; exit 1; }
  # shellcheck disable=SC1090
  . "$SUGGEST"                                             # sets RATES and PROBE_RATES
fi
PROBE_RATES="${PROBE_RATES:-}"
[ "$PROBES" = "1" ] && [ -z "$PROBE_RATES" ] && PROBES=0

die() { echo "ERROR: $*" >&2; exit 1; }
to_seconds() { case "$1" in *h) echo $(( ${1%h}*3600 ));; *m) echo $(( ${1%m}*60 ));; *s) echo "${1%s}";; *) echo "$1";; esac; }

# ---- 3. Plan / dry run ---------------------------------------------------------
n_rates=$(wc -w <<<"$RATES")
n_probe=0; [ "$PROBES" = "1" ] && n_probe=$(wc -w <<<"$PROBE_RATES")
total_s=$(( n_rates*REPEATS*($(to_seconds "$DURATION")+OVERHEAD_S) + n_probe*($(to_seconds "$PROBE_DURATION")+OVERHEAD_S+20) ))
echo "== $EXP_ID $MODE: $((n_rates*REPEATS)) measured runs + $n_probe probe runs, results in $OUT =="
echo "== rates: $RATES | duration $DURATION | repeats $REPEATS | expected total about $(( total_s/60 )) min =="
if [ "$DRY_RUN" = "1" ]; then
  for rep in $(seq 1 "$REPEATS"); do for r in $RATES; do echo "  measured: repeat=$rep rate=$r ($DURATION)"; done; done
  for r in $( [ "$PROBES" = 1 ] && echo "$PROBE_RATES" ); do echo "  probe:    rate=$r ($PROBE_DURATION)"; done
  echo "  CPU plan: server=$SERVER_CPUS postgres=$PG_CPUS k6=$K6_CPUS monitoring=$MON_CPUS"
  exit 0
fi

# ---- 4. Helpers ------------------------------------------------------------------
pgq()  { docker exec -i "$PG_CONTAINER" psql -U "$PGUSER" -d "$PGDB" -X -At -c "$1"; }   # one value / rows, no decoration
pgqt() { docker exec -i "$PG_CONTAINER" psql -U "$PGUSER" -d "$PGDB" -X -c "$1"; }       # pretty table for captures

detect_containers() {
  if [ -z "$PG_CONTAINER" ]; then
    local found; found=$(docker ps --format '{{.Names}} {{.Image}}' | awk '$2 ~ /^postgres/ {print $1}')
    [ "$(wc -w <<<"$found")" = 1 ] || die "found [$found] postgres containers; set PG_CONTAINER=<name> (docker ps to see names)"
    PG_CONTAINER="$found"
  fi
  if [ -z "$MON_CONTAINERS" ]; then
    MON_CONTAINERS=$(docker ps --format '{{.Names}} {{.Image}}' | awk '$2 ~ /prometheus|grafana|node-exporter|node_exporter/ {print $1}' | tr '\n' ' ')
  fi
}

preflight() {
  for c in k6 taskset curl docker python3; do command -v "$c" >/dev/null || die "'$c' not found in PATH"; done
  command -v iostat >/dev/null || echo "WARN: iostat missing (apt install sysstat); probe runs will skip it" >&2
  [ -x "$RUNNER_DIR/runner.sh" ] || die "$RUNNER_DIR/runner.sh missing or not executable"
  curl -fsS --max-time 3 "$PROM_URL/-/ready" >/dev/null || die "Prometheus not ready at $PROM_URL (docker compose -f infra/compose/docker-compose.yml up -d)"
  curl -fsS --max-time 3 "$PROM_URL/api/v1/query?query=up%7Bjob%3D%22node%22%7D" | grep -q '"value":\[[^]]*"1"\]' \
    || echo "WARN: node_exporter (job=node) is not 'up'; host panels will be empty" >&2
  detect_containers
  docker exec "$PG_CONTAINER" true || die "cannot docker exec into $PG_CONTAINER"
  pgq "select 1" >/dev/null || die "psql inside $PG_CONTAINER failed (PGUSER=$PGUSER PGDB=$PGDB?)"
  pgq "select count(*) from events" >/dev/null || die "table 'events' not found: run the migrations (pnpm migrate:up)"
  if [ "$MANAGE_SERVER" = 1 ]; then
    [ -f "$API_DIR/dist/main.js" ] || die "$API_DIR/dist/main.js missing: run 'pnpm build' in services/ingest-api"
    [ -f "$API_DIR/.env" ] || die "$API_DIR/.env missing"
  fi
  if [ -z "${API_KEY:-}" ]; then      # first key in API_KEYS=key:tenant,key2:tenant2
    API_KEY=$(grep -E '^API_KEYS=' "$API_DIR/.env" 2>/dev/null | head -1 | cut -d= -f2- | tr -d '"' | cut -d, -f1 | cut -d: -f1 || true)
    [ -n "$API_KEY" ] || die "API_KEY not set and not found in $API_DIR/.env (API_KEYS=key:tenant)"
  fi
  export API_KEY
  echo "postgres container: $PG_CONTAINER | monitoring containers: ${MON_CONTAINERS:-none found}"
}

# ---- 5. CPU pinning (docker update changes a running container; no compose edit needed) ----
declare -A OLD_CPUSET=()
pin_container() {   # pin_container <name> <cpus>
  local c="$1" cpus="$2"
  OLD_CPUSET["$c"]="$(docker inspect -f '{{.HostConfig.CpusetCpus}}' "$c")"
  docker update --cpuset-cpus "$cpus" "$c" >/dev/null && echo "pinned $c -> $cpus (was '${OLD_CPUSET[$c]}')"
}
restore_pinning() {
  [ "$KEEP_PINNING" = 1 ] && return 0
  for c in "${!OLD_CPUSET[@]}"; do
    local old="${OLD_CPUSET[$c]}"; [ -n "$old" ] || old="0-$(( $(nproc) - 1 ))"   # was unpinned -> all cpus again
    docker update --cpuset-cpus "$old" "$c" >/dev/null 2>&1 || true
  done
}

# ---- 6. Server lifecycle (ingest-api; I do NOT use the Stage 0 server-runner.sh, it is hard-wired to labs/stage-0) ----
SERVER_PID=""
start_server() {
  if [ "$MANAGE_SERVER" != 1 ]; then return 0; fi
  stop_server
  ( cd "$API_DIR" && exec env ${POOL_MAX:+PG_POOL_MAX=$POOL_MAX} taskset -c "$SERVER_CPUS" node --env-file=.env dist/main.js ) >"$OUT/server.out" 2>&1 &
  SERVER_PID=$!       # exec keeps the pid: this is the node pid
  for _ in $(seq 1 30); do curl -fsS --max-time 1 "$BASE_URL/healthz" >/dev/null 2>&1 && break; sleep 1; done
  curl -fsS --max-time 2 "$BASE_URL/readyz" >/dev/null || die "ingest-api not ready (see $OUT/server.out)"
}
stop_server() {
  [ "$MANAGE_SERVER" = 1 ] || return 0
  if [ -n "${SERVER_PID:-}" ] && kill -0 "$SERVER_PID" 2>/dev/null; then kill "$SERVER_PID" 2>/dev/null || true; wait "$SERVER_PID" 2>/dev/null || true; fi
  SERVER_PID=""
}

EXPORTER_PID=""
cleanup() { stop_server; [ -n "$EXPORTER_PID" ] && kill "$EXPORTER_PID" 2>/dev/null || true; restore_pinning; }
trap cleanup EXIT

# ---- 7. Per-run helpers ---------------------------------------------------------
wait_db_quiet() {   # wait until no other backend is working (a collapsed run leaves queries finishing)
  local n waited=0
  while :; do
    n=$(pgq "select count(*) from pg_stat_activity where datname='$PGDB' and state<>'idle' and pid<>pg_backend_pid()")
    [ "${n:-0}" -eq 0 ] && return 0
    [ "$waited" -ge 120 ] && { echo "WARN: $n backends still active after 120 s, continuing"; return 0; }
    echo "  waiting for postgres to go quiet ($n active)..."; sleep 3; waited=$((waited+3))
  done
}
reset_db() {        # empty table so size/index growth of earlier runs cannot contaminate this one
  pgq "TRUNCATE events" >/dev/null
  pgq "SELECT pg_stat_statements_reset()" >/dev/null 2>&1 || echo "WARN: pg_stat_statements_reset failed (extension missing?)" >&2
  pgq "CHECKPOINT" >/dev/null      # flush dirty pages now, so a checkpoint from the previous run does not land in this one
}
warmup() {          # 100 requests, 10 at a time: opens the pool connections and warms up V8; truncated afterwards
  seq 1 100 | xargs -P 10 -I{} sh -c 'curl -s -o /dev/null -X POST "'"$BASE_URL"'/v1/events" \
    -H "Content-Type: application/json" -H "Authorization: Bearer $API_KEY" \
    -d "{\"event_id\":\"$(cat /proc/sys/kernel/random/uuid)\",\"type\":\"warmup\",\"occurred_at\":\"$(date -u +%Y-%m-%dT%H:%M:%S.000Z)\",\"schema_version\":1,\"payload\":{\"w\":1}}"'
}

CAP_FILE=""
tag() { printf '\n=== exp=%s testid=%s phase=%s ts=%s cmd: %s\n' "$EXP_ID" "$testid" "$1" "$(date -Is)" "$2" >>"$CAP_FILE"; }
# Cheap snapshots: one small query each, before and after the run (never during it). Counters: subtract pre from post.
light_snapshot() {
  tag "$1" "pg_stat_database + checkpoints + /debug/stats"
  pgqt "select xact_commit, xact_rollback, blks_read, blks_hit, tup_inserted, temp_files from pg_stat_database where datname='$PGDB'" >>"$CAP_FILE" 2>&1 || true
  pgqt "select checkpoints_timed, checkpoints_req, buffers_checkpoint, buffers_backend from pg_stat_bgwriter" >>"$CAP_FILE" 2>&1 || true
  curl -fsS --max-time 2 "$BASE_URL/debug/stats" >>"$CAP_FILE" 2>&1 || true; echo >>"$CAP_FILE"
}
# Heavy tools: ONLY from probe runs. They cost CPU on the machine.
heavy_probe() {
  tag probe-mid "pg_stat_activity grouped by state / wait event"
  pgqt "select state, wait_event_type, wait_event, count(*) from pg_stat_activity where datname='$PGDB' group by 1,2,3 order by 4 desc" >>"$CAP_FILE" 2>&1 || true
  tag probe-mid "blocked and blocking"
  pgqt "select pid, pg_blocking_pids(pid) as blocked_by, wait_event_type, wait_event, left(query,60) q from pg_stat_activity where cardinality(pg_blocking_pids(pid))>0" >>"$CAP_FILE" 2>&1 || true
  tag probe-mid "pg_stat_statements top 5 by total time"
  pgqt "select calls, round(mean_exec_time::numeric,3) mean_ms, round(total_exec_time::numeric,0) total_ms, left(query,70) q from pg_stat_statements order by total_exec_time desc limit 5" >>"$CAP_FILE" 2>&1 || true
  tag probe-mid "iostat -x 1 5"; command -v iostat >/dev/null && iostat -x 1 5 >>"$CAP_FILE" 2>&1 || true
  tag probe-mid "vmstat 1 5";   vmstat 1 5 >>"$CAP_FILE" 2>&1 || true
  tag probe-mid "pidstat -u -p $SERVER_PID 1 5"; command -v pidstat >/dev/null && [ -n "$SERVER_PID" ] && pidstat -u -p "$SERVER_PID" 1 5 >>"$CAP_FILE" 2>&1 || true
}

run_k6() {   # run_k6 <rate> <duration> <testid>
  ( cd "$RUNNER_DIR" && env RESULTS_ROOT="$HERE/results" RESULTS_NAME="$RESULTS_NAME" LABEL="$3" SCRIPT="stage-1/B1/script.js" \
      RATE="$1" DURATION="$2" PHASE="$PHASE" API_KEY="$API_KEY" BASE_URL="$BASE_URL" ROUTE="/healthz" ./runner.sh )
}
record() { printf '%s\t%s\t%s\t%s\t%s\n' "$testid" "$1" "$2" "$3" "$4" >>"$RUNS_TSV"; }

one_run() {  # one_run <phase> <rate> <duration> <rep>
  PHASE="$1"; local rate="$2" dur="$3" rep="$4"
  testid="b1-${PHASE:0:1}-r${rate}-n${rep}"
  [ "$PHASE" = probe ] && testid="b1-probe-r${rate}"
  echo; echo "== $PHASE: rate=$rate repeat=$rep ($testid) =="
  CAP_FILE="$CAP/$testid.txt"; : >"$CAP_FILE"
  stop_server; wait_db_quiet; start_server; warmup; wait_db_quiet; reset_db
  sleep 5                                    # let the 5 s of reset/checkpoint I/O finish before measuring
  light_snapshot pre; local t0; t0=$(date +%s); local status=ok
  if [ "$PHASE" = probe ]; then
    run_k6 "$rate" "$dur" "$testid" & local kpid=$!
    for _ in $(seq 1 60); do pgrep -x k6 >/dev/null && break; sleep 1; done
    sleep $(( $(to_seconds "$dur") / 3 )); heavy_probe
    wait "$kpid" || status=FAILED
  else
    run_k6 "$rate" "$dur" "$testid" || { status=FAILED; echo "!! $testid failed: see its manifest.md"; }
  fi
  sleep 3; wait_db_quiet                      # let requests that were still running finish before counting rows
  local rows; rows=$(pgq "select count(*) from events")
  light_snapshot post; record "$PHASE" "$t0" "$(date +%s)" "$status"
  local js; js=$(ls -t "$OUT"/*/"${testid}_1-lap.json" 2>/dev/null | head -1 || true)
  if [ -n "$js" ]; then
    python3 "$HERE/analyze.py" row --json "$js" --testid "$testid" --phase "$PHASE" --rate "$rate" --rows "$rows" >>"$SUMMARY"
    tail -1 "$SUMMARY" | awk -F'\t' '{printf "   -> achieved %s rps, err %s%%, p99 %s ms, dropped %s, audit: %s | %s\n",$4,$9,$12,$14,$16,$17}'
  else
    echo "   !! no summary json for $testid (k6 failed before finishing?)"
  fi
}

# ---- 8. Main ----------------------------------------------------------------------
preflight
mkdir -p "$CAP"
printf 'testid\tphase\tstart_epoch\tend_epoch\tstatus\n' >"$RUNS_TSV"
python3 - "$SUMMARY" <<'PY'
import sys
cols = ["testid","phase","rate","achieved_rps","reqs","n202","n503","n_other","err_pct","p50_ms","p95_ms","p99_ms","max_ms","dropped","rows","audit","verdict"]
open(sys.argv[1], "w").write("\t".join(cols) + "\n")
PY

pin_container "$PG_CONTAINER" "$PG_CPUS"
for c in $MON_CONTAINERS; do pin_container "$c" "$MON_CPUS"; done

{   # environment snapshot
  echo "experiment: $EXP_ID ($MODE)   date: $(date -Is)"
  echo "git: $(git -C "$REPO" rev-parse --short HEAD 2>/dev/null || echo n/a), $(git -C "$REPO" status --porcelain 2>/dev/null | wc -l) dirty files"
  echo "kernel: $(uname -r)   node: $(node -v)   k6: $(k6 version | head -1)   cpus: $(nproc)"
  echo "pinning: server=$SERVER_CPUS postgres=$PG_CPUS k6=$K6_CPUS monitoring=$MON_CPUS ($MON_CONTAINERS)"
  echo "settings: RATES=[$RATES] REPEATS=$REPEATS DURATION=$DURATION POOL_MAX=${POOL_MAX:-from .env}"
  echo "--- postgres container ---"
  docker inspect -f 'image={{.Config.Image}} nanocpus={{.HostConfig.NanoCpus}} memory={{.HostConfig.Memory}} cpuset={{.HostConfig.CpusetCpus}}' "$PG_CONTAINER"
  pgq "select version()" || true
  for s in max_connections shared_buffers synchronous_commit fsync wal_level checkpoint_timeout max_wal_size; do echo "$s = $(pgq "show $s" 2>/dev/null)"; done
  echo "--- cpu model ---"; lscpu | grep -E 'Model name|Thread|Core|Socket' || true
} >"$OUT/env.txt"

# Postgres container CPU/mem/disk -> Prometheus (needs cgroup v2; if it cannot start, the Postgres CPU panels stay empty)
PG_CONTAINER="$PG_CONTAINER" bash "$REPO/infra/scripts/pg-container-exporter.sh" 2>"$OUT/pg-exporter.log" &
EXPORTER_PID=$!; sleep 2
kill -0 "$EXPORTER_PID" 2>/dev/null || { echo "WARN: pg-container-exporter did not start ($(tail -1 "$OUT/pg-exporter.log")); Postgres CPU panels will be empty" >&2; EXPORTER_PID=""; }

# measured runs: repeat is the OUTER loop, so every repeat sweeps all rates (drift hits every rate the same way)
for rep in $(seq 1 "$REPEATS"); do
  for rate in $RATES; do one_run measured "$rate" "$DURATION" "$rep"; done
done
# probe runs: short, separate
if [ "$PROBES" = 1 ]; then for rate in $PROBE_RATES; do one_run probe "$rate" "$PROBE_DURATION" 1; done; fi

echo; python3 "$HERE/analyze.py" table "$SUMMARY"
if [ "$MODE" = coarse ]; then echo; python3 "$HERE/analyze.py" suggest "$SUMMARY" "$SUGGEST"; echo "Next: ./run.sh fine   (it reads $SUGGEST)"; fi
echo; echo "Done. Zoom Grafana to the times in $RUNS_TSV (epoch seconds). Summary: $SUMMARY  captures: $CAP  env: $OUT/env.txt"
