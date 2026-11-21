#!/usr/bin/env bash
# B7: Payload size (200 B, 500 B, 2 KB, 10 KB, 100 KB request bodies)
# ONE command runs the whole experiment:
#     ./run.sh
#
# Idea: every event carries a payload. The bigger the payload, the more bytes the server reads and parses (JSON),
# the more bytes it sends to Postgres, the more WAL Postgres writes, and above about 2 KB per row Postgres moves the
# jsonb value into a separate TOAST table (cut into ~2 KB chunks, one extra insert + one extra index entry per chunk).
# I change only the size of the request body and measure, per size: the highest rps the server accepts, the WAL
# bytes written per event, and how much disk (heap, TOAST, indexes) one event takes.
#
# Run design (same server setup as B3 to B6: pool 25, 100 tenants, synchronous_commit on, server restarted before every run):
#   measured  size {200, 500, 2048, 10240, 102400} x phase {at, over}, REPEATS times. Repeat is the outer loop; odd
#             repeats go from small to big, even repeats from big to small, so drift hits every size equally.
#               at    a rate just under the capacity I predicted for that size (compare latency at about equal load)
#               over  a rate above the capacity of that size: max accepted rps (the "knee" is where "at" starts to fail)
#   probe     ONE short check, separate from the runs: with the server's DEFAULT body limit (64 KB), a 60 KB and a
#             100 KB body are sent with curl. It shows why BODY_LIMIT has to be raised for this experiment.
#
# BODY_LIMIT: the ingest-api rejects JSON bodies bigger than BODY_LIMIT (default 64kb) with a 413. The measured runs
# start the server with BODY_LIMIT=1mb (env var, the code is unchanged) for every size, so all sizes see the same limit.
#
# DISK: at 100 KB per event one run can write several GB (heap + TOAST). The table is truncated before every run
# (freeing the space) and once more when the script ends. Preflight needs DISK_MIN_GB free.
#
# CPU pinning: NOT done here. Containers come from docker compose (cpusets are in the compose file).
#   server 0,6 (taskset below)   postgres 1,7,2,8   k6 3,9,4,10   monitoring + toxiproxy 5,11
#
# This script changes: the rows of events (TRUNCATE, also at the end), CHECKPOINTs, pg_stat_statements_reset. It does
# NOT change any Postgres setting, sysctl or container. No sudo.
#
# Usage:  DRY_RUN=1 ./run.sh                                      (print the plan, run nothing)
#         SIZES="500 102400" REPEATS=1 DURATION=30s PROBE_413=0 RESULTS_NAME=b7-trial ./run.sh   (trial, NOT a result)
#         AT_RATES="4000 4000 3900 3700 1500" OVER_RATES="8000 8000 8000 6000 3000" ./run.sh      (override the rates;
#                                                                  the lists match SIZES one to one)
set -euo pipefail

EXP_ID="B7"
export PYTHONDONTWRITEBYTECODE=1                 # analyze.py loads hook.py; do not leave a __pycache__ in the repo

# ---- 1. Settings (all can be overridden from the environment) ---------------
SIZES="${SIZES:-200 500 2048 10240 102400}"      # body size in bytes
PHASES="${PHASES:-at over}"
# Offered rates, one per size (same order as SIZES). at = about 93 % of my predicted capacity, over = well above it.
# Predicted capacity (before the run): 4400 4300 4200 4000 1700 req/s, see Explanation.md.
AT_RATES="${AT_RATES:-4000 4000 3900 3700 1500}"
OVER_RATES="${OVER_RATES:-8000 8000 8000 6000 3000}"   # lower for big bodies: 3000 x 100 KB is already 300 MB/s of generator work
TENANTS="${TENANTS:-100}"
POOL="${POOL:-25}"                               # PG_POOL_MAX, same as B3 to B6
REPEATS="${REPEATS:-3}"
DURATION="${DURATION:-45s}"
PROBE_413="${PROBE_413:-1}"                      # 1 = run the body-limit probe at the end
SERVER_BODY_LIMIT="${SERVER_BODY_LIMIT:-1mb}"    # BODY_LIMIT given to the server in the measured runs
DISK_MIN_GB="${DISK_MIN_GB:-20}"
KEY_PREFIX="${KEY_PREFIX:-epk_b7_}"              # api key i = KEY_PREFIX + i, tenant i = t<i>
RESULTS_NAME="${RESULTS_NAME:-b7}"
BASE_URL="${BASE_URL:-http://localhost:8000}"
PROM_URL="${PROM_URL:-http://localhost:9090}"
MANAGE_SERVER="${MANAGE_SERVER:-1}"
SERVER_CPUS="${SERVER_CPUS:-0,6}"
export K6_CPUS="${K6_CPUS:-3,9,4,10}"            # runner.sh reads this (taskset for k6)
PG_CONTAINER="${PG_CONTAINER:-}"                 # empty = auto-detect
PGUSER="${PGUSER:-eventpulse}"; PGDB="${PGDB:-eventpulse}"
DRY_RUN="${DRY_RUN:-0}"
OVERHEAD_S=45                                    # restart + warm-up + truncate + checkpoint + settle per run (rough)

# ---- 2. Paths ----------------------------------------------------------------
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"      # loadtests/k6/stage-1/B7
STAGE_DIR="$(dirname "$HERE")"                             # loadtests/k6/stage-1
RUNNER_DIR="$(dirname "$STAGE_DIR")"                       # loadtests/k6  (runner.sh lives here)
REPO="$(cd "$RUNNER_DIR/../.." && pwd)"
API_DIR="$REPO/services/ingest-api"
ANALYZE="$RUNNER_DIR/analyze.py"                           # shared analyzer
OUT="$HERE/results/$RESULTS_NAME"
CAP="$OUT/captures"; RUNS_TSV="$OUT/runs.tsv"; SUMMARY="$OUT/summary.tsv"

die() { echo "ERROR: $*" >&2; exit 1; }
to_seconds() { case "$1" in *h) echo $(( ${1%h}*3600 ));; *m) echo $(( ${1%m}*60 ));; *s) echo "${1%s}";; *) echo "$1";; esac; }

# sizes for repeat number $1: odd = as listed, even = reversed
sizes_for_rep() { if [ $(( $1 % 2 )) -eq 1 ]; then echo "$SIZES"; else echo "$SIZES" | tr ' ' '\n' | tac | tr '\n' ' '; fi; }
# nth_word "<list>" <n> : the n-th (1-based) word of a list
nth_word() { echo "$1" | awk -v n="$2" '{print $n}'; }
# rate_for <size> <at|over>: the rate in the list at the same position as the size in SIZES
rate_for() {
  local i=1 s
  for s in $SIZES; do
    if [ "$s" = "$1" ]; then
      if [ "$2" = at ]; then nth_word "$AT_RATES" "$i"; else nth_word "$OVER_RATES" "$i"; fi
      return 0
    fi
    i=$((i+1))
  done
  return 1
}
n_words() { wc -w <<<"$1" | tr -d ' '; }
[ "$(n_words "$SIZES")" = "$(n_words "$AT_RATES")" ] && [ "$(n_words "$SIZES")" = "$(n_words "$OVER_RATES")" ] \
  || die "SIZES, AT_RATES and OVER_RATES must have the same number of entries (they match one to one)"

# ---- 3. Plan / dry run ---------------------------------------------------------
n_sizes=$(n_words "$SIZES"); n_phases=$(n_words "$PHASES")
dur_s=$(to_seconds "$DURATION")
n_meas=$(( n_sizes*n_phases*REPEATS ))
total_s=$(( n_meas*(dur_s+OVERHEAD_S) + (PROBE_413==1 ? 60 : 0) ))
echo "== $EXP_ID: $n_meas measured runs$( [ "$PROBE_413" = 1 ] && echo " + 1 body-limit probe"), results in $OUT =="
echo "== pool $POOL | tenants $TENANTS | sizes $SIZES | phases $PHASES | duration $DURATION | repeats $REPEATS | expected total about $(( total_s/60 )) min =="
if [ "$DRY_RUN" = "1" ]; then
  for rep in $(seq 1 "$REPEATS"); do echo "  repeat $rep size order: $(sizes_for_rep "$rep")"; done
  for s in $SIZES; do echo "  size $s B: at=$(rate_for "$s" at) over=$(rate_for "$s" over) req/s"; done
  [ "$PROBE_413" = 1 ] && echo "  probe (last): server with default BODY_LIMIT=64kb, curl a 60000 B and a 100000 B body, record the status codes"
  echo "  server BODY_LIMIT in the measured runs: $SERVER_BODY_LIMIT | CPU plan (from compose, not changed here): server=$SERVER_CPUS k6=$K6_CPUS"
  echo "  changes made: TRUNCATE events (before every run and at the end), CHECKPOINT, pg_stat_statements_reset. No sudo, no Postgres settings."
  exit 0
fi

# ---- 4. Helpers ------------------------------------------------------------------
pgq()  { docker exec -i "$PG_CONTAINER" psql -U "$PGUSER" -d "$PGDB" -X -At -c "$1"; }   # one value / rows, no decoration
pgqt() { docker exec -i "$PG_CONTAINER" psql -U "$PGUSER" -d "$PGDB" -X -c "$1"; }       # pretty table for captures

find_pg_container() {
  if [ -z "$PG_CONTAINER" ]; then
    local found; found=$(docker ps -a --format '{{.Names}} {{.Image}}' | awk '$2 ~ /^postgres:/ {print $1}')
    [ "$(wc -w <<<"$found")" = 1 ] || die "found [$found] postgres containers; set PG_CONTAINER=<name>"
    PG_CONTAINER="$found"
  fi
}

preflight() {
  for c in k6 taskset curl docker python3 tac seq awk jq; do command -v "$c" >/dev/null || die "'$c' not found in PATH"; done
  [ -x "$RUNNER_DIR/runner.sh" ] || die "$RUNNER_DIR/runner.sh missing or not executable"
  [ -f "$ANALYZE" ] || die "$ANALYZE missing"
  curl -fsS --max-time 3 "$PROM_URL/-/ready" >/dev/null || die "Prometheus not ready at $PROM_URL (docker compose -f infra/compose/docker-compose.yml up -d)"
  curl -fsS --max-time 3 "$PROM_URL/api/v1/query?query=up%7Bjob%3D%22postgres%22%7D" | grep -q '"value":\[[^]]*"1"\]' \
    || die "postgres-exporter (job=postgres) is not 'up' (docker compose -f infra/compose/docker-compose.yml up -d postgres-exporter)"
  curl -fsS --max-time 3 "$PROM_URL/api/v1/query?query=up%7Bjob%3D%22node%22%7D" | grep -q '"value":\[[^]]*"1"\]' \
    || echo "WARN: node_exporter (job=node) is not 'up'; host panels will be empty" >&2
  find_pg_container
  pgq "select count(*) from events" >/dev/null || die "cannot query table 'events' in $PG_CONTAINER (migrations? PGUSER/PGDB?)"
  pgq "select wal_bytes from pg_stat_wal" >/dev/null 2>&1 || die "pg_stat_wal not available (needs Postgres 14 or newer)"
  [ "$(pgq "show synchronous_commit")" = on ] || die "synchronous_commit is not 'on' (a leftover from an earlier experiment? ./stage-1/B6/run.sh RESTORE)"
  [ -f "$API_DIR/dist/main.js" ] || die "$API_DIR/dist/main.js missing: run 'pnpm build' in services/ingest-api"
  [ -f "$API_DIR/.env" ] || die "$API_DIR/.env missing"
  local free; free=$(df -P -BG "$REPO" | awk 'NR==2 {gsub("G","",$4); print $4}')
  [ "${free:-0}" -ge "$DISK_MIN_GB" ] || die "only ${free:-?} GB free under $REPO, need $DISK_MIN_GB GB (100 KB events write several GB per run). Free space or set DISK_MIN_GB."
  if [ "$MANAGE_SERVER" = 1 ] && curl -fsS --max-time 2 "$BASE_URL/healthz" >/dev/null 2>&1; then
    die "something already answers on $BASE_URL; stop it (run.sh starts its own ingest-api per run) or set MANAGE_SERVER=0"
  fi
  echo "postgres container: $PG_CONTAINER | ${free} GB free | server connects directly to Postgres (no proxy)"
}

# ---- 5. Server lifecycle (ingest-api) ---------------------------------------------
SERVER_PID=""
# start_server <pool> <body limit>. Env vars beat --env-file, so API_KEYS/COUNTER_MODE/PG_POOL_MAX/BODY_LIMIT here win over .env.
start_server() {
  [ "$MANAGE_SERVER" = 1 ] || return 0
  stop_server
  local keys; keys=$(seq 0 $(( TENANTS - 1 )) | awk -v p="$KEY_PREFIX" '{printf "%s%s%d:t%d", (NR>1?",":""), p, $1, $1}')
  ( cd "$API_DIR" && exec env API_KEYS="$keys" COUNTER_MODE=off PG_POOL_MAX="$1" BODY_LIMIT="$2" taskset -c "$SERVER_CPUS" node --env-file=.env dist/main.js ) >"$OUT/server.out" 2>&1 &
  SERVER_PID=$!       # exec keeps the pid: this is the node pid
  for _ in $(seq 1 30); do curl -fsS --max-time 1 "$BASE_URL/healthz" >/dev/null 2>&1 && break; sleep 1; done
  curl -fsS --max-time 2 "$BASE_URL/readyz" >/dev/null || die "ingest-api not ready (see $OUT/server.out)"
  local got; got=$(curl -fsS --max-time 2 "$BASE_URL/debug/stats" | python3 -c 'import sys,json; print(json.load(sys.stdin)["pool"]["max"])' 2>/dev/null || echo "?")
  [ "$got" = "$1" ] || die "server reports pool max '$got', I asked for '$1' (see $OUT/server.out)"
}
stop_server() {
  [ "$MANAGE_SERVER" = 1 ] || return 0
  if [ -n "${SERVER_PID:-}" ] && kill -0 "$SERVER_PID" 2>/dev/null; then kill "$SERVER_PID" 2>/dev/null || true; wait "$SERVER_PID" 2>/dev/null || true; fi
  SERVER_PID=""
}

PGC_PID=""; WAL_PID=""; TRUNC_TOUCHED=0
cleanup() {
  stop_server
  [ -n "$PGC_PID" ] && kill "$PGC_PID" 2>/dev/null || true
  # pg-wal-exporter stops its own children (docker exec, read loop) when it gets TERM; I give it a moment to do that
  if [ -n "$WAL_PID" ]; then kill "$WAL_PID" 2>/dev/null || true; sleep 1; fi
  # give the disk back: the last measured run can leave GBs of 100 KB events in the table
  if [ "$TRUNC_TOUCHED" = 1 ]; then pgq "TRUNCATE events" >/dev/null 2>&1 && echo "events truncated (disk space freed)" || echo "WARN: could not truncate events at the end" >&2; fi
  return 0
}
trap cleanup EXIT
trap 'exit 130' INT TERM        # so Ctrl+C also runs the EXIT trap above

# ---- 6. Per-run helpers ---------------------------------------------------------
wait_db_quiet() {   # wait until no other backend is working
  local n waited=0
  while :; do
    n=$(pgq "select count(*) from pg_stat_activity where datname='$PGDB' and state<>'idle' and pid<>pg_backend_pid()")
    [ "${n:-0}" -eq 0 ] && return 0
    [ "$waited" -ge 120 ] && { echo "WARN: $n backends still active after 120 s, continuing"; return 0; }
    echo "  waiting for postgres to go quiet ($n active)..."; sleep 3; waited=$((waited+3))
  done
}

# reset_db: empty table (TRUNCATE gives the disk space back), fresh statistics, checkpoint (no dirty pages at the start).
reset_db() {
  TRUNC_TOUCHED=1
  pgq "TRUNCATE events" >/dev/null
  pgq "SELECT pg_stat_statements_reset()" >/dev/null 2>&1 || echo "WARN: pg_stat_statements_reset failed (extension missing?)" >&2
  pgq "CHECKPOINT" >/dev/null
}
warmup() {          # 400 small requests, 40 at a time, tenant t0: opens (almost) all pool connections
  seq 1 400 | xargs -P 40 -I{} sh -c 'curl -s -o /dev/null -X POST "'"$BASE_URL"'/v1/events" \
    -H "Content-Type: application/json" -H "Authorization: Bearer '"${KEY_PREFIX}"'0" \
    -d "{\"event_id\":\"$(cat /proc/sys/kernel/random/uuid)\",\"type\":\"warmup\",\"occurred_at\":\"$(date -u +%Y-%m-%dT%H:%M:%S.000Z)\",\"schema_version\":1,\"payload\":{\"w\":1}}"'
}

CAP_FILE=""
tag() { printf '\n=== exp=%s testid=%s phase=%s ts=%s cmd: %s\n' "$EXP_ID" "$testid" "$1" "$(date -Is)" "$2" >>"$CAP_FILE"; }

# One psql call with the cumulative numbers I need, separated by '|' (psql -At default).
#   wal_bytes from the WAL position (exact, no stats delay); xact_commit = commits of this database.
stat_vec() {
  pgq "select pg_wal_lsn_diff(pg_current_wal_insert_lsn(),'0/0')::bigint,
              (select wal_fpi from pg_stat_wal), (select wal_records from pg_stat_wal),
              (select xact_commit from pg_stat_database where datname='$PGDB')"
}
PRE=""
snapshot() {   # snapshot <pre|post>: raw vector into the capture file, and into $PRE for pre
  local v; v=$(stat_vec)
  tag "$1" "stat_vec: wal_bytes|wal_fpi|wal_records|xact_commit"
  echo "$v" >>"$CAP_FILE"
  [ "$1" = pre ] && PRE="$v"
  return 0
}
# Sizes of the table parts, after the run (the table was empty before it, so this is the growth of this run):
#   heap = the main table file; toast = its TOAST table (big jsonb values live here); two indexes.
size_vec() {
  pgq "select pg_relation_size('events'),
              coalesce((select pg_relation_size(reltoastrelid) from pg_class where oid='events'::regclass),0),
              pg_relation_size('events_pkey'), pg_relation_size('events_tenant_time_idx')"
}
# ONE cheap capture in the middle of an "over" run: what are the backends waiting for, and the WAL counters.
mid_capture() {
  tag mid "pg_stat_activity by state/wait event, pg_stat_wal (one psql call)"
  pgqt "select state, wait_event_type, wait_event, count(*) from pg_stat_activity where datname='$PGDB' group by 1,2,3 order by 4 desc;
        select * from pg_stat_wal;" >>"$CAP_FILE" 2>&1 || true
}
# Error types of THIS run, from the server counter (the server was restarted for this run, so it starts at 0).
err_types() {
  curl -fsS --max-time 2 "$BASE_URL/metrics" 2>/dev/null | awk '
    /^ep_db_errors_by_type_total\{/ { match($0,/type="[^"]*"/); t=substr($0,RSTART+6,RLENGTH-7); n=$NF; if (n>0) printf "%s%s=%d", (c++?";":""), t, n }'
}

run_k6() {   # run_k6 <rate> <duration> <testid> <phase> <size>
  ( cd "$RUNNER_DIR" && env RESULTS_ROOT="$HERE/results" RESULTS_NAME="$RESULTS_NAME" LABEL="$3" SCRIPT="stage-1/B7/script.js" \
      RATE="$1" DURATION="$2" PHASE="$4" SIZE="$5" TENANTS="$TENANTS" POOL_MAX="$POOL" KEY_PREFIX="$KEY_PREFIX" BASE_URL="$BASE_URL" ROUTE="/healthz" ./runner.sh )
}
record() { printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$testid" "$PHASE" "$SIZE_NOW" "$RATE_NOW" "$1" "$2" "$3" >>"$RUNS_TSV"; }

# Start of every run: fresh server (new pool connections), empty table, checkpoint.
prepare_run() {
  stop_server; wait_db_quiet
  start_server "$POOL" "$SERVER_BODY_LIMIT"
  warmup; wait_db_quiet; reset_db
  sleep 5                                    # let the reset/checkpoint I/O finish before measuring
}

# Add the summary row of this run with the shared analyzer.
summarize() {   # summarize <rows> <extra args...>
  local rows="$1"; shift
  local js; js=$(ls -t "$OUT"/*/"${testid}_1-lap.json" 2>/dev/null | head -1 || true)
  if [ -n "$js" ]; then
    python3 "$ANALYZE" row --json "$js" --testid "$testid" --phase "$PHASE" --rate "$RATE_NOW" --prefix b7 \
      --var "size=$SIZE_NOW" --rows "$rows" "$@" --hook "$HERE/hook.py" --out "$SUMMARY" \
      || echo "WARN: could not add the summary row of $testid (the numbers are still in its capture file); continuing" >&2
    echo "   -> $(python3 "$ANALYZE" table "$SUMMARY" | tail -1 | tr -s ' ')"
  else
    echo "   !! no summary json for $testid (k6 failed before finishing?)"
  fi
}

# ONE measured run. one_run <phase> <size> <rate> <duration> <rep>
one_run() {
  PHASE="$1"; SIZE_NOW="$2"; RATE_NOW="$3"; local dur="$4" rep="$5"
  testid="b7-${PHASE}-${SIZE_NOW}-n${rep}"
  echo; echo "== $PHASE: body=${SIZE_NOW} B rate=$RATE_NOW repeat=$rep ($testid) =="
  CAP_FILE="$CAP/$testid.txt"; : >"$CAP_FILE"
  prepare_run
  local rows_pre; rows_pre=$(pgq "select count(*) from events")
  snapshot pre; local t0; t0=$(date +%s); local status=ok
  if [ "$PHASE" = over ]; then
    run_k6 "$RATE_NOW" "$dur" "$testid" "$PHASE" "$SIZE_NOW" & local kpid=$!
    sleep $(( $(to_seconds "$dur") / 2 + 3 )); mid_capture
    wait "$kpid" || { status=FAILED; echo "!! $testid failed: see its manifest.md"; }
  else
    run_k6 "$RATE_NOW" "$dur" "$testid" "$PHASE" "$SIZE_NOW" || { status=FAILED; echo "!! $testid failed: see its manifest.md"; }
  fi
  sleep 3; wait_db_quiet                      # let requests that were still running finish before counting rows
  local rows_post; rows_post=$(pgq "select count(*) from events"); local rows=$(( rows_post - rows_pre ))
  local et; et=$(err_types || true)
  snapshot post
  local POST SZ; POST=$(stat_vec); SZ=$(size_vec)
  tag post "error types from the server: ${et:-none}; events rows $rows_post (before the run: $rows_pre)"
  tag sizes "size_vec: heap|toast|pkey_index|tenant_time_index (bytes, table was empty at the start): $SZ"
  record "$t0" "$(date +%s)" "$status"
  # per-row numbers from the snapshots
  local p1 p2 p3 p4 q1 q2 q3 q4 h t i1 i2 wal_row fpi_row heap_row toast_row idx_row
  IFS='|' read -r p1 p2 p3 p4 <<<"$PRE"; IFS='|' read -r q1 q2 q3 q4 <<<"$POST"
  IFS='|' read -r h t i1 i2 <<<"$SZ"
  wal_row=$(awk -v a="$((q1-p1))" -v n="$rows" 'BEGIN{ if (n>0) printf "%.0f", a/n; else print "n/a" }')
  fpi_row=$(awk -v a="$((q2-p2))" -v n="$rows" 'BEGIN{ if (n>0) printf "%.2f", a/n; else print "n/a" }')
  heap_row=$(awk -v a="$h" -v n="$rows" 'BEGIN{ if (n>0) printf "%.0f", a/n; else print "n/a" }')
  toast_row=$(awk -v a="$t" -v n="$rows" 'BEGIN{ if (n>0) printf "%.0f", a/n; else print "n/a" }')
  idx_row=$(awk -v a="$((i1+i2))" -v n="$rows" 'BEGIN{ if (n>0) printf "%.0f", a/n; else print "n/a" }')
  summarize "$rows" --extra "wal_B_per_row=$wal_row" --extra "wal_fpi_per_row=$fpi_row" --extra "heap_B_per_row=$heap_row" \
    --extra "toast_B_per_row=$toast_row" --extra "index_B_per_row=$idx_row" --extra "err_types=${et:-none}"
}

# The body-limit probe: default BODY_LIMIT (64kb), one request of 60000 B and one of 100000 B, plain curl. Not k6.
limit_probe() {
  testid="b7-probe-413"; CAP_FILE="$CAP/$testid.txt"; : >"$CAP_FILE"
  echo; echo "== probe: default BODY_LIMIT (64kb), bodies of 60000 B and 100000 B =="
  stop_server; start_server "$POOL" "64kb"
  local n code
  for n in 60000 100000; do
    python3 - "$n" "$OUT/probe-body-$n.json" <<'PY'
import sys, json, uuid, random
n, path = int(sys.argv[1]), sys.argv[2]
body = {"event_id": str(uuid.uuid4()), "type": "probe", "occurred_at": "2026-01-01T00:00:00.000Z", "schema_version": 1, "payload": {"data": ""}}
empty = len(json.dumps(body, separators=(",", ":")))
body["payload"]["data"] = "".join(random.choice("0123456789abcdef") for _ in range(n - empty))
open(path, "w").write(json.dumps(body, separators=(",", ":")))
PY
    code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$BASE_URL/v1/events" -H "Content-Type: application/json" \
      -H "Authorization: Bearer ${KEY_PREFIX}0" --data-binary "@$OUT/probe-body-$n.json")
    tag probe "curl POST /v1/events, body $(wc -c <"$OUT/probe-body-$n.json") B, BODY_LIMIT=64kb"
    echo "http status: $code" >>"$CAP_FILE"
    echo "   body $n B -> HTTP $code"
    rm -f "$OUT/probe-body-$n.json"
  done
  stop_server
}

# ---- 7. Main ----------------------------------------------------------------------
preflight
mkdir -p "$CAP"
printf 'testid\tphase\tsize_B\trate\tstart_epoch\tend_epoch\tstatus\n' >"$RUNS_TSV"

{   # environment snapshot (read only)
  echo "experiment: $EXP_ID   date: $(date -Is)"
  echo "git: $(git -C "$REPO" rev-parse --short HEAD 2>/dev/null || echo n/a), $(git -C "$REPO" status --porcelain 2>/dev/null | wc -l) dirty files"
  echo "kernel: $(uname -r)   node: $(node -v)   k6: $(k6 version | head -1)   cpus: $(nproc)"
  echo "pinning: server=$SERVER_CPUS k6=$K6_CPUS; container cpusets (from compose):"
  for c in "$PG_CONTAINER" ep-prometheus ep-grafana ep-node-exporter ep-postgres-exporter; do
    docker inspect -f '  {{.Name}} cpuset={{.HostConfig.CpusetCpus}} nanocpus={{.HostConfig.NanoCpus}} restart={{.HostConfig.RestartPolicy.Name}}' "$c" 2>/dev/null || true
  done
  echo "settings: POOL=$POOL TENANTS=$TENANTS SIZES=[$SIZES] PHASES=[$PHASES] REPEATS=$REPEATS DURATION=$DURATION SERVER_BODY_LIMIT=$SERVER_BODY_LIMIT PROBE_413=$PROBE_413"
  echo "rates: at=[$AT_RATES] over=[$OVER_RATES] (one per size, same order as SIZES)"
  echo "--- postgres ---"
  docker inspect -f 'image={{.Config.Image}} nanocpus={{.HostConfig.NanoCpus}} memory={{.HostConfig.Memory}} cpuset={{.HostConfig.CpusetCpus}}' "$PG_CONTAINER"
  pgq "select version()" || true
  for s in max_connections shared_buffers synchronous_commit fsync wal_sync_method wal_compression default_toast_compression toast_tuple_target full_page_writes max_wal_size checkpoint_timeout wal_buffers block_size; do echo "$s = $(pgq "show $s" 2>/dev/null)"; done
  echo "toast_tuple_target of events: $(pgq "select coalesce(reloptions::text,'default (about 2 kB)') from pg_class where oid='events'::regclass")"
  echo "--- cpu model ---"; lscpu | grep -E 'Model name|Thread|Core|Socket' || true
  echo "--- disk under the postgres data volume ---"; docker inspect -f '{{range .Mounts}}{{.Source}} {{end}}' "$PG_CONTAINER" || true
  df -h "$REPO" | tail -1
} >"$OUT/env.txt"

# Postgres container CPU/mem/disk -> Prometheus (needs cgroup v2; if it cannot start, those panels stay empty)
PG_CONTAINER="$PG_CONTAINER" bash "$REPO/infra/scripts/pg-container-exporter.sh" 2>"$OUT/pg-exporter.log" &
PGC_PID=$!; sleep 2
kill -0 "$PGC_PID" 2>/dev/null || { echo "WARN: pg-container-exporter did not start ($(tail -1 "$OUT/pg-exporter.log")); Postgres CPU/memory panels will be empty" >&2; PGC_PID=""; }
# WAL volume -> Prometheus
PG_CONTAINER="$PG_CONTAINER" PGUSER="$PGUSER" PGDB="$PGDB" bash "$REPO/infra/scripts/pg-wal-exporter.sh" 2>"$OUT/pg-wal-exporter.log" &
WAL_PID=$!; sleep 2
kill -0 "$WAL_PID" 2>/dev/null || { echo "WARN: pg-wal-exporter did not start ($(tail -1 "$OUT/pg-wal-exporter.log")); the WAL panels will be empty (summary.tsv still has WAL bytes per row)" >&2; WAL_PID=""; }

# measured runs: repeat is the OUTER loop; odd repeats small to big, even repeats big to small; "at" before "over"
for rep in $(seq 1 "$REPEATS"); do
  for sz in $(sizes_for_rep "$rep"); do
    for ph in $PHASES; do
      one_run "$ph" "$sz" "$(rate_for "$sz" "$ph")" "$DURATION" "$rep"
    done
  done
done
# the body-limit probe: separate, never a headline number, after everything else
[ "$PROBE_413" = 1 ] && limit_probe

echo; [ -f "$SUMMARY" ] && python3 "$ANALYZE" table "$SUMMARY"
echo; echo "Done. Zoom Grafana to the times in $RUNS_TSV (epoch seconds). Summary: $SUMMARY  captures: $CAP  env: $OUT/env.txt"
