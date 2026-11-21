#!/usr/bin/env bash
# B6: Durability knob (synchronous_commit on vs off)
# ONE command runs the whole experiment:
#     ./run.sh
#
# Idea: with synchronous_commit=on (the default) a COMMIT does not return until the WAL record is flushed to disk
# (fsync). B1 and B3 showed that almost all of the 5 ms a request holds its pool connection is that wait. With
# synchronous_commit=off the COMMIT returns at once and a background process (the WAL writer, every 200 ms) flushes the
# WAL later. That should raise throughput, but the server already said "202" for commits that are not on disk yet, so a
# hard crash can lose them. I measure both sides: the gain (rps, p99) and the loss (acknowledged rows missing after a kill).
#
# Two kinds of runs (the same server setup as B3 to B5: pool 25, 100 tenants, server restarted before every run):
#   measured  sync {on, off} x phase {at, over}, REPEATS times. Repeat is the outer loop; odd repeats run on then off,
#             even repeats off then on, so drift hits both modes equally.
#               at    4000 req/s  just under the capacity of "on" (about 4300): compare latency at equal load
#               over  8000 req/s  above both capacities: max accepted rps
#   probes    "crash" probes, SEPARATE runs, never headline numbers, run AFTER all measured runs:
#               TRUNCATE events, k6 at 4000 req/s for 30 s, and CRASH_AT seconds in: docker kill -s KILL the Postgres
#               container, stop the server, wait for k6 to end, start Postgres again, wait for crash recovery,
#               count rows. lost = 202s k6 saw minus rows found.
#             They run last because the kill also breaks the WAL/CPU exporters of this script (they read the container).
#
# How synchronous_commit is set:  ALTER ROLE eventpulse SET synchronous_commit = <on|off>   (the role of the server's
# connections). The server is restarted after the ALTER, so every pool connection is new and picks it up.
# An EXIT trap does RESET again and makes sure the Postgres container is running.
#
# What a crash probe CAN and CANNOT show: docker kill = SIGKILL of the Postgres processes. Everything in Postgres'
# shared memory (WAL buffers) is lost, but data Postgres already handed to the kernel (write() into the page cache)
# survives, because the kernel keeps running. A power cut or kernel crash would also lose that part. So I only see the
# WAL-buffer part of the loss, a lower bound of what a real power cut would do. I cannot test a power cut here.
#
# CPU pinning: NOT done here. Containers come from docker compose (cpusets are in the compose file).
#   server 0,6 (taskset below)   postgres 1,7,2,8   k6 3,9,4,10   monitoring + toxiproxy 5,11
#
# This script changes (all restored on exit): the role setting synchronous_commit, the rows of events (TRUNCATE), and it
# kills and starts the ep-postgres container. If it is killed hard (kill -9) the trap does not run; restore by hand:
#     ./run.sh RESTORE
#
# Usage:  DRY_RUN=1 ./run.sh                                       (print the plan, run nothing)
#         ./run.sh RESTORE                                         (start Postgres if needed, RESET the role setting)
#         MEASURED=0 MODES="off" CRASH_REPS=1 RESULTS_NAME=b6-trial-crash ./run.sh   (crash trial: one probe only, NOT a result)
#         MODES="off" PHASES="at" REPEATS=1 DURATION=30s PROBES=0 RESULTS_NAME=b6-trial ./run.sh   (trial, NOT a result)
#         AT_RATE=3800 OVER_RATE=7000 ./run.sh                     (override the offered rates)
set -euo pipefail

EXP_ID="B6"
export PYTHONDONTWRITEBYTECODE=1                 # analyze.py loads hook.py; do not leave a __pycache__ in the repo

# ---- 1. Settings (all can be overridden from the environment) ---------------
MODES="${MODES:-on off}"                         # synchronous_commit values
PHASES="${PHASES:-at over}"
TENANTS="${TENANTS:-100}"
POOL="${POOL:-25}"                               # PG_POOL_MAX, same as B3 to B5
AT_RATE="${AT_RATE:-4000}"                       # just under the capacity of "on" (B3: about 4300 req/s)
OVER_RATE="${OVER_RATE:-8000}"                   # above the capacity of both modes
REPEATS="${REPEATS:-3}"
DURATION="${DURATION:-45s}"
MEASURED="${MEASURED:-1}"                        # 0 = skip the measured runs (only crash probes, for a crash trial)
PROBES="${PROBES:-1}"                            # 1 = also run the crash probes
CRASH_REPS="${CRASH_REPS:-3}"                    # crash probes per mode
CRASH_RATE="${CRASH_RATE:-4000}"
CRASH_DURATION="${CRASH_DURATION:-30s}"
CRASH_AT="${CRASH_AT:-20}"                       # seconds after k6 is started when I kill Postgres
KEY_PREFIX="${KEY_PREFIX:-epk_b6_}"              # api key i = KEY_PREFIX + i, tenant i = t<i>
RESULTS_NAME="${RESULTS_NAME:-b6}"
BASE_URL="${BASE_URL:-http://localhost:8000}"
PROM_URL="${PROM_URL:-http://localhost:9090}"
MANAGE_SERVER="${MANAGE_SERVER:-1}"
SERVER_CPUS="${SERVER_CPUS:-0,6}"
export K6_CPUS="${K6_CPUS:-3,9,4,10}"            # runner.sh reads this (taskset for k6)
PG_CONTAINER="${PG_CONTAINER:-}"                 # empty = auto-detect
PGUSER="${PGUSER:-eventpulse}"; PGDB="${PGDB:-eventpulse}"
DRY_RUN="${DRY_RUN:-0}"
OVERHEAD_S=45                                    # restart + warm-up + truncate + checkpoint + settle per run (rough)
RECOVERY_S=40                                    # kill + container restart + crash recovery per probe (rough, not known yet)

# ---- 2. Paths ----------------------------------------------------------------
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"      # loadtests/k6/stage-1/B6
STAGE_DIR="$(dirname "$HERE")"                             # loadtests/k6/stage-1
RUNNER_DIR="$(dirname "$STAGE_DIR")"                       # loadtests/k6  (runner.sh lives here)
REPO="$(cd "$RUNNER_DIR/../.." && pwd)"
API_DIR="$REPO/services/ingest-api"
ANALYZE="$RUNNER_DIR/analyze.py"                           # shared analyzer
OUT="$HERE/results/$RESULTS_NAME"
CAP="$OUT/captures"; RUNS_TSV="$OUT/runs.tsv"; SUMMARY="$OUT/summary.tsv"

die() { echo "ERROR: $*" >&2; exit 1; }
to_seconds() { case "$1" in *h) echo $(( ${1%h}*3600 ));; *m) echo $(( ${1%m}*60 ));; *s) echo "${1%s}";; *) echo "$1";; esac; }

# modes for repeat number $1: odd = as listed, even = reversed
modes_for_rep() { if [ $(( $1 % 2 )) -eq 1 ]; then echo "$MODES"; else echo "$MODES" | tr ' ' '\n' | tac | tr '\n' ' '; fi; }

# ---- 3. Plan / dry run ---------------------------------------------------------
if [ "${1:-}" != "RESTORE" ]; then
  n_modes=$(wc -w <<<"$MODES"); n_phases=$(wc -w <<<"$PHASES")
  dur_s=$(to_seconds "$DURATION"); cdur_s=$(to_seconds "$CRASH_DURATION")
  n_meas=$(( MEASURED==1 ? n_modes*n_phases*REPEATS : 0 ))
  total_s=$(( n_meas*(dur_s+OVERHEAD_S) ))
  n_probes=0
  if [ "$PROBES" = 1 ]; then n_probes=$(( n_modes*CRASH_REPS )); total_s=$(( total_s + n_probes*(cdur_s+OVERHEAD_S+RECOVERY_S) )); fi
  echo "== $EXP_ID: $n_meas measured runs + $n_probes crash probes, results in $OUT =="
  echo "== pool $POOL | tenants $TENANTS | modes $MODES | phases $PHASES | duration $DURATION | repeats $REPEATS | expected total about $(( total_s/60 )) min =="
  if [ "$DRY_RUN" = "1" ]; then
    for rep in $(seq 1 "$REPEATS"); do echo "  repeat $rep mode order: $(modes_for_rep "$rep")"; done
    echo "  offered rates: at=$AT_RATE  over=$OVER_RATE req/s (the same for every mode)"
    [ "$PROBES" = 1 ] && echo "  crash probes (after the measured runs): $CRASH_REPS per mode, $CRASH_RATE req/s for $CRASH_DURATION, docker kill -s KILL $CRASH_AT s after k6 starts; order per repeat: $(for r in $(seq 1 "$CRASH_REPS"); do echo -n "[$(modes_for_rep "$r")] "; done)"
    echo "  CPU plan (from compose, not changed here): server=$SERVER_CPUS k6=$K6_CPUS"
    echo "  changes made (all restored on exit): ALTER ROLE $PGUSER SET synchronous_commit; TRUNCATE events; docker kill + start of the postgres container"
    exit 0
  fi
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

# Wait until the database answers a query (after a start or a crash recovery). Prints the seconds it took.
wait_db_up() {   # wait_db_up <max seconds>
  local t0 waited=0; t0=$(date +%s)
  until docker exec "$PG_CONTAINER" psql -U "$PGUSER" -d "$PGDB" -X -Atc "select 1" >/dev/null 2>&1; do
    [ "$waited" -ge "$1" ] && return 1
    sleep 1; waited=$(( $(date +%s) - t0 ))
  done
  echo $(( $(date +%s) - t0 ))
}

# Make sure the container is running (docker's restart policy may already have started it after a kill).
ensure_pg_running() {
  [ "$(docker inspect -f '{{.State.Running}}' "$PG_CONTAINER" 2>/dev/null)" = true ] || docker start "$PG_CONTAINER" >/dev/null
}

# Put the database back the way it was: no role-level synchronous_commit (the server default "on" applies again).
SYNC_TOUCHED=0
restore_sync() {
  ensure_pg_running 2>/dev/null || true
  wait_db_up 120 >/dev/null || true
  pgq "ALTER ROLE $PGUSER RESET synchronous_commit" >/dev/null 2>&1 && echo "synchronous_commit restored: role $PGUSER has no override again" \
    || echo "WARN: could not reset the role setting; run  ./run.sh RESTORE" >&2
}

if [ "${1:-}" = "RESTORE" ]; then
  find_pg_container; restore_sync; exit 0
fi

# set_sync <on|off>: new sessions of the role get this value. Check from a fresh session that it took effect.
set_sync() {
  SYNC_TOUCHED=1
  pgq "ALTER ROLE $PGUSER SET synchronous_commit = $1" >/dev/null
  local got; got=$(pgq "show synchronous_commit")
  [ "$got" = "$1" ] || die "asked for synchronous_commit=$1 but a new session sees '$got'"
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
  [ "$(pgq "show synchronous_commit")" = on ] || die "synchronous_commit is not 'on' at the start (a leftover from an earlier run? run ./run.sh RESTORE)"
  [ -f "$API_DIR/dist/main.js" ] || die "$API_DIR/dist/main.js missing: run 'pnpm build' in services/ingest-api"
  [ -f "$API_DIR/.env" ] || die "$API_DIR/.env missing"
  if [ "$MANAGE_SERVER" = 1 ] && curl -fsS --max-time 2 "$BASE_URL/healthz" >/dev/null 2>&1; then
    die "something already answers on $BASE_URL; stop it (run.sh starts its own ingest-api per run) or set MANAGE_SERVER=0"
  fi
  echo "postgres container: $PG_CONTAINER | server connects directly to Postgres (no proxy)"
}

# ---- 5. Server lifecycle (ingest-api) ---------------------------------------------
SERVER_PID=""
# start_server <pool>. Env vars beat --env-file, so API_KEYS/COUNTER_MODE/PG_POOL_MAX here win over .env.
start_server() {
  [ "$MANAGE_SERVER" = 1 ] || return 0
  stop_server
  local keys; keys=$(seq 0 $(( TENANTS - 1 )) | awk -v p="$KEY_PREFIX" '{printf "%s%s%d:t%d", (NR>1?",":""), p, $1, $1}')
  ( cd "$API_DIR" && exec env API_KEYS="$keys" COUNTER_MODE=off PG_POOL_MAX="$1" taskset -c "$SERVER_CPUS" node --env-file=.env dist/main.js ) >"$OUT/server.out" 2>&1 &
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

PGC_PID=""; WAL_PID=""
cleanup() {
  stop_server
  [ -n "$PGC_PID" ] && kill "$PGC_PID" 2>/dev/null || true
  # pg-wal-exporter stops its own children (docker exec, read loop) when it gets TERM; I give it a moment to do that
  if [ -n "$WAL_PID" ]; then kill "$WAL_PID" 2>/dev/null || true; sleep 1; fi
  [ "$SYNC_TOUCHED" = 1 ] && restore_sync
  return 0
}
trap cleanup EXIT
trap 'exit 130' INT TERM        # so Ctrl+C also runs the EXIT trap above (and resets the role setting)

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

# reset_db: empty table, fresh statistics, checkpoint (so the run starts with no dirty pages and a short WAL to replay).
reset_db() {
  pgq "TRUNCATE events" >/dev/null
  pgq "SELECT pg_stat_statements_reset()" >/dev/null 2>&1 || echo "WARN: pg_stat_statements_reset failed (extension missing?)" >&2
  pgq "CHECKPOINT" >/dev/null
}
warmup() {          # 400 requests, 40 at a time, tenant t0: opens (almost) all pool connections
  seq 1 400 | xargs -P 40 -I{} sh -c 'curl -s -o /dev/null -X POST "'"$BASE_URL"'/v1/events" \
    -H "Content-Type: application/json" -H "Authorization: Bearer '"${KEY_PREFIX}"'0" \
    -d "{\"event_id\":\"$(cat /proc/sys/kernel/random/uuid)\",\"type\":\"warmup\",\"occurred_at\":\"$(date -u +%Y-%m-%dT%H:%M:%S.000Z)\",\"schema_version\":1,\"payload\":{\"w\":1}}"'
}

CAP_FILE=""
tag() { printf '\n=== exp=%s testid=%s phase=%s ts=%s cmd: %s\n' "$EXP_ID" "$testid" "$1" "$(date -Is)" "$2" >>"$CAP_FILE"; }

# One psql call with the cumulative numbers I need, separated by '|' (psql -At default).
#   wal_bytes from the WAL position (exact, no stats delay); wal_sync = how many times Postgres called fsync/fdatasync on
#   the WAL (pg_stat_wal; counted even without track_wal_io_timing); xact_commit = commits of this database.
stat_vec() {
  pgq "select pg_wal_lsn_diff(pg_current_wal_insert_lsn(),'0/0')::bigint,
              (select wal_sync from pg_stat_wal), (select wal_write from pg_stat_wal),
              (select xact_commit from pg_stat_database where datname='$PGDB')"
}
PRE=""
snapshot() {   # snapshot <pre|post>: raw vector into the capture file, and into $PRE for pre
  local v; v=$(stat_vec)
  tag "$1" "stat_vec: wal_bytes|wal_sync|wal_write|xact_commit"
  echo "$v" >>"$CAP_FILE"
  [ "$1" = pre ] && PRE="$v"
  return 0
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

run_k6() {   # run_k6 <rate> <duration> <testid> <phase> <sync>
  ( cd "$RUNNER_DIR" && env RESULTS_ROOT="$HERE/results" RESULTS_NAME="$RESULTS_NAME" LABEL="$3" SCRIPT="stage-1/B6/script.js" \
      RATE="$1" DURATION="$2" PHASE="$4" SYNC="$5" TENANTS="$TENANTS" POOL_MAX="$POOL" KEY_PREFIX="$KEY_PREFIX" BASE_URL="$BASE_URL" ROUTE="/healthz" ./runner.sh )
}
record() { printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$testid" "$PHASE" "$SYNC" "$RATE_NOW" "$1" "$2" "$3" >>"$RUNS_TSV"; }

# Start of every run: fresh server with the right synchronous_commit, empty table, checkpoint.
prepare_run() {   # prepare_run <sync>
  stop_server; wait_db_quiet
  set_sync "$1"
  start_server "$POOL"
  warmup; wait_db_quiet; reset_db
  sleep 5                                    # let the reset/checkpoint I/O finish before measuring
}

# Add the summary row of this run with the shared analyzer. Needs: testid PHASE SYNC RATE_NOW rows and extras in $EXTRA_ARGS.
summarize() {   # summarize <rows> <extra args...>
  local rows="$1"; shift
  local js; js=$(ls -t "$OUT"/*/"${testid}_1-lap.json" 2>/dev/null | head -1 || true)
  if [ -n "$js" ]; then
    python3 "$ANALYZE" row --json "$js" --testid "$testid" --phase "$PHASE" --rate "$RATE_NOW" --prefix b6 \
      --var "sync=$SYNC" --rows "$rows" "$@" --hook "$HERE/hook.py" --out "${SUMMARY_OUT:-$SUMMARY}" \
      || echo "WARN: could not add the summary row of $testid (the numbers are still in its capture file); continuing" >&2
    echo "   -> $(python3 "$ANALYZE" table "${SUMMARY_OUT:-$SUMMARY}" | tail -1 | tr -s ' ')"
  else
    echo "   !! no summary json for $testid (k6 failed before finishing?)"
  fi
}

# ONE measured run. one_run <phase> <sync> <rate> <duration> <rep>
one_run() {
  PHASE="$1"; SYNC="$2"; RATE_NOW="$3"; local dur="$4" rep="$5"
  testid="b6-${PHASE}-${SYNC}-n${rep}"
  echo; echo "== $PHASE: synchronous_commit=$SYNC rate=$RATE_NOW repeat=$rep ($testid) =="
  CAP_FILE="$CAP/$testid.txt"; : >"$CAP_FILE"
  prepare_run "$SYNC"
  local rows_pre; rows_pre=$(pgq "select count(*) from events")
  snapshot pre; local t0; t0=$(date +%s); local status=ok
  if [ "$PHASE" = over ]; then
    run_k6 "$RATE_NOW" "$dur" "$testid" "$PHASE" "$SYNC" & local kpid=$!
    sleep $(( $(to_seconds "$dur") / 2 + 3 )); mid_capture
    wait "$kpid" || { status=FAILED; echo "!! $testid failed: see its manifest.md"; }
  else
    run_k6 "$RATE_NOW" "$dur" "$testid" "$PHASE" "$SYNC" || { status=FAILED; echo "!! $testid failed: see its manifest.md"; }
  fi
  sleep 3; wait_db_quiet                      # let requests that were still running finish before counting rows
  local rows_post; rows_post=$(pgq "select count(*) from events"); local rows=$(( rows_post - rows_pre ))
  local et; et=$(err_types || true)
  snapshot post
  local POST; POST=$(stat_vec)
  tag post "error types from the server: ${et:-none}; events rows $rows_post (before the run: $rows_pre)"; record "$t0" "$(date +%s)" "$status"
  # per-row numbers from the two snapshots (post - pre)
  local p1 p2 p3 p4 q1 q2 q3 q4 wal_per_row sync_per_1k commit_per_row
  IFS='|' read -r p1 p2 p3 p4 <<<"$PRE"; IFS='|' read -r q1 q2 q3 q4 <<<"$POST"
  wal_per_row=$(awk -v a="$((q1-p1))" -v n="$rows" 'BEGIN{ if (n>0) printf "%.0f", a/n; else print "n/a" }')
  sync_per_1k=$(awk -v a="$((q2-p2))" -v n="$rows" 'BEGIN{ if (n>0) printf "%.1f", 1000*a/n; else print "n/a" }')
  commit_per_row=$(awk -v a="$((q4-p4))" -v n="$rows" 'BEGIN{ if (n>0) printf "%.2f", a/n; else print "n/a" }')
  summarize "$rows" --extra "wal_B_per_row=$wal_per_row" --extra "wal_sync_per_1k_rows=$sync_per_1k" --extra "commits_per_row=$commit_per_row" --extra "err_types=${et:-none}"
}

# ONE crash probe. crash_probe <sync> <rep>
# Order of events: k6 starts in the background; after CRASH_AT s I kill Postgres (SIGKILL, no clean shutdown, no
# final WAL flush), stop the server at once (so no request can be answered "202" by a restarted database), let k6 finish,
# start Postgres again, wait for crash recovery, count rows.
crash_probe() {
  PHASE=probe_crash; SYNC="$1"; RATE_NOW="$CRASH_RATE"; local rep="$2"
  testid="b6-crash-${SYNC}-n${rep}"
  echo; echo "== crash probe: synchronous_commit=$SYNC rate=$RATE_NOW repeat=$rep ($testid) =="
  CAP_FILE="$CAP/$testid.txt"; : >"$CAP_FILE"
  prepare_run "$SYNC"
  local rows_pre; rows_pre=$(pgq "select count(*) from events"); local t0 status=ok; t0=$(date +%s)
  run_k6 "$RATE_NOW" "$CRASH_DURATION" "$testid" "$PHASE" "$SYNC" & local kpid=$!
  sleep "$CRASH_AT"
  local kill_epoch kill_iso; kill_epoch=$(date +%s.%N); kill_iso=$(date -u +%Y-%m-%dT%H:%M:%S)
  docker kill -s KILL "$PG_CONTAINER" >/dev/null                     # SIGKILL: the container dies like a crashed machine would (minus the OS)
  stop_server                                                         # no more answers from here on
  wait "$kpid" || true                                                # k6 finishes by itself; its failed requests after the kill are expected
  sleep 2; ensure_pg_running                                          # restart policy may already have done it
  local up_s; up_s=$(wait_db_up 180) || die "Postgres did not come back within 180 s after the kill (see: docker logs $PG_CONTAINER)"
  local rows_post; rows_post=$(pgq "select count(*) from events"); local rows=$(( rows_post - rows_pre ))
  local js acked; js=$(ls -t "$OUT"/*/"${testid}_1-lap.json" 2>/dev/null | head -1 || true)
  acked=$(jq -r '(.metrics.b6_resp_202.count // .metrics.b6_resp_202.values.count // "NA")' "$js" 2>/dev/null || echo NA)
  tag crash "kill at epoch $kill_epoch (about $CRASH_AT s after k6 start); first query answered $up_s s after the restart was requested; acked(202)=$acked rows_found=$rows"
  docker logs --since "${kill_iso}Z" "$PG_CONTAINER" 2>&1 | grep -E "not properly shut down|redo|automatic recovery|ready to accept|checkpoint" | head -12 >>"$CAP_FILE" || true
  record "$t0" "$(date +%s)" "$status"
  if [ "$acked" != NA ]; then echo "   acknowledged (202): $acked   rows found after recovery: $rows   lost: $(( acked - rows ))   (negative = rows whose 202 never reached k6)"; fi
  # analyze.py refuses to mix different extra columns in one summary, so the probes have their own file.
  SUMMARY_OUT="$OUT/summary-crash.tsv" summarize "$rows" --extra "kill_epoch=${kill_epoch%.*}" --extra "recovery_wait_s=$up_s"
}

# ---- 7. Main ----------------------------------------------------------------------
preflight
mkdir -p "$CAP"
printf 'testid\tphase\tsync\trate\tstart_epoch\tend_epoch\tstatus\n' >"$RUNS_TSV"

{   # environment snapshot (read only)
  echo "experiment: $EXP_ID   date: $(date -Is)"
  echo "git: $(git -C "$REPO" rev-parse --short HEAD 2>/dev/null || echo n/a), $(git -C "$REPO" status --porcelain 2>/dev/null | wc -l) dirty files"
  echo "kernel: $(uname -r)   node: $(node -v)   k6: $(k6 version | head -1)   cpus: $(nproc)"
  echo "pinning: server=$SERVER_CPUS k6=$K6_CPUS; container cpusets (from compose):"
  for c in "$PG_CONTAINER" ep-prometheus ep-grafana ep-node-exporter ep-postgres-exporter; do
    docker inspect -f '  {{.Name}} cpuset={{.HostConfig.CpusetCpus}} nanocpus={{.HostConfig.NanoCpus}} restart={{.HostConfig.RestartPolicy.Name}}' "$c" 2>/dev/null || true
  done
  echo "settings: POOL=$POOL TENANTS=$TENANTS MODES=[$MODES] PHASES=[$PHASES] REPEATS=$REPEATS DURATION=$DURATION PROBES=$PROBES CRASH_REPS=$CRASH_REPS CRASH_RATE=$CRASH_RATE CRASH_DURATION=$CRASH_DURATION CRASH_AT=$CRASH_AT"
  echo "rates: at=$AT_RATE over=$OVER_RATE (same for every mode)"
  echo "--- postgres ---"
  docker inspect -f 'image={{.Config.Image}} nanocpus={{.HostConfig.NanoCpus}} memory={{.HostConfig.Memory}} cpuset={{.HostConfig.CpusetCpus}}' "$PG_CONTAINER"
  pgq "select version()" || true
  for s in max_connections shared_buffers synchronous_commit fsync wal_sync_method wal_writer_delay wal_writer_flush_after commit_delay wal_level full_page_writes max_wal_size checkpoint_timeout wal_buffers; do echo "$s = $(pgq "show $s" 2>/dev/null)"; done
  echo "--- cpu model ---"; lscpu | grep -E 'Model name|Thread|Core|Socket' || true
  echo "--- disk under the postgres data volume ---"; docker inspect -f '{{range .Mounts}}{{.Source}} {{end}}' "$PG_CONTAINER" || true
} >"$OUT/env.txt"

# Postgres container CPU/mem/disk -> Prometheus (needs cgroup v2; if it cannot start, those panels stay empty)
PG_CONTAINER="$PG_CONTAINER" bash "$REPO/infra/scripts/pg-container-exporter.sh" 2>"$OUT/pg-exporter.log" &
PGC_PID=$!; sleep 2
kill -0 "$PGC_PID" 2>/dev/null || { echo "WARN: pg-container-exporter did not start ($(tail -1 "$OUT/pg-exporter.log")); Postgres CPU/memory panels will be empty" >&2; PGC_PID=""; }
# WAL volume -> Prometheus (the postgres-exporter container has no WAL volume metric on Postgres 15)
PG_CONTAINER="$PG_CONTAINER" PGUSER="$PGUSER" PGDB="$PGDB" bash "$REPO/infra/scripts/pg-wal-exporter.sh" 2>"$OUT/pg-wal-exporter.log" &
WAL_PID=$!; sleep 2
kill -0 "$WAL_PID" 2>/dev/null || { echo "WARN: pg-wal-exporter did not start ($(tail -1 "$OUT/pg-wal-exporter.log")); the WAL panels will be empty (summary.tsv still has WAL bytes per row)" >&2; WAL_PID=""; }

# measured runs: repeat is the OUTER loop; odd repeats forward, even repeats reversed; "at" before "over"
for rep in $(seq 1 "$REPEATS"); do
  [ "$MEASURED" = 1 ] || break
  for m in $(modes_for_rep "$rep"); do
    for ph in $PHASES; do
      if [ "$ph" = at ]; then r="$AT_RATE"; else r="$OVER_RATE"; fi
      one_run "$ph" "$m" "$r" "$DURATION" "$rep"
    done
  done
done
# crash probes: separate, never headline numbers, after everything else (the kill breaks the container exporters)
if [ "$PROBES" = 1 ]; then
  for rep in $(seq 1 "$CRASH_REPS"); do
    for m in $(modes_for_rep "$rep"); do crash_probe "$m" "$rep"; done
  done
fi

echo; [ -f "$SUMMARY" ] && python3 "$ANALYZE" table "$SUMMARY"
[ -f "$OUT/summary-crash.tsv" ] && { echo; echo "crash probes (lost = acknowledged - rows, in the audit column):"; python3 "$ANALYZE" table "$OUT/summary-crash.tsv"; }
echo; echo "Done. Zoom Grafana to the times in $RUNS_TSV (epoch seconds). Summary: $SUMMARY  captures: $CAP  env: $OUT/env.txt"
