#!/usr/bin/env bash
# B4: Lock contention with a hot row
# ONE command runs the whole experiment:
#     ./run.sh
#
# Idea: with COUNTER_MODE=hot every insert is one transaction  BEGIN; INSERT event; UPDATE tenant_counters
# SET n = n + 1 WHERE tenant_id = $1; COMMIT.  The UPDATE takes a row lock on the tenant's counter row and keeps
# it until COMMIT is flushed to disk. So all writers of ONE tenant go through that lock one after another.
# The thing I change is how many tenants share the load: 1, 10, 100, 1000. With COUNTER_MODE=off (no counter,
# no transaction) the tenant count should not matter, that is my control (levels 1 and 1000 only).
#
# Cells (mode:tenants): hot:1 hot:10 hot:100 hot:1000 off:1 off:1000.  Every cell has TWO measured runs:
#   under  150 req/s  below what one hot tenant can do -> measures the latency cost of the extra round trips
#   over  3000 req/s  far above what one hot tenant can do, still below the no-counter capacity (about 4300 in B3)
#                     -> measures capacity (accepted 202/s), pool queue, lock waits
# The SAME offered rates are used for every cell, so cells can be compared directly.
#
# What the script does (so I can re-read it later):
#   1. preflight    - tools, Prometheus, postgres-exporter, build of ingest-api (with COUNTER_MODE), table tenant_counters
#   2. env snapshot - versions, Postgres settings, cpusets of the containers (read only)
#   3. for every repeat x cell x phase: restart server (own API_KEYS, COUNTER_MODE, pool), seed one counter row per
#      tenant, warm up, check the counter really moves (hot) or not (off), truncate events, k6 via runner.sh,
#      snapshots, row audit (events rows vs sum of counters), summary row
#      In the middle of every "over" run ONE cheap capture is taken (pg_stat_activity by wait event + blocked
#      sessions + pg_locks, one psql call). Prometheus has the same numbers every second; the capture is the
#      readable copy for the write-up.
#   4. probe        - SEPARATE run, never a headline number: pool50 = hot:1 with pool 50 instead of 25
#                     (does a bigger pool help?).
#
# Order: repeat is the OUTER loop. Odd repeats go through the cells as listed, even repeats reversed.
# The server connects DIRECTLY to Postgres (:5434 from .env). No Toxiproxy in B4.
#
# CPU pinning: NOT done here. Containers come from docker compose (cpusets are in the compose file).
#   server 0,6 (taskset below)   postgres 1,7,2,8   k6 3,9,4,10   monitoring + toxiproxy 5,11
#
# Usage:  DRY_RUN=1 ./run.sh                                            (print the plan, run nothing)
#         CELLS="hot:1" PHASES="over" REPEATS=1 DURATION=30s PROBES=0 RESULTS_NAME=b4-trial ./run.sh   (trial, NOT a result)
#         OVER_RATE=2000 ./run.sh                                       (override the offered rate)
set -euo pipefail

EXP_ID="B4"
export PYTHONDONTWRITEBYTECODE=1                 # analyze.py loads hook.py; do not leave a __pycache__ in the repo

# ---- 1. Settings (all can be overridden from the environment) ---------------
CELLS="${CELLS:-hot:1 hot:10 hot:100 hot:1000 off:1 off:1000}"   # mode:tenants
PHASES="${PHASES:-under over}"
POOL="${POOL:-25}"                               # PG_POOL_MAX, same as B3
UNDER_RATE="${UNDER_RATE:-150}"                  # below the predicted ~300 req/s of one hot tenant
OVER_RATE="${OVER_RATE:-3000}"                   # far above it, below the ~4300 req/s no-counter capacity of B3
REPEATS="${REPEATS:-3}"
DURATION="${DURATION:-45s}"
PROBES="${PROBES:-1}"                            # 1 = also run the probe run
PROBE_POOL="${PROBE_POOL:-50}"
KEY_PREFIX="${KEY_PREFIX:-epk_b4_}"              # api key i = KEY_PREFIX + i, tenant i = t<i>
RESULTS_NAME="${RESULTS_NAME:-b4}"
BASE_URL="${BASE_URL:-http://localhost:8000}"
PROM_URL="${PROM_URL:-http://localhost:9090}"
MANAGE_SERVER="${MANAGE_SERVER:-1}"
SERVER_CPUS="${SERVER_CPUS:-0,6}"
export K6_CPUS="${K6_CPUS:-3,9,4,10}"            # runner.sh reads this (taskset for k6)
PG_CONTAINER="${PG_CONTAINER:-}"                 # empty = auto-detect
PGUSER="${PGUSER:-eventpulse}"; PGDB="${PGDB:-eventpulse}"
DRY_RUN="${DRY_RUN:-0}"
OVERHEAD_S=45                                    # restart + warm-up + truncate + settle per run (rough)

# ---- 2. Paths ----------------------------------------------------------------
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"      # loadtests/k6/stage-1/B4
STAGE_DIR="$(dirname "$HERE")"                             # loadtests/k6/stage-1
RUNNER_DIR="$(dirname "$STAGE_DIR")"                       # loadtests/k6  (runner.sh lives here)
REPO="$(cd "$RUNNER_DIR/../.." && pwd)"
API_DIR="$REPO/services/ingest-api"
ANALYZE="$RUNNER_DIR/analyze.py"                           # shared analyzer
OUT="$HERE/results/$RESULTS_NAME"
CAP="$OUT/captures"; RUNS_TSV="$OUT/runs.tsv"; SUMMARY="$OUT/summary.tsv"

die() { echo "ERROR: $*" >&2; exit 1; }
to_seconds() { case "$1" in *h) echo $(( ${1%h}*3600 ));; *m) echo $(( ${1%m}*60 ));; *s) echo "${1%s}";; *) echo "$1";; esac; }

# cells for repeat number $1: odd = as listed, even = reversed
cells_for_rep() { if [ $(( $1 % 2 )) -eq 1 ]; then echo "$CELLS"; else echo "$CELLS" | tr ' ' '\n' | tac | tr '\n' ' '; fi; }

# ---- 3. Plan / dry run ---------------------------------------------------------
n_cells=$(wc -w <<<"$CELLS"); n_phases=$(wc -w <<<"$PHASES")
dur_s=$(to_seconds "$DURATION")
total_s=$(( n_cells*n_phases*REPEATS*(dur_s+OVERHEAD_S) ))
[ "$PROBES" = 1 ] && total_s=$(( total_s + dur_s + OVERHEAD_S ))
echo "== $EXP_ID: $((n_cells*n_phases*REPEATS)) measured runs$([ "$PROBES" = 1 ] && echo " + 1 probe run"), results in $OUT =="
echo "== pool $POOL | cells $CELLS | phases $PHASES | duration $DURATION | repeats $REPEATS | expected total about $(( total_s/60 )) min =="
if [ "$DRY_RUN" = "1" ]; then
  for rep in $(seq 1 "$REPEATS"); do echo "  repeat $rep order: $(cells_for_rep "$rep")"; done
  echo "  offered rates: under=$UNDER_RATE  over=$OVER_RATE req/s (the same for every cell)"
  [ "$PROBES" = 1 ] && echo "  probe: pool$PROBE_POOL = hot:1, pool $PROBE_POOL, $OVER_RATE req/s, $DURATION"
  echo "  CPU plan (from compose, not changed here): server=$SERVER_CPUS k6=$K6_CPUS"
  exit 0
fi

# ---- 4. Helpers ------------------------------------------------------------------
pgq()  { docker exec -i "$PG_CONTAINER" psql -U "$PGUSER" -d "$PGDB" -X -At -c "$1"; }   # one value / rows, no decoration
pgqt() { docker exec -i "$PG_CONTAINER" psql -U "$PGUSER" -d "$PGDB" -X -c "$1"; }       # pretty table for captures

preflight() {
  for c in k6 taskset curl docker python3 tac seq awk; do command -v "$c" >/dev/null || die "'$c' not found in PATH"; done
  [ -x "$RUNNER_DIR/runner.sh" ] || die "$RUNNER_DIR/runner.sh missing or not executable"
  [ -f "$ANALYZE" ] || die "$ANALYZE missing"
  curl -fsS --max-time 3 "$PROM_URL/-/ready" >/dev/null || die "Prometheus not ready at $PROM_URL (docker compose -f infra/compose/docker-compose.yml up -d)"
  curl -fsS --max-time 3 "$PROM_URL/api/v1/query?query=up%7Bjob%3D%22postgres%22%7D" | grep -q '"value":\[[^]]*"1"\]' \
    || die "postgres-exporter (job=postgres) is not 'up' (docker compose -f infra/compose/docker-compose.yml up -d postgres-exporter)"
  curl -fsS --max-time 3 "$PROM_URL/api/v1/query?query=up%7Bjob%3D%22node%22%7D" | grep -q '"value":\[[^]]*"1"\]' \
    || echo "WARN: node_exporter (job=node) is not 'up'; host panels will be empty" >&2
  if [ -z "$PG_CONTAINER" ]; then
    local found; found=$(docker ps --format '{{.Names}} {{.Image}}' | awk '$2 ~ /^postgres:/ {print $1}')
    [ "$(wc -w <<<"$found")" = 1 ] || die "found [$found] postgres containers; set PG_CONTAINER=<name>"
    PG_CONTAINER="$found"
  fi
  pgq "select count(*) from events" >/dev/null || die "cannot query table 'events' in $PG_CONTAINER (migrations? PGUSER/PGDB?)"
  pgq "select count(*) from tenant_counters" >/dev/null 2>&1 \
    || die "table tenant_counters missing: run 'pnpm migrate:up' in services/ingest-api (migration 0003)"
  [ -f "$API_DIR/dist/main.js" ] || die "$API_DIR/dist/main.js missing: run 'pnpm build' in services/ingest-api"
  grep -rq 'COUNTER_MODE' "$API_DIR/dist/" 2>/dev/null || die "dist/ has no COUNTER_MODE: rebuild ingest-api (pnpm build)"
  [ -f "$API_DIR/.env" ] || die "$API_DIR/.env missing"
  DIRECT_URL=$(grep -E '^DATABASE_URL=' "$API_DIR/.env" | head -1 | cut -d= -f2- | tr -d '"')
  [ -n "$DIRECT_URL" ] || die "DATABASE_URL not found in $API_DIR/.env"
  echo "postgres container: $PG_CONTAINER | server connects directly to Postgres (no proxy)"
}

# ---- 5. Server lifecycle (ingest-api) ---------------------------------------------
SERVER_PID=""
# start_server <counter_mode> <tenants> <pool>. Env vars beat --env-file, so API_KEYS/COUNTER_MODE/PG_POOL_MAX here win over .env.
start_server() {
  [ "$MANAGE_SERVER" = 1 ] || return 0
  stop_server
  local keys; keys=$(seq 0 $(( $2 - 1 )) | awk -v p="$KEY_PREFIX" '{printf "%s%s%d:t%d", (NR>1?",":""), p, $1, $1}')
  ( cd "$API_DIR" && exec env API_KEYS="$keys" COUNTER_MODE="$1" PG_POOL_MAX="$3" taskset -c "$SERVER_CPUS" node --env-file=.env dist/main.js ) >"$OUT/server.out" 2>&1 &
  SERVER_PID=$!       # exec keeps the pid: this is the node pid
  for _ in $(seq 1 30); do curl -fsS --max-time 1 "$BASE_URL/healthz" >/dev/null 2>&1 && break; sleep 1; done
  curl -fsS --max-time 2 "$BASE_URL/readyz" >/dev/null || die "ingest-api not ready (see $OUT/server.out)"
  local got; got=$(curl -fsS --max-time 2 "$BASE_URL/debug/stats" | python3 -c 'import sys,json; print(json.load(sys.stdin)["pool"]["max"])' 2>/dev/null || echo "?")
  [ "$got" = "$3" ] || die "server reports pool max '$got', I asked for '$3' (see $OUT/server.out)"
}
stop_server() {
  [ "$MANAGE_SERVER" = 1 ] || return 0
  if [ -n "${SERVER_PID:-}" ] && kill -0 "$SERVER_PID" 2>/dev/null; then kill "$SERVER_PID" 2>/dev/null || true; wait "$SERVER_PID" 2>/dev/null || true; fi
  SERVER_PID=""
}

EXPORTER_PID=""
cleanup() { stop_server; [ -n "$EXPORTER_PID" ] && kill "$EXPORTER_PID" 2>/dev/null || true; }
trap cleanup EXIT

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
seed_counters() {   # seed_counters <tenants>: one row per tenant, n = 0
  pgq "TRUNCATE tenant_counters" >/dev/null
  pgq "INSERT INTO tenant_counters (tenant_id, n) SELECT 't' || g, 0 FROM generate_series(0, $1 - 1) g" >/dev/null
}
reset_db() {        # empty tables so size/index growth of earlier runs cannot contaminate this one; counters back to 0
  pgq "TRUNCATE events" >/dev/null
  seed_counters "$1"
  pgq "SELECT pg_stat_statements_reset()" >/dev/null 2>&1 || echo "WARN: pg_stat_statements_reset failed (extension missing?)" >&2
  pgq "CHECKPOINT" >/dev/null
}
warmup() {          # 400 requests, 40 at a time, always tenant t0 (key 0 exists in every cell): opens (almost) all pool connections
  seq 1 400 | xargs -P 40 -I{} sh -c 'curl -s -o /dev/null -X POST "'"$BASE_URL"'/v1/events" \
    -H "Content-Type: application/json" -H "Authorization: Bearer '"${KEY_PREFIX}"'0" \
    -d "{\"event_id\":\"$(cat /proc/sys/kernel/random/uuid)\",\"type\":\"warmup\",\"occurred_at\":\"$(date -u +%Y-%m-%dT%H:%M:%S.000Z)\",\"schema_version\":1,\"payload\":{\"w\":1}}"'
}
check_counter() {   # check_counter <mode>: after the warm-up the counter of t0 must have moved (hot) or stayed 0 (off)
  local n; n=$(pgq "select coalesce(sum(n),0) from tenant_counters")
  if [ "$1" = hot ]; then [ "$n" -gt 0 ] || die "COUNTER_MODE=hot but tenant_counters did not move after the warm-up (see $OUT/server.out)"
  else [ "$n" -eq 0 ] || die "COUNTER_MODE=off but tenant_counters moved ($n)"; fi
  echo "   self-check: counter sum after warm-up = $n (mode $1)"
}

CAP_FILE=""
tag() { printf '\n=== exp=%s testid=%s phase=%s ts=%s cmd: %s\n' "$EXP_ID" "$testid" "$1" "$(date -Is)" "$2" >>"$CAP_FILE"; }
# Cheap snapshots before and after the run. Counters: subtract pre from post.
light_snapshot() {
  tag "$1" "pg_stat_database + checkpoints + /debug/stats"
  pgqt "select xact_commit, xact_rollback, tup_inserted, tup_updated, deadlocks, temp_files from pg_stat_database where datname='$PGDB'" >>"$CAP_FILE" 2>&1 || true
  pgqt "select n_tup_upd, n_tup_hot_upd, n_dead_tup, last_autovacuum from pg_stat_user_tables where relname='tenant_counters'" >>"$CAP_FILE" 2>&1 || true
  curl -fsS --max-time 2 "$BASE_URL/debug/stats" >>"$CAP_FILE" 2>&1 || true; echo >>"$CAP_FILE"
}
# ONE cheap capture in the middle of an "over" run: who is waiting for what.
mid_capture() {
  tag mid "pg_stat_activity by state/wait event, blocked sessions, pg_locks (one psql call)"
  pgqt "select state, wait_event_type, wait_event, count(*) from pg_stat_activity where datname='$PGDB' group by 1,2,3 order by 4 desc;
        select count(*) as blocked_sessions, count(distinct b) as distinct_blockers from (select pid, unnest(pg_blocking_pids(pid)) b from pg_stat_activity where datname='$PGDB' and cardinality(pg_blocking_pids(pid)) > 0) x;
        select locktype, mode, granted, count(*) from pg_locks where locktype in ('transactionid','tuple','relation') group by 1,2,3 order by 4 desc;" >>"$CAP_FILE" 2>&1 || true
}
# Error types of THIS run, from the server counter (the server was restarted for this run, so it starts at 0).
err_types() {
  curl -fsS --max-time 2 "$BASE_URL/metrics" 2>/dev/null | awk '
    /^ep_db_errors_by_type_total\{/ { match($0,/type="[^"]*"/); t=substr($0,RSTART+6,RLENGTH-7); n=$NF; if (n>0) printf "%s%s=%d", (c++?";":""), t, n }'
}

run_k6() {   # run_k6 <rate> <duration> <testid> <phase> <counter> <tenants> <pool>
  ( cd "$RUNNER_DIR" && env RESULTS_ROOT="$HERE/results" RESULTS_NAME="$RESULTS_NAME" LABEL="$3" SCRIPT="stage-1/B4/script.js" \
      RATE="$1" DURATION="$2" PHASE="$4" COUNTER="$5" TENANTS="$6" POOL_MAX="$7" KEY_PREFIX="$KEY_PREFIX" BASE_URL="$BASE_URL" ROUTE="/healthz" ./runner.sh )
}
record() { printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$testid" "$PHASE" "$MODE" "$TENANTS" "$RATE_NOW" "$1" "$2" "$3" >>"$RUNS_TSV"; }

# ONE run. one_run <phase> <mode hot|off> <tenants> <rate> <duration> <rep> <pool>
one_run() {
  PHASE="$1"; MODE="$2"; TENANTS="$3"; RATE_NOW="$4"; local dur="$5" rep="$6" pool="$7"
  testid="b4-${PHASE}-${MODE}${TENANTS}-n${rep}"
  case "$PHASE" in probe_*) testid="b4-${PHASE//_/-}";; esac
  echo; echo "== $PHASE: counter=$MODE tenants=$TENANTS rate=$RATE_NOW pool=$pool repeat=$rep ($testid) =="
  CAP_FILE="$CAP/$testid.txt"; : >"$CAP_FILE"
  stop_server; wait_db_quiet
  seed_counters "$TENANTS"                    # the warm-up (tenant t0) needs its counter row in hot mode
  start_server "$MODE" "$TENANTS" "$pool"
  warmup; check_counter "$MODE"; wait_db_quiet; reset_db "$TENANTS"
  sleep 5                                    # let the reset/checkpoint I/O finish before measuring
  light_snapshot pre; local t0; t0=$(date +%s); local status=ok
  if [ "$PHASE" = over ] || [ "$PHASE" = probe_pool50 ]; then
    run_k6 "$RATE_NOW" "$dur" "$testid" "$PHASE" "$MODE" "$TENANTS" "$pool" & local kpid=$!
    sleep $(( $(to_seconds "$dur") / 2 + 3 )); mid_capture
    wait "$kpid" || { status=FAILED; echo "!! $testid failed: see its manifest.md"; }
  else
    run_k6 "$RATE_NOW" "$dur" "$testid" "$PHASE" "$MODE" "$TENANTS" "$pool" || { status=FAILED; echo "!! $testid failed: see its manifest.md"; }
  fi
  sleep 3; wait_db_quiet                      # let requests that were still running finish before counting rows
  local rows csum; rows=$(pgq "select count(*) from events"); csum=$(pgq "select coalesce(sum(n),0) from tenant_counters")
  local et; et=$(err_types || true)
  light_snapshot post; tag post "error types from the server: ${et:-none}; events rows $rows; counter sum $csum"; record "$t0" "$(date +%s)" "$status"
  local js; js=$(ls -t "$OUT"/*/"${testid}_1-lap.json" 2>/dev/null | head -1 || true)
  if [ -n "$js" ]; then
    local hot=0; [ "$MODE" = hot ] && hot=1
    python3 "$ANALYZE" row --json "$js" --testid "$testid" --phase "$PHASE" --rate "$RATE_NOW" --prefix b4 \
      --var "tenants=$TENANTS" --var "hot=$hot" --var "pool=$pool" --rows "$rows" \
      --extra "counter_sum=$csum" --extra "err_types=${et:-none}" \
      --hook "$HERE/hook.py" --out "$SUMMARY"
    echo "   -> $(python3 "$ANALYZE" table "$SUMMARY" | tail -1 | tr -s ' ')"
  else
    echo "   !! no summary json for $testid (k6 failed before finishing?)"
  fi
}

# ---- 7. Main ----------------------------------------------------------------------
preflight
mkdir -p "$CAP"
printf 'testid\tphase\tcounter\ttenants\trate\tstart_epoch\tend_epoch\tstatus\n' >"$RUNS_TSV"

{   # environment snapshot (read only)
  echo "experiment: $EXP_ID   date: $(date -Is)"
  echo "git: $(git -C "$REPO" rev-parse --short HEAD 2>/dev/null || echo n/a), $(git -C "$REPO" status --porcelain 2>/dev/null | wc -l) dirty files"
  echo "kernel: $(uname -r)   node: $(node -v)   k6: $(k6 version | head -1)   cpus: $(nproc)"
  echo "pinning: server=$SERVER_CPUS k6=$K6_CPUS; container cpusets (from compose):"
  for c in "$PG_CONTAINER" ep-prometheus ep-grafana ep-node-exporter ep-postgres-exporter; do
    docker inspect -f '  {{.Name}} cpuset={{.HostConfig.CpusetCpus}} nanocpus={{.HostConfig.NanoCpus}}' "$c" 2>/dev/null || true
  done
  echo "settings: POOL=$POOL CELLS=[$CELLS] PHASES=[$PHASES] REPEATS=$REPEATS DURATION=$DURATION PROBES=$PROBES PROBE_POOL=$PROBE_POOL"
  echo "rates: under=$UNDER_RATE over=$OVER_RATE (same for every cell)"
  echo "--- postgres ---"
  docker inspect -f 'image={{.Config.Image}} nanocpus={{.HostConfig.NanoCpus}} memory={{.HostConfig.Memory}} cpuset={{.HostConfig.CpusetCpus}}' "$PG_CONTAINER"
  pgq "select version()" || true
  for s in max_connections shared_buffers synchronous_commit fsync wal_level commit_delay deadlock_timeout; do echo "$s = $(pgq "show $s" 2>/dev/null)"; done
  echo "--- ingest-api pool settings (from services/ingest-api/src/config.ts defaults, not read at run time) ---"
  echo "connectionTimeoutMillis=PG_CONN_TIMEOUT_MS (default 2000)  statement_timeout=PG_STATEMENT_TIMEOUT_MS (default 5000)"
  grep -E '^(PG_CONN_TIMEOUT_MS|PG_STATEMENT_TIMEOUT_MS)=' "$API_DIR/.env" 2>/dev/null || echo "(no PG_*_TIMEOUT overrides in .env)"
  echo "--- cpu model ---"; lscpu | grep -E 'Model name|Thread|Core|Socket' || true
} >"$OUT/env.txt"

# Postgres container CPU/mem/disk -> Prometheus (needs cgroup v2; if it cannot start, those panels stay empty)
PG_CONTAINER="$PG_CONTAINER" bash "$REPO/infra/scripts/pg-container-exporter.sh" 2>"$OUT/pg-exporter.log" &
EXPORTER_PID=$!; sleep 2
kill -0 "$EXPORTER_PID" 2>/dev/null || { echo "WARN: pg-container-exporter did not start ($(tail -1 "$OUT/pg-exporter.log")); Postgres CPU/memory panels will be empty" >&2; EXPORTER_PID=""; }

# measured runs: repeat is the OUTER loop; odd repeats forward, even repeats reversed; under before over
for rep in $(seq 1 "$REPEATS"); do
  for cell in $(cells_for_rep "$rep"); do
    for ph in $PHASES; do
      if [ "$ph" = under ]; then r="$UNDER_RATE"; else r="$OVER_RATE"; fi
      one_run "$ph" "${cell%%:*}" "${cell#*:}" "$r" "$DURATION" "$rep" "$POOL"
    done
  done
done
# probe run: separate
if [ "$PROBES" = 1 ]; then
  one_run probe_pool50 hot 1 "$OVER_RATE" "$DURATION" 1 "$PROBE_POOL"
fi

echo; python3 "$ANALYZE" table "$SUMMARY"
echo; echo "Done. Zoom Grafana to the times in $RUNS_TSV (epoch seconds). Summary: $SUMMARY  captures: $CAP  env: $OUT/env.txt"
