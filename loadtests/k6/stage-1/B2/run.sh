#!/usr/bin/env bash
# B2: Pool exhaustion
# ONE command runs the whole experiment:
#     ./run.sh
#
# Idea: the request rate is FIXED (RATE, default 2150 req/s, which collapsed with pool 10 in B1).
# The only thing I change is the size of the pg connection pool of ingest-api (PG_POOL_MAX):
#     5  10  25  50  90  120      (120 is above Postgres max_connections=100)
# For every pool size the script restarts ingest-api with that PG_POOL_MAX, so the pool starts empty.
#
# What the script does (so I can re-read it later):
#   1. preflight    - tools, Prometheus, postgres-exporter, build of ingest-api, Postgres container
#   2. pinning      - docker update --cpuset-cpus so server / postgres / k6 / monitoring use different cores
#   3. env snapshot - versions, Postgres settings, max_connections and usable slots, pinning
#   4. for every repeat x pool size: restart server, warm up, truncate table, k6 via runner.sh,
#                     light pg snapshots before/after, row-count audit, error types, one summary row
#   5. probes       - SEPARATE short runs where heavy tools (pg_stat_activity by wait event, iostat...) are allowed.
#                     Never used for headline numbers (Stage 0 lesson: strace inside a run polluted it).
#
# Order: repeat is the OUTER loop. Odd repeats go small -> big pool, even repeats big -> small,
# so drift (thermal, cache) and order effects do not always hit the same pool size.
#
# Usage:  DRY_RUN=1 ./run.sh                              (print the plan, run nothing)
#         LEVELS="25" REPEATS=1 DURATION=30s ./run.sh     (small trial, NOT a headline result)
#         RATE=1350 ./run.sh                              (another fixed rate)
#         MANAGE_SERVER=0 ./run.sh                        (NOT useful for B2: it needs a restart per pool size)
#         PG_CONTAINER=my-postgres ./run.sh               (if auto-detection finds 0 or 2+ containers)
#
# CPU plan (12 logical CPUs = 6 cores, SMT pairs 0/6 1/7 2/8 3/9 4/10 5/11). Each component gets whole cores:
#   server   0,6         postgres 1,7,2,8 (2 cores, matches cpus: 2)
#   k6       3,9,4,10    monitoring 5,11  (prometheus, grafana, node_exporter, postgres-exporter)
set -euo pipefail

EXP_ID="B2"

# ---- 1. Settings (all can be overridden from the environment) ---------------
LEVELS="${LEVELS:-5 10 25 50 90 120}"           # PG_POOL_MAX values. 120 > max_connections (100) on purpose
RATE="${RATE:-2150}"                            # fixed offered rate, req/s
REPEATS="${REPEATS:-3}"
DURATION="${DURATION:-60s}"
PROBES="${PROBES:-1}"                           # 1 = also run the probe runs below
PROBE_LEVELS="${PROBE_LEVELS:-10 90}"           # one "normal" pool and one "big" pool
PROBE_DURATION="${PROBE_DURATION:-30s}"
RESULTS_NAME="${RESULTS_NAME:-b2}"
BASE_URL="${BASE_URL:-http://localhost:8000}"
PROM_URL="${PROM_URL:-http://localhost:9090}"
MANAGE_SERVER="${MANAGE_SERVER:-1}"
SERVER_CPUS="${SERVER_CPUS:-0,6}"
PG_CPUS="${PG_CPUS:-1,7,2,8}"
export K6_CPUS="${K6_CPUS:-3,9,4,10}"          # runner.sh reads this (taskset for k6)
MON_CPUS="${MON_CPUS:-5,11}"
PG_CONTAINER="${PG_CONTAINER:-}"                # empty = auto-detect
MON_CONTAINERS="${MON_CONTAINERS:-}"            # empty = auto-detect (prometheus, grafana, node-exporter, postgres-exporter)
PGUSER="${PGUSER:-eventpulse}"; PGDB="${PGDB:-eventpulse}"
KEEP_PINNING="${KEEP_PINNING:-0}"               # 1 = do not restore the old cpusets at the end
DRY_RUN="${DRY_RUN:-0}"
OVERHEAD_S=45                                   # restart + warm-up + truncate + settle per run (rough)

# ---- 2. Paths ----------------------------------------------------------------
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"      # loadtests/k6/stage-1/B2
STAGE_DIR="$(dirname "$HERE")"                             # loadtests/k6/stage-1
RUNNER_DIR="$(dirname "$STAGE_DIR")"                       # loadtests/k6  (runner.sh lives here)
REPO="$(cd "$RUNNER_DIR/../.." && pwd)"
API_DIR="$REPO/services/ingest-api"
OUT="$HERE/results/$RESULTS_NAME"                    # runner.sh writes <timestamp>/ folders in here
CAP="$OUT/captures"; RUNS_TSV="$OUT/runs.tsv"; SUMMARY="$OUT/summary.tsv"

die() { echo "ERROR: $*" >&2; exit 1; }
to_seconds() { case "$1" in *h) echo $(( ${1%h}*3600 ));; *m) echo $(( ${1%m}*60 ));; *s) echo "${1%s}";; *) echo "$1";; esac; }

# order of pool sizes for repeat number $1: odd = as listed, even = reversed
levels_for_rep() { if [ $(( $1 % 2 )) -eq 1 ]; then echo "$LEVELS"; else echo "$LEVELS" | tr ' ' '\n' | tac | tr '\n' ' '; fi; }

# ---- 3. Plan / dry run ---------------------------------------------------------
n_levels=$(wc -w <<<"$LEVELS")
n_probe=0; [ "$PROBES" = "1" ] && n_probe=$(wc -w <<<"$PROBE_LEVELS")
total_s=$(( n_levels*REPEATS*($(to_seconds "$DURATION")+OVERHEAD_S) + n_probe*($(to_seconds "$PROBE_DURATION")+OVERHEAD_S+20) ))
echo "== $EXP_ID: $((n_levels*REPEATS)) measured runs + $n_probe probe runs, results in $OUT =="
echo "== fixed rate: $RATE req/s | pool sizes: $LEVELS | duration $DURATION | repeats $REPEATS | expected total about $(( total_s/60 )) min =="
if [ "$DRY_RUN" = "1" ]; then
  for rep in $(seq 1 "$REPEATS"); do echo "  repeat $rep order: $(levels_for_rep "$rep")"; done
  for p in $( [ "$PROBES" = 1 ] && echo "$PROBE_LEVELS" ); do echo "  probe:    pool=$p ($PROBE_DURATION)"; done
  echo "  CPU plan: server=$SERVER_CPUS postgres=$PG_CPUS k6=$K6_CPUS monitoring=$MON_CPUS"
  exit 0
fi

# ---- 4. Helpers ------------------------------------------------------------------
pgq()  { docker exec -i "$PG_CONTAINER" psql -U "$PGUSER" -d "$PGDB" -X -At -c "$1"; }   # one value / rows, no decoration
pgqt() { docker exec -i "$PG_CONTAINER" psql -U "$PGUSER" -d "$PGDB" -X -c "$1"; }       # pretty table for captures

detect_containers() {
  if [ -z "$PG_CONTAINER" ]; then
    local found; found=$(docker ps --format '{{.Names}} {{.Image}}' | awk '$2 ~ /^postgres:/ {print $1}')
    [ "$(wc -w <<<"$found")" = 1 ] || die "found [$found] postgres containers; set PG_CONTAINER=<name> (docker ps to see names)"
    PG_CONTAINER="$found"
  fi
  if [ -z "$MON_CONTAINERS" ]; then
    MON_CONTAINERS=$(docker ps --format '{{.Names}} {{.Image}}' | awk '$2 ~ /prometheus|grafana|node-exporter|node_exporter|postgres-exporter/ {print $1}' | tr '\n' ' ')
  fi
}

preflight() {
  for c in k6 taskset curl docker python3 tac; do command -v "$c" >/dev/null || die "'$c' not found in PATH"; done
  command -v iostat >/dev/null || echo "WARN: iostat missing (apt install sysstat); probe runs will skip it" >&2
  [ -x "$RUNNER_DIR/runner.sh" ] || die "$RUNNER_DIR/runner.sh missing or not executable"
  curl -fsS --max-time 3 "$PROM_URL/-/ready" >/dev/null || die "Prometheus not ready at $PROM_URL (docker compose -f infra/compose/docker-compose.yml up -d)"
  curl -fsS --max-time 3 "$PROM_URL/api/v1/query?query=up%7Bjob%3D%22node%22%7D" | grep -q '"value":\[[^]]*"1"\]' \
    || echo "WARN: node_exporter (job=node) is not 'up'; host panels will be empty" >&2
  curl -fsS --max-time 3 "$PROM_URL/api/v1/query?query=up%7Bjob%3D%22postgres%22%7D" | grep -q '"value":\[[^]]*"1"\]' \
    || die "postgres-exporter (job=postgres) is not 'up'. Start it: docker compose -f infra/compose/docker-compose.yml up -d postgres-exporter, and reload Prometheus config (curl -X POST $PROM_URL/-/reload)"
  detect_containers
  docker exec "$PG_CONTAINER" true || die "cannot docker exec into $PG_CONTAINER"
  pgq "select 1" >/dev/null || die "psql inside $PG_CONTAINER failed (PGUSER=$PGUSER PGDB=$PGDB?)"
  pgq "select count(*) from events" >/dev/null || die "table 'events' not found: run the migrations (pnpm migrate:up)"
  if [ "$MANAGE_SERVER" = 1 ]; then
    [ -f "$API_DIR/dist/main.js" ] || die "$API_DIR/dist/main.js missing: run 'pnpm build' in services/ingest-api"
    grep -q 'ep_db_errors_by_type_total' "$API_DIR/dist/"*.js "$API_DIR/dist/"*/*.js 2>/dev/null \
      || die "dist/ has no ep_db_errors_by_type_total: rebuild ingest-api (pnpm build) after the B2 server change"
    [ -f "$API_DIR/.env" ] || die "$API_DIR/.env missing"
  else
    echo "WARN: MANAGE_SERVER=0: B2 needs a restart with a different PG_POOL_MAX per level; this will not do that." >&2
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

# ---- 6. Server lifecycle (ingest-api; NOT the Stage 0 server-runner.sh, it is hard-wired to labs/stage-0) ----
SERVER_PID=""
start_server() {   # start_server <pool_max>   (PG_POOL_MAX on the command line overrides services/ingest-api/.env)
  if [ "$MANAGE_SERVER" != 1 ]; then return 0; fi
  stop_server
  ( cd "$API_DIR" && exec env PG_POOL_MAX="$1" taskset -c "$SERVER_CPUS" node --env-file=.env dist/main.js ) >"$OUT/server.out" 2>&1 &
  SERVER_PID=$!       # exec keeps the pid: this is the node pid
  for _ in $(seq 1 30); do curl -fsS --max-time 1 "$BASE_URL/healthz" >/dev/null 2>&1 && break; sleep 1; done
  curl -fsS --max-time 2 "$BASE_URL/readyz" >/dev/null || die "ingest-api not ready (see $OUT/server.out)"
  # the server must really run with the pool size I asked for (otherwise the whole run is meaningless)
  local got; got=$(curl -fsS --max-time 2 "$BASE_URL/debug/stats" | python3 -c 'import sys,json; print(json.load(sys.stdin)["pool"]["max"])' 2>/dev/null || echo "?")
  [ "$got" = "$1" ] || die "server reports pool max '$got', I asked for '$1' (see $OUT/server.out)"
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
warmup() {          # 100 requests, 10 at a time: opens some pool connections and warms up V8; truncated afterwards
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
# Error types of THIS run, from the server counter (the server was restarted for this run, so it starts at 0).
# Output like "pool_timeout=120;too_many_clients=4"; empty if there were none.
err_types() {
  curl -fsS --max-time 2 "$BASE_URL/metrics" 2>/dev/null | awk '
    /^ep_db_errors_by_type_total\{/ { match($0,/type="[^"]*"/); t=substr($0,RSTART+6,RLENGTH-7); n=$NF; if (n>0) printf "%s%s=%d", (c++?";":""), t, n }'
}
# Heavy tools: ONLY from probe runs. They cost CPU on the machine.
heavy_probe() {
  tag probe-mid "pg_stat_activity grouped by application, state, wait event"
  pgqt "select application_name, state, wait_event_type, wait_event, count(*) from pg_stat_activity where datname='$PGDB' group by 1,2,3,4 order by 5 desc" >>"$CAP_FILE" 2>&1 || true
  tag probe-mid "backends total vs max_connections"
  pgqt "select count(*) as backends, current_setting('max_connections') as max_connections, current_setting('superuser_reserved_connections') as reserved from pg_stat_activity where backend_type='client backend'" >>"$CAP_FILE" 2>&1 || true
  tag probe-mid "blocked and blocking"
  pgqt "select pid, pg_blocking_pids(pid) as blocked_by, wait_event_type, wait_event, left(query,60) q from pg_stat_activity where cardinality(pg_blocking_pids(pid))>0" >>"$CAP_FILE" 2>&1 || true
  tag probe-mid "pg_stat_statements top 5 by total time"
  pgqt "select calls, round(mean_exec_time::numeric,3) mean_ms, round(total_exec_time::numeric,0) total_ms, left(query,70) q from pg_stat_statements order by total_exec_time desc limit 5" >>"$CAP_FILE" 2>&1 || true
  tag probe-mid "iostat -x 1 5"; command -v iostat >/dev/null && iostat -x 1 5 >>"$CAP_FILE" 2>&1 || true
  tag probe-mid "vmstat 1 5";   vmstat 1 5 >>"$CAP_FILE" 2>&1 || true
  tag probe-mid "pidstat -u -p $SERVER_PID 1 5"; command -v pidstat >/dev/null && [ -n "$SERVER_PID" ] && pidstat -u -p "$SERVER_PID" 1 5 >>"$CAP_FILE" 2>&1 || true
}

run_k6() {   # run_k6 <pool> <duration> <testid>
  ( cd "$RUNNER_DIR" && env RESULTS_ROOT="$HERE/results" RESULTS_NAME="$RESULTS_NAME" LABEL="$3" SCRIPT="stage-1/B2/script.js" \
      RATE="$RATE" DURATION="$2" PHASE="$PHASE" POOL_MAX="$1" API_KEY="$API_KEY" BASE_URL="$BASE_URL" ROUTE="/healthz" ./runner.sh )
}
record() { printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$testid" "$1" "$POOL" "$2" "$3" "$4" >>"$RUNS_TSV"; }

one_run() {  # one_run <phase> <pool> <duration> <rep>
  PHASE="$1"; POOL="$2"; local dur="$3" rep="$4"
  testid="b2-${PHASE:0:1}-p${POOL}-n${rep}"
  [ "$PHASE" = probe ] && testid="b2-probe-p${POOL}"
  echo; echo "== $PHASE: pool=$POOL rate=$RATE repeat=$rep ($testid) =="
  CAP_FILE="$CAP/$testid.txt"; : >"$CAP_FILE"
  stop_server; wait_db_quiet; start_server "$POOL"; warmup; wait_db_quiet; reset_db
  sleep 5                                    # let the 5 s of reset/checkpoint I/O finish before measuring
  light_snapshot pre; local t0; t0=$(date +%s); local status=ok
  if [ "$PHASE" = probe ]; then
    run_k6 "$POOL" "$dur" "$testid" & local kpid=$!
    for _ in $(seq 1 60); do pgrep -x k6 >/dev/null && break; sleep 1; done
    sleep $(( $(to_seconds "$dur") / 3 )); heavy_probe
    wait "$kpid" || status=FAILED
  else
    run_k6 "$POOL" "$dur" "$testid" || { status=FAILED; echo "!! $testid failed: see its manifest.md"; }
  fi
  sleep 3; wait_db_quiet                      # let requests that were still running finish before counting rows
  local rows; rows=$(pgq "select count(*) from events")
  local et; et=$(err_types || true)
  light_snapshot post; tag post "error types from the server: ${et:-none}"; record "$PHASE" "$t0" "$(date +%s)" "$status"
  local js; js=$(ls -t "$OUT"/*/"${testid}_1-lap.json" 2>/dev/null | head -1 || true)
  if [ -n "$js" ]; then
    python3 "$HERE/analyze.py" row --json "$js" --testid "$testid" --phase "$PHASE" --pool "$POOL" --rate "$RATE" --rows "$rows" --errtypes "$et" >>"$SUMMARY"
    tail -1 "$SUMMARY" | awk -F'\t' '{printf "   -> pool %s: achieved %s rps, err %s%%, p99 %s ms, dropped %s, audit: %s, errors: %s | %s\n",$3,$5,$10,$13,$15,$17,$18,$19}'
  else
    echo "   !! no summary json for $testid (k6 failed before finishing?)"
  fi
}

# ---- 8. Main ----------------------------------------------------------------------
preflight
mkdir -p "$CAP"
printf 'testid\tphase\tpool\tstart_epoch\tend_epoch\tstatus\n' >"$RUNS_TSV"
python3 - "$SUMMARY" <<'PY'
import sys
cols = ["testid","phase","pool","rate","achieved_rps","reqs","n202","n503","n_other","err_pct","p50_ms","p95_ms","p99_ms","max_ms","dropped","rows","audit","err_types","verdict"]
open(sys.argv[1], "w").write("\t".join(cols) + "\n")
PY

pin_container "$PG_CONTAINER" "$PG_CPUS"
for c in $MON_CONTAINERS; do pin_container "$c" "$MON_CPUS"; done

{   # environment snapshot
  echo "experiment: $EXP_ID   date: $(date -Is)"
  echo "git: $(git -C "$REPO" rev-parse --short HEAD 2>/dev/null || echo n/a), $(git -C "$REPO" status --porcelain 2>/dev/null | wc -l) dirty files"
  echo "kernel: $(uname -r)   node: $(node -v)   k6: $(k6 version | head -1)   cpus: $(nproc)"
  echo "pinning: server=$SERVER_CPUS postgres=$PG_CPUS k6=$K6_CPUS monitoring=$MON_CPUS ($MON_CONTAINERS)"
  echo "settings: RATE=$RATE LEVELS=[$LEVELS] REPEATS=$REPEATS DURATION=$DURATION PROBES=$PROBES PROBE_LEVELS=[$PROBE_LEVELS]"
  echo "--- postgres container ---"
  docker inspect -f 'image={{.Config.Image}} nanocpus={{.HostConfig.NanoCpus}} memory={{.HostConfig.Memory}} cpuset={{.HostConfig.CpusetCpus}}' "$PG_CONTAINER"
  pgq "select version()" || true
  for s in max_connections superuser_reserved_connections shared_buffers work_mem synchronous_commit fsync wal_level checkpoint_timeout max_wal_size; do echo "$s = $(pgq "show $s" 2>/dev/null)"; done
  echo "usable slots for the pool = max_connections - superuser_reserved_connections - 1 (postgres-exporter) - 1 (a psql of mine) = $(( $(pgq "show max_connections") - $(pgq "show superuser_reserved_connections") - 2 ))"
  echo "--- ingest-api pool settings (from services/ingest-api/src/db/db.module.ts, not read at run time) ---"
  echo "connectionTimeoutMillis=PG_CONN_TIMEOUT_MS (default 2000)  statement_timeout=PG_STATEMENT_TIMEOUT_MS (default 5000)  idleTimeoutMillis=30000"
  grep -E '^(PG_CONN_TIMEOUT_MS|PG_STATEMENT_TIMEOUT_MS)=' "$API_DIR/.env" 2>/dev/null || echo "(no PG_*_TIMEOUT overrides in .env)"
  echo "--- cpu model ---"; lscpu | grep -E 'Model name|Thread|Core|Socket' || true
} >"$OUT/env.txt"

# Postgres container CPU/mem/disk -> Prometheus (needs cgroup v2; if it cannot start, those panels stay empty)
PG_CONTAINER="$PG_CONTAINER" bash "$REPO/infra/scripts/pg-container-exporter.sh" 2>"$OUT/pg-exporter.log" &
EXPORTER_PID=$!; sleep 2
kill -0 "$EXPORTER_PID" 2>/dev/null || { echo "WARN: pg-container-exporter did not start ($(tail -1 "$OUT/pg-exporter.log")); Postgres CPU/memory panels will be empty" >&2; EXPORTER_PID=""; }

# measured runs: repeat is the OUTER loop; odd repeats ascending, even repeats descending pool size
for rep in $(seq 1 "$REPEATS"); do
  for pool in $(levels_for_rep "$rep"); do one_run measured "$pool" "$DURATION" "$rep"; done
done
# probe runs: short, separate
if [ "$PROBES" = 1 ]; then for pool in $PROBE_LEVELS; do one_run probe "$pool" "$PROBE_DURATION" 1; done; fi

echo; python3 "$HERE/analyze.py" table "$SUMMARY"
echo; echo "Done. Zoom Grafana to the times in $RUNS_TSV (epoch seconds). Summary: $SUMMARY  captures: $CAP  env: $OUT/env.txt"
