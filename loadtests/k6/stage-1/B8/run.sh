#!/usr/bin/env bash
# B8: Database restart under load
# ONE command runs the whole experiment:
#     ./run.sh
#
# Idea: while k6 sends events at a fixed rate, I break the path to Postgres once and watch what the API does:
#   restart          docker restart of the Postgres container (the image stops Postgres with SIGINT = "fast shutdown":
#                    every session is terminated, then the container starts again and Postgres comes back up)
#   pause_reset      Toxiproxy proxy "pg" disabled for PAUSE_S seconds: open connections are closed, new ones refused
#   pause_blackhole  Toxiproxy "timeout" toxic with timeout 0 on both directions for PAUSE_S seconds: packets are
#                    held back, sockets stay open, nobody gets an error (a cable cut, not a crash). Toxic removed after.
#                    (Toxiproxy docs: with timeout 0 the data is delayed until the toxic is removed. Not verified by me.)
#   none             control run, nothing broken (checks that rows == 202s when nothing goes wrong)
# I look at: error rate over time, how long until the API answers 202 again, whether the pool gets new connections,
# and a row-count audit afterwards (202s the client saw vs rows really in the table).
#
# Every run: restart the server (fresh pool), warm up, TRUNCATE events, then k6 at RATE req/s for DURATION. FAULT_AT
# seconds after k6 started the fault is injected. The server always connects THROUGH Toxiproxy (localhost:15434), also in
# the restart variant, so the setup is the same everywhere (the proxy costs a little latency, see B3 level 0).
#
# Two small probe loops run during every run (pinned to the monitoring cores, the same cost in every variant):
#   db probe   docker exec pg_isready every 0.25 s, straight into the container (not through the proxy)
#   api probe  curl GET /readyz every 0.25 s (SELECT 1 through the pool), 1 s timeout
# They give the moment the DB was back and the moment the API was back, so I can see if the API recovers later than
# the database. hook.py turns the two files into summary columns.
#
# CPU pinning: NOT done here for containers (cpusets are in the compose file).
#   server 0,6 (taskset below)   postgres 1,7,2,8   k6 3,9,4,10   monitoring + toxiproxy 5,11
#
# This script changes (all restored on exit): rows of table events (TRUNCATE), the toxiproxy proxy "pg" (created if
# missing, enabled/disabled, toxics added and removed), and it restarts the Postgres container. If it is killed hard
# (kill -9) the trap does not run; restore by hand:   ./run.sh RESTORE
# No sudo is needed. Heavy things (docker logs, pg_stat_activity dumps) are only taken in "post" captures, after k6.
#
# Usage:  DRY_RUN=1 ./run.sh                                  (print the plan, run nothing)
#         ./run.sh RESTORE                                    (enable the proxy, remove toxics, start Postgres)
#         VARIANTS="restart" REPEATS=1 DURATION=60s RESULTS_NAME=b8-trial ./run.sh     (trial, NOT a result)
#         RATE=1000 PAUSE_S=20 ./run.sh                       (other rate / longer pause)
set -euo pipefail

EXP_ID="B8"
export PYTHONDONTWRITEBYTECODE=1                 # analyze.py loads hook.py; do not leave a __pycache__ in the repo

# ---- 1. Settings (all can be overridden from the environment) ---------------
VARIANTS="${VARIANTS:-restart pause_reset pause_blackhole none}"
RATE="${RATE:-2000}"                             # well below the capacity (about 4300 direct, B3), so errors come from the fault
DURATION="${DURATION:-120s}"
FAULT_AT="${FAULT_AT:-30}"                       # seconds after k6 is started when the fault is injected
PAUSE_S="${PAUSE_S:-10}"                         # length of the two network pauses
REPEATS="${REPEATS:-3}"
TENANTS="${TENANTS:-100}"
POOL="${POOL:-25}"                               # PG_POOL_MAX, same as B3 to B7
KEY_PREFIX="${KEY_PREFIX:-epk_b8_}"              # api key i = KEY_PREFIX + i, tenant i = t<i>
RESULTS_NAME="${RESULTS_NAME:-b8}"
BASE_URL="${BASE_URL:-http://localhost:8000}"
PROM_URL="${PROM_URL:-http://localhost:9090}"
TOXI_URL="${TOXI_URL:-http://localhost:8474}"
TOXI_PORT="${TOXI_PORT:-15434}"                  # proxied port (published by the compose service)
TOXI_UPSTREAM="${TOXI_UPSTREAM:-postgres:5432}"  # as seen from INSIDE the compose network
MANAGE_SERVER="${MANAGE_SERVER:-1}"
SERVER_CPUS="${SERVER_CPUS:-0,6}"
PROBE_CPUS="${PROBE_CPUS:-5,11}"                 # the probe loops run on the monitoring cores
export K6_CPUS="${K6_CPUS:-3,9,4,10}"            # runner.sh reads this (taskset for k6)
PG_CONTAINER="${PG_CONTAINER:-}"                 # empty = auto-detect
PGUSER="${PGUSER:-eventpulse}"; PGDB="${PGDB:-eventpulse}"
DRY_RUN="${DRY_RUN:-0}"
OVERHEAD_S=50                                    # server restart + warm-up + truncate + checkpoint + settle + row count (rough)

# ---- 2. Paths ----------------------------------------------------------------
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"      # loadtests/k6/stage-1/B8
STAGE_DIR="$(dirname "$HERE")"                             # loadtests/k6/stage-1
RUNNER_DIR="$(dirname "$STAGE_DIR")"                       # loadtests/k6  (runner.sh lives here)
REPO="$(cd "$RUNNER_DIR/../.." && pwd)"
API_DIR="$REPO/services/ingest-api"
ANALYZE="$RUNNER_DIR/analyze.py"                           # shared analyzer
OUT="$HERE/results/$RESULTS_NAME"
CAP="$OUT/captures"; RUNS_TSV="$OUT/runs.tsv"; SUMMARY="$OUT/summary.tsv"
ANALYZE_FLAGS=(--prefix b8 --ok-code 202 --fail-code 503 --err-limit-pct 100 --p99-limit-ms 100000000)
# A run with errors is expected here, so the analyzer's "collapsed" limits are switched off (100 % errors, 100000 s);
# the only verdict that can still fire is LOST-ROWS (an acknowledged event missing from the table).

die() { echo "ERROR: $*" >&2; exit 1; }
to_seconds() { case "$1" in *h) echo $(( ${1%h}*3600 ));; *m) echo $(( ${1%m}*60 ));; *s) echo "${1%s}";; *) echo "$1";; esac; }

# variants for repeat number $1: odd = as listed, even = reversed (drift hits every variant equally)
variants_for_rep() { if [ $(( $1 % 2 )) -eq 1 ]; then echo "$VARIANTS"; else echo "$VARIANTS" | tr ' ' '\n' | tac | tr '\n' ' '; fi; }

# ---- 3. Plan / dry run ---------------------------------------------------------
if [ "${1:-}" != "RESTORE" ]; then
  n_var=$(wc -w <<<"$VARIANTS"); dur_s=$(to_seconds "$DURATION")
  n_runs=$(( n_var*REPEATS )); total_s=$(( n_runs*(dur_s+OVERHEAD_S) ))
  echo "== $EXP_ID: $n_runs runs ($n_var variants x $REPEATS repeats), results in $OUT =="
  echo "== rate $RATE req/s | duration $DURATION | fault at $FAULT_AT s | pause $PAUSE_S s | pool $POOL | tenants $TENANTS | expected total about $(( total_s/60 )) min =="
  if [ "$DRY_RUN" = "1" ]; then
    for rep in $(seq 1 "$REPEATS"); do echo "  repeat $rep variant order: $(variants_for_rep "$rep")"; done
    echo "  variants: restart = docker restart of the Postgres container | pause_reset = proxy disabled for ${PAUSE_S}s | pause_blackhole = timeout toxic (both directions) for ${PAUSE_S}s | none = control"
    echo "  the server connects through Toxiproxy localhost:$TOXI_PORT -> $TOXI_UPSTREAM in every variant"
    echo "  CPU plan (from compose, not changed here): server=$SERVER_CPUS k6=$K6_CPUS probes=$PROBE_CPUS"
    echo "  changes made (all restored on exit): TRUNCATE events; docker restart of the postgres container; toxiproxy proxy 'pg' (create, enable/disable, toxics)"
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
ensure_pg_running() { [ "$(docker inspect -f '{{.State.Running}}' "$PG_CONTAINER" 2>/dev/null)" = true ] || docker start "$PG_CONTAINER" >/dev/null; }

# Wait until the database answers a query. Prints the seconds it took; returns 1 after the limit.
wait_db_up() {   # wait_db_up <max seconds>
  local t0 waited=0; t0=$(date +%s)
  until docker exec "$PG_CONTAINER" psql -U "$PGUSER" -d "$PGDB" -X -Atc "select 1" >/dev/null 2>&1; do
    [ "$waited" -ge "$1" ] && return 1
    sleep 0.5; waited=$(( $(date +%s) - t0 ))
  done
  echo $(( $(date +%s) - t0 ))
}

# ---- 5. Toxiproxy ---------------------------------------------------------------
toxi() { curl -fsS --max-time 5 "$@"; }
toxi_quiet() { curl -sS --max-time 5 -o /dev/null "$@" || true; }
clear_toxics() { for n in bh_down bh_up; do toxi_quiet -X DELETE "$TOXI_URL/proxies/pg/toxics/$n"; done; }
set_proxy_enabled() {   # set_proxy_enabled true|false   (false closes every open connection and refuses new ones)
  toxi -X POST "$TOXI_URL/proxies/pg" -H 'Content-Type: application/json' -d "{\"enabled\":$1}" >/dev/null
}
setup_proxy() {
  # create the proxy if it does not exist yet (409 = exists, fine), then make sure it is enabled and has no toxics
  toxi_quiet -X POST "$TOXI_URL/proxies" -H 'Content-Type: application/json' \
    -d "{\"name\":\"pg\",\"listen\":\"0.0.0.0:${TOXI_PORT}\",\"upstream\":\"${TOXI_UPSTREAM}\",\"enabled\":true}"
  toxi "$TOXI_URL/proxies/pg" >/dev/null || die "proxy 'pg' does not exist and could not be created"
  clear_toxics; set_proxy_enabled true
}
# The black hole: "timeout" toxic with timeout=0 keeps the connection open and lets no data through. Per the Toxiproxy
# docs the data is delayed until the toxic is removed (not dropped); I have not verified that, B8 will show it.
blackhole_on() {
  for d in down:bh_down up:bh_up; do
    toxi -X POST "$TOXI_URL/proxies/pg/toxics" -H 'Content-Type: application/json' \
      -d "{\"name\":\"${d#*:}\",\"type\":\"timeout\",\"stream\":\"$([ "${d%%:*}" = down ] && echo downstream || echo upstream)\",\"toxicity\":1.0,\"attributes\":{\"timeout\":0}}" >/dev/null \
      || die "could not add the timeout toxic ${d#*:}"
  done
}

restore_all() {
  ensure_pg_running 2>/dev/null || true
  clear_toxics; set_proxy_enabled true 2>/dev/null || true
  echo "restored: postgres running, proxy 'pg' enabled, no toxics"
}

if [ "${1:-}" = "RESTORE" ]; then find_pg_container; restore_all; exit 0; fi

preflight() {
  for c in k6 taskset curl docker python3 tac seq awk jq; do command -v "$c" >/dev/null || die "'$c' not found in PATH"; done
  [ -x "$RUNNER_DIR/runner.sh" ] || die "$RUNNER_DIR/runner.sh missing or not executable"
  [ -f "$ANALYZE" ] || die "$ANALYZE missing"
  curl -fsS --max-time 3 "$PROM_URL/-/ready" >/dev/null || die "Prometheus not ready at $PROM_URL (docker compose -f infra/compose/docker-compose.yml up -d)"
  curl -fsS --max-time 3 "$PROM_URL/api/v1/query?query=up%7Bjob%3D%22postgres%22%7D" | grep -q '"value":\[[^]]*"1"\]' \
    || die "postgres-exporter (job=postgres) is not 'up' (docker compose -f infra/compose/docker-compose.yml up -d postgres-exporter)"
  curl -fsS --max-time 3 "$PROM_URL/api/v1/query?query=up%7Bjob%3D%22node%22%7D" | grep -q '"value":\[[^]]*"1"\]' \
    || echo "WARN: node_exporter (job=node) is not 'up'; host panels will be empty" >&2
  curl -fsS --max-time 3 "$TOXI_URL/version" >/dev/null || die "Toxiproxy not reachable at $TOXI_URL (docker compose -f infra/compose/docker-compose.yml up -d toxiproxy)"
  find_pg_container; ensure_pg_running
  wait_db_up 60 >/dev/null || die "Postgres container $PG_CONTAINER does not answer"
  pgq "select count(*) from events" >/dev/null || die "cannot query table 'events' in $PG_CONTAINER (migrations? PGUSER/PGDB?)"
  docker exec "$PG_CONTAINER" pg_isready -q -U "$PGUSER" -d "$PGDB" || die "pg_isready not usable in $PG_CONTAINER"
  [ -f "$API_DIR/dist/main.js" ] || die "$API_DIR/dist/main.js missing: run 'pnpm build' in services/ingest-api"
  grep -q 'ep_db_errors_by_code_total' "$API_DIR/dist/"*/*.js 2>/dev/null \
    || die "dist/ has no ep_db_errors_by_code_total (the B8 instrumentation): rebuild ingest-api (pnpm build)"
  [ -f "$API_DIR/.env" ] || die "$API_DIR/.env missing"
  DIRECT_URL=$(grep -E '^DATABASE_URL=' "$API_DIR/.env" | head -1 | cut -d= -f2- | tr -d '"')
  [ -n "$DIRECT_URL" ] || die "DATABASE_URL not found in $API_DIR/.env"
  PROXY_URL=$(sed -E "s#@[^/]+/#@localhost:${TOXI_PORT}/#" <<<"$DIRECT_URL")    # same user/password/db, host:port of the proxy
  if [ "$MANAGE_SERVER" = 1 ] && curl -fsS --max-time 2 "$BASE_URL/healthz" >/dev/null 2>&1; then
    die "something already answers on $BASE_URL; stop it (run.sh starts its own ingest-api per run) or set MANAGE_SERVER=0"
  fi
  echo "postgres container: $PG_CONTAINER | server connects through the proxy localhost:$TOXI_PORT -> $TOXI_UPSTREAM"
}

# ---- 6. Server lifecycle (ingest-api) ---------------------------------------------
SERVER_PID=""
# start_server. Env vars beat --env-file, so DATABASE_URL/API_KEYS/COUNTER_MODE/PG_POOL_MAX here win over .env.
# PG_CONN_TIMEOUT_MS and PG_STATEMENT_TIMEOUT_MS are NOT set here: the values of .env are what the service runs with.
start_server() {
  [ "$MANAGE_SERVER" = 1 ] || return 0
  stop_server
  local keys; keys=$(seq 0 $(( TENANTS - 1 )) | awk -v p="$KEY_PREFIX" '{printf "%s%s%d:t%d", (NR>1?",":""), p, $1, $1}')
  ( cd "$API_DIR" && exec env DATABASE_URL="$PROXY_URL" API_KEYS="$keys" COUNTER_MODE=off PG_POOL_MAX="$POOL" taskset -c "$SERVER_CPUS" node --env-file=.env dist/main.js ) >"$OUT/server.out" 2>&1 &
  SERVER_PID=$!       # exec keeps the pid: this is the node pid
  for _ in $(seq 1 30); do curl -fsS --max-time 1 "$BASE_URL/healthz" >/dev/null 2>&1 && break; sleep 1; done
  curl -fsS --max-time 2 "$BASE_URL/readyz" >/dev/null || die "ingest-api not ready (see $OUT/server.out)"
  local got; got=$(curl -fsS --max-time 2 "$BASE_URL/debug/stats" | python3 -c 'import sys,json; print(json.load(sys.stdin)["pool"]["max"])' 2>/dev/null || echo "?")
  [ "$got" = "$POOL" ] || die "server reports pool max '$got', I asked for '$POOL' (see $OUT/server.out)"
}
stop_server() {
  [ "$MANAGE_SERVER" = 1 ] || return 0
  if [ -n "${SERVER_PID:-}" ] && kill -0 "$SERVER_PID" 2>/dev/null; then kill "$SERVER_PID" 2>/dev/null || true; wait "$SERVER_PID" 2>/dev/null || true; fi
  SERVER_PID=""
}

PROBE_PIDS=""
stop_probes() {
  for p in $PROBE_PIDS; do kill "$p" 2>/dev/null || true; wait "$p" 2>/dev/null || true; done
  PROBE_PIDS=""
}
cleanup() { stop_probes; stop_server; restore_all; return 0; }
trap cleanup EXIT
trap 'exit 130' INT TERM        # so Ctrl+C also runs the EXIT trap above (and puts the proxy and Postgres back)

# ---- 7. Per-run helpers ---------------------------------------------------------
wait_db_quiet() {   # wait until no other backend is working
  local n waited=0
  while :; do
    n=$(pgq "select count(*) from pg_stat_activity where datname='$PGDB' and state<>'idle' and pid<>pg_backend_pid()")
    [ "${n:-0}" -eq 0 ] && return 0
    [ "$waited" -ge 120 ] && { echo "WARN: $n backends still active after 120 s, continuing"; return 0; }
    echo "  waiting for postgres to go quiet ($n active)..."; sleep 3; waited=$((waited+3))
  done
}
reset_db() {   # empty table, fresh statistics, checkpoint (the run starts with no dirty pages)
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

# Probe loops: one line per probe, "epoch_ms<TAB>ok<TAB>ms". Pinned to the monitoring cores; children inherit it.
# (-q of pg_isready: no output, only the exit code. curl -m 1: a black hole must not hold the loop for longer than 1 s.)
start_probes() {   # start_probes <file prefix>
  local pre="$1"
  ( while :; do
      t0=$(date +%s%3N)
      if docker exec "$PG_CONTAINER" pg_isready -q -U "$PGUSER" -d "$PGDB" >/dev/null 2>&1; then ok=1; else ok=0; fi
      printf '%s\t%s\t%s\n' "$t0" "$ok" "$(( $(date +%s%3N) - t0 ))"; sleep 0.2
    done ) >"$pre.db.tsv" 2>/dev/null &
  PROBE_PIDS="$!"; taskset -cp "$PROBE_CPUS" "$!" >/dev/null 2>&1 || true
  ( while :; do
      t0=$(date +%s%3N)
      if [ "$(curl -s -o /dev/null -m 1 -w '%{http_code}' "$BASE_URL/readyz")" = 200 ]; then ok=1; else ok=0; fi
      printf '%s\t%s\t%s\n' "$t0" "$ok" "$(( $(date +%s%3N) - t0 ))"; sleep 0.2
    done ) >"$pre.api.tsv" 2>/dev/null &
  PROBE_PIDS="$PROBE_PIDS $!"; taskset -cp "$PROBE_CPUS" "$!" >/dev/null 2>&1 || true
}

# Server-side numbers of THIS run (the server was restarted for this run, so its counters start at 0):
# error codes, pool connects / removes / idle errors, pool state at the end.
err_codes() {
  curl -fsS --max-time 2 "$BASE_URL/metrics" 2>/dev/null | awk '
    /^ep_db_errors_by_code_total\{/ { match($0,/code="[^"]*"/); t=substr($0,RSTART+6,RLENGTH-7); n=$NF; if (n>0) printf "%s%s=%d", (c++?";":""), t, n }'
}
metric1() { curl -fsS --max-time 2 "$BASE_URL/metrics" 2>/dev/null | awk -v m="$1" '$1==m {printf "%d", $2; f=1} END{if(!f) printf "n/a"}'; }
pool_end() { curl -fsS --max-time 2 "$BASE_URL/debug/stats" 2>/dev/null | python3 -c 'import sys,json; p=json.load(sys.stdin)["pool"]; print("%s/%s/%s of %s" % (p["total"],p["idle"],p["waiting"],p["max"]))' 2>/dev/null || echo n/a; }

run_k6() {   # run_k6 <testid> <variant>
  ( cd "$RUNNER_DIR" && env RESULTS_ROOT="$HERE/results" RESULTS_NAME="$RESULTS_NAME" LABEL="$1" SCRIPT="stage-1/B8/script.js" \
      RATE="$RATE" DURATION="$DURATION" PHASE=outage VARIANT="$2" TENANTS="$TENANTS" POOL_MAX="$POOL" KEY_PREFIX="$KEY_PREFIX" \
      BASE_URL="$BASE_URL" ROUTE="/healthz" ./runner.sh )
}

# Inject the fault of the variant. Sets FAULT_START and FAULT_END (epoch seconds, FAULT_END = when the path / database is
# back). For "none" both are the planned time and nothing happens.
inject_fault() {   # inject_fault <variant>
  FAULT_START=$(date +%s.%N)
  case "$1" in
    restart)
      docker restart -t 30 "$PG_CONTAINER" >/dev/null     # returns when the container is started again; Postgres may still be recovering
      wait_db_up 120 >/dev/null || die "Postgres did not answer within 120 s after the restart"
      ;;
    pause_reset)
      set_proxy_enabled false; sleep "$PAUSE_S"; set_proxy_enabled true
      ;;
    pause_blackhole)
      blackhole_on; sleep "$PAUSE_S"; clear_toxics
      ;;
    none) ;;
    *) die "unknown variant '$1'" ;;
  esac
  FAULT_END=$(date +%s.%N)
  [ "$1" = none ] && FAULT_END="$FAULT_START"
  return 0
}

# ONE run. one_run <variant> <rep>
one_run() {
  VARIANT="$1"; local rep="$2"
  testid="b8-${VARIANT}-n${rep}"
  echo; echo "== $VARIANT: rate=$RATE duration=$DURATION fault at ${FAULT_AT}s repeat=$rep ($testid) =="
  CAP_FILE="$CAP/$testid.txt"; : >"$CAP_FILE"
  # fresh start: working path, fresh server (new pool), empty table
  stop_probes; stop_server; ensure_pg_running; setup_proxy; wait_db_up 60 >/dev/null || die "Postgres not answering"
  wait_db_quiet; start_server; warmup; wait_db_quiet; reset_db; sleep 5
  local rows_pre; rows_pre=$(pgq "select count(*) from events")
  tag pre "rows_pre=$rows_pre pool=$(pool_end)"
  local t0 status=ok; t0=$(date +%s)
  start_probes "$CAP/$testid"
  run_k6 "$testid" "$VARIANT" & local kpid=$!
  sleep "$FAULT_AT"
  inject_fault "$VARIANT"
  tag fault "variant=$VARIANT fault_start=$FAULT_START fault_end=$FAULT_END"
  wait "$kpid" || { status=FAILED; echo "!! $testid failed: see its manifest.md"; }
  stop_probes
  sleep 3; wait_db_quiet                       # let requests that were still running finish before counting rows
  local rows_post; rows_post=$(pgq "select count(*) from events"); local rows=$(( rows_post - rows_pre ))
  local codes; codes=$(err_codes || true)
  local pend="pool_end=$(pool_end) connects=$(metric1 ep_pg_pool_connects_total) removes=$(metric1 ep_pg_pool_removes_total) idle_errors=$(metric1 ep_pg_pool_idle_errors_total)"
  tag post "error codes from the server: ${codes:-none}; $pend; events rows $rows_post (before the run: $rows_pre)"
  # postgres' own view of the restart (only the variant that restarts it)
  if [ "$VARIANT" = restart ]; then
    tag post "docker logs of the postgres container since the fault"
    docker logs --since "$(date -u -d "@${FAULT_START%.*}" +%Y-%m-%dT%H:%M:%SZ)" "$PG_CONTAINER" 2>&1 | grep -E "shutting down|terminating|checkpoint|redo|ready to accept|starting up|not properly" | head -15 >>"$CAP_FILE" || true
  fi
  printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$testid" "$VARIANT" "$t0" "$(date +%s)" "$status" "${FAULT_START%.*}" >>"$RUNS_TSV"
  local js; js=$(ls -t "$OUT"/*/"${testid}_1-lap.json" 2>/dev/null | head -1 || true)
  if [ -n "$js" ]; then
    B8_PROBE_PREFIX="$CAP/$testid" B8_FAULT_START="$FAULT_START" B8_FAULT_END="$FAULT_END" \
    python3 "$ANALYZE" row --json "$js" --testid "$testid" --phase outage --rate "$RATE" "${ANALYZE_FLAGS[@]}" \
      --var "variant=$VARIANT" --rows "$rows" --extra "err_codes=${codes:-none}" \
      --extra "pool_end=$(pool_end)" --extra "pool_connects=$(metric1 ep_pg_pool_connects_total)" --extra "pool_removes=$(metric1 ep_pg_pool_removes_total)" \
      --hook "$HERE/hook.py" --out "$SUMMARY" \
      || echo "WARN: could not add the summary row of $testid (the numbers are still in its capture files); continuing" >&2
    echo "   -> $(python3 "$ANALYZE" table "$SUMMARY" | tail -1 | tr -s ' ')"
  else
    echo "   !! no summary json for $testid (k6 failed before finishing?)"
  fi
}

# ---- 8. Main ----------------------------------------------------------------------
preflight
mkdir -p "$CAP"
printf 'testid\tvariant\tstart_epoch\tend_epoch\tstatus\tfault_start_epoch\n' >"$RUNS_TSV"

{   # environment snapshot (read only)
  echo "experiment: $EXP_ID   date: $(date -Is)"
  echo "git: $(git -C "$REPO" rev-parse --short HEAD 2>/dev/null || echo n/a), $(git -C "$REPO" status --porcelain 2>/dev/null | wc -l) dirty files"
  echo "kernel: $(uname -r)   node: $(node -v)   k6: $(k6 version | head -1)   cpus: $(nproc)"
  echo "pinning: server=$SERVER_CPUS k6=$K6_CPUS probes=$PROBE_CPUS; container cpusets (from compose):"
  for c in "$PG_CONTAINER" ep-prometheus ep-grafana ep-node-exporter ep-postgres-exporter ep-toxiproxy; do
    docker inspect -f '  {{.Name}} cpuset={{.HostConfig.CpusetCpus}} nanocpus={{.HostConfig.NanoCpus}} restart={{.HostConfig.RestartPolicy.Name}}' "$c" 2>/dev/null || true
  done
  echo "settings: RATE=$RATE DURATION=$DURATION FAULT_AT=$FAULT_AT PAUSE_S=$PAUSE_S POOL=$POOL TENANTS=$TENANTS VARIANTS=[$VARIANTS] REPEATS=$REPEATS"
  echo "server .env (as used, passwords hidden):"; grep -E '^(PG_|RETRY|BODY|LOG)' "$API_DIR/.env" | sed 's/^/  /'
  echo "--- postgres ---"
  docker inspect -f 'image={{.Config.Image}} nanocpus={{.HostConfig.NanoCpus}} memory={{.HostConfig.Memory}} cpuset={{.HostConfig.CpusetCpus}} stopsignal={{.Config.StopSignal}}' "$PG_CONTAINER"
  pgq "select version()" || true
  for s in max_connections synchronous_commit fsync checkpoint_timeout max_wal_size tcp_keepalives_idle; do echo "$s = $(pgq "show $s" 2>/dev/null)"; done
  echo "--- toxiproxy ---"; toxi "$TOXI_URL/version" || true; echo
  echo "--- cpu model ---"; lscpu | grep -E 'Model name|Thread|Core|Socket' || true
} >"$OUT/env.txt"

# repeat is the OUTER loop; odd repeats forward, even repeats reversed
for rep in $(seq 1 "$REPEATS"); do
  for v in $(variants_for_rep "$rep"); do
    one_run "$v" "$rep"
  done
done

echo; [ -f "$SUMMARY" ] && python3 "$ANALYZE" table "$SUMMARY"
echo; echo "Done. Zoom Grafana to the times in $RUNS_TSV (epoch seconds). Summary: $SUMMARY  captures: $CAP (probe files: *.db.tsv, *.api.tsv)  env: $OUT/env.txt"
