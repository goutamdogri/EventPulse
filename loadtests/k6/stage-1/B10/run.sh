#!/usr/bin/env bash
# B10: Missing timeouts
# ONE command runs the whole experiment:
#     ./run.sh
#
# Idea: the same overload and the same stall of the database, once with NO deadlines in the server
# (variant "unset": PG_CONN_TIMEOUT_MS=0 and PG_STATEMENT_TIMEOUT_MS=0, which db.module.ts turns into "no timeout")
# and once WITH deadlines (variant "set": 2000 ms waiting for a pool connection, 5000 ms per statement).
#
# Two faults (a latency toxic alone can never trigger statement_timeout: that timer runs inside Postgres, and a
# slow NETWORK is not a slow STATEMENT. So the second fault makes the statement itself slow on the server):
#   lat    the B3 situation: Toxiproxy adds 200 ms to every database answer for the whole run, k6 offers more than
#          the pool can handle. Only the pool wait deadline can help here.
#   stall  a separate Postgres session takes a lock on the table "events" and sleeps while holding it, so every
#          INSERT of the API blocks inside Postgres. Both deadlines apply.
#
# What the script does (so I can re-read it later):
#   1. preflight    - tools, Prometheus, Toxiproxy, build of ingest-api, Postgres container
#   2. env snapshot - versions, settings, pinning (read only)
#   3. for every repeat x fault x variant: restart server with that variant, warm up, (lat: add toxic), truncate,
#      k6 via runner.sh (stall: take the lock STALL_AT seconds after k6 starts), wait until the server has
#      drained, row count, summary row through the shared analyze.py
#   4. one probe run (stall, unset) with heavy tools in the middle of the stall. Never a headline number.
#
# Order: repeat is the OUTER loop. Odd repeats run unset before set, even repeats the other way round.
#
# CPU pinning: NOT done here. Containers have cpusets from compose:
#   server 0,6 (taskset below)   postgres 1,7,2,8   k6 3,9,4,10   monitoring + toxiproxy 5,11
#
# Usage:  DRY_RUN=1 ./run.sh                                  (print the plan, run nothing)
#         FAULTS="stall" REPEATS=1 PROBES=0 RESULTS_NAME=b10-trial ./run.sh     (trial, NOT a result)
set -euo pipefail

EXP_ID="B10"
export PYTHONDONTWRITEBYTECODE=1                 # analyze.py loads hook.py; do not leave a __pycache__ in the repo

# ---- 1. Settings (all can be overridden from the environment) ---------------
VARIANTS="${VARIANTS:-unset set}"
FAULTS="${FAULTS:-lat stall}"
REPEATS="${REPEATS:-3}"
POOL="${POOL:-25}"                               # PG_POOL_MAX, fixed, same as B3
SET_CONN_MS="${SET_CONN_MS:-2000}"               # variant "set": PG_CONN_TIMEOUT_MS
SET_STMT_MS="${SET_STMT_MS:-5000}"               # variant "set": PG_STATEMENT_TIMEOUT_MS
PRE_VUS="${PRE_VUS:-3000}"                       # k6 VUs created BEFORE the run: hung requests need rate x timeout of them at once (lat 2,700), and k6 creating them late drops iterations (seen in the trial)
REQ_TIMEOUT="${REQ_TIMEOUT:-15s}"                # k6 client deadline; a request without an answer by then = "hung"
# fault lat: offered rate is above the pool capacity 25 / (5 ms + 200 ms) = about 122 req/s
LAT_MS="${LAT_MS:-200}"; LAT_RATE="${LAT_RATE:-180}"; LAT_DURATION="${LAT_DURATION:-60}"; PAD_LAT="${PAD_LAT:-300}"
# fault stall: the lock is taken STALL_AT seconds after k6 starts and held STALL_S seconds
STALL_RATE="${STALL_RATE:-100}"; STALL_DURATION="${STALL_DURATION:-80}"; STALL_AT="${STALL_AT:-15}"; STALL_S="${STALL_S:-35}"
PAD_STALL="${PAD_STALL:-20000}"                  # 20 KB filler: a queued request is heavy, so memory growth is visible
DRAIN_MAX_S="${DRAIN_MAX_S:-180}"                # how long to wait for the server to finish queued work after k6
PROBES="${PROBES:-1}"
RESULTS_NAME="${RESULTS_NAME:-b10}"
BASE_URL="${BASE_URL:-http://localhost:8000}"
PROM_URL="${PROM_URL:-http://localhost:9090}"
TOXI_URL="${TOXI_URL:-http://localhost:8474}"
TOXI_PORT="${TOXI_PORT:-15434}"
TOXI_UPSTREAM="${TOXI_UPSTREAM:-postgres:5432}"
MANAGE_SERVER="${MANAGE_SERVER:-1}"
SERVER_CPUS="${SERVER_CPUS:-0,6}"
export K6_CPUS="${K6_CPUS:-3,9,4,10}"            # runner.sh reads this (taskset for k6)
PG_CONTAINER="${PG_CONTAINER:-}"                 # empty = auto-detect
PGUSER="${PGUSER:-eventpulse}"; PGDB="${PGDB:-eventpulse}"
DRY_RUN="${DRY_RUN:-0}"
OVERHEAD_S=45                                    # restart + warm-up + truncate + settle per run (rough)
DRAIN_GUESS_S=30                                 # rough time to drain a queue (only for the printed estimate)

# ---- 2. Paths ----------------------------------------------------------------
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"      # loadtests/k6/stage-1/B10
STAGE_DIR="$(dirname "$HERE")"
RUNNER_DIR="$(dirname "$STAGE_DIR")"                       # loadtests/k6 (runner.sh lives here)
REPO="$(cd "$RUNNER_DIR/../.." && pwd)"
API_DIR="$REPO/services/ingest-api"
ANALYZE="$RUNNER_DIR/analyze.py"
OUT="$HERE/results/$RESULTS_NAME"
CAP="$OUT/captures"; RUNS_TSV="$OUT/runs.tsv"; SUMMARY="$OUT/summary.tsv"; FAULTS_TSV="$OUT/faults.tsv"

die() { echo "ERROR: $*" >&2; exit 1; }
rate_of()     { case "$1" in lat) echo "$LAT_RATE";; *) echo "$STALL_RATE";; esac; }
duration_of() { case "$1" in lat) echo "$LAT_DURATION";; *) echo "$STALL_DURATION";; esac; }
pad_of()      { case "$1" in lat) echo "$PAD_LAT";; *) echo "$PAD_STALL";; esac; }
# variant order for repeat number $1: odd = as listed, even = reversed
variants_for_rep() { if [ $(( $1 % 2 )) -eq 1 ]; then echo "$VARIANTS"; else echo "$VARIANTS" | tr ' ' '\n' | tac | tr '\n' ' '; fi; }
conn_of() { [ "$1" = set ] && echo "$SET_CONN_MS" || echo 0; }
stmt_of() { [ "$1" = set ] && echo "$SET_STMT_MS" || echo 0; }

# ---- 3. Plan / dry run ---------------------------------------------------------
n_var=$(wc -w <<<"$VARIANTS"); total_s=0
for f in $FAULTS; do total_s=$(( total_s + n_var*REPEATS*( $(duration_of "$f") + OVERHEAD_S + DRAIN_GUESS_S ) )); done
[ "$PROBES" = 1 ] && total_s=$(( total_s + STALL_DURATION + OVERHEAD_S + DRAIN_GUESS_S ))
n_runs=$(( $(wc -w <<<"$FAULTS") * n_var * REPEATS ))
echo "== $EXP_ID: $n_runs measured runs$([ "$PROBES" = 1 ] && echo " + 1 probe run"), results in $OUT =="
echo "== variants [$VARIANTS] | faults [$FAULTS] | repeats $REPEATS | pool $POOL | expected total about $(( total_s/60 )) min =="
if [ "$DRY_RUN" = "1" ]; then
  echo "  variant unset: PG_CONN_TIMEOUT_MS=0 PG_STATEMENT_TIMEOUT_MS=0   variant set: PG_CONN_TIMEOUT_MS=$SET_CONN_MS PG_STATEMENT_TIMEOUT_MS=$SET_STMT_MS"
  for rep in $(seq 1 "$REPEATS"); do echo "  repeat $rep variant order: $(variants_for_rep "$rep")"; done
  for f in $FAULTS; do echo "  fault $f: $(rate_of "$f") req/s for $(duration_of "$f") s, payload filler $(pad_of "$f") B, k6 timeout $REQ_TIMEOUT"; done
  echo "  fault lat: Toxiproxy latency ${LAT_MS} ms downstream, whole run. fault stall: LOCK TABLE events (ACCESS EXCLUSIVE) at +${STALL_AT} s for ${STALL_S} s"
  [ "$PROBES" = 1 ] && echo "  probe: stall + unset with heavy tools in the middle of the stall"
  echo "  CPU plan (from compose, not changed here): server=$SERVER_CPUS k6=$K6_CPUS"
  exit 0
fi

# ---- 4. Helpers ------------------------------------------------------------------
pgq()  { docker exec -i "$PG_CONTAINER" psql -U "$PGUSER" -d "$PGDB" -X -At -c "$1"; }   # -X no psqlrc, -A -t plain values
pgqt() { docker exec -i "$PG_CONTAINER" psql -U "$PGUSER" -d "$PGDB" -X -c "$1"; }       # pretty table for captures

preflight() {
  for c in k6 taskset curl docker python3 tac; do command -v "$c" >/dev/null || die "'$c' not found in PATH"; done
  [ -x "$RUNNER_DIR/runner.sh" ] || die "$RUNNER_DIR/runner.sh missing or not executable"
  [ -f "$ANALYZE" ] || die "$ANALYZE missing"
  curl -fsS --max-time 3 "$PROM_URL/-/ready" >/dev/null || die "Prometheus not ready at $PROM_URL (docker compose -f infra/compose/docker-compose.yml up -d)"
  curl -fsS --max-time 3 "$PROM_URL/api/v1/query?query=up%7Bjob%3D%22node%22%7D" | grep -q '"value":\[[^]]*"1"\]' \
    || echo "WARN: node_exporter (job=node) is not 'up'; host panels will be empty" >&2
  curl -fsS --max-time 3 "$TOXI_URL/version" >/dev/null \
    || die "Toxiproxy API not reachable at $TOXI_URL (docker compose -f infra/compose/docker-compose.yml up -d toxiproxy)"
  if [ -z "$PG_CONTAINER" ]; then
    local found; found=$(docker ps --format '{{.Names}} {{.Image}}' | awk '$2 ~ /^postgres:/ {print $1}')
    [ "$(wc -w <<<"$found")" = 1 ] || die "found [$found] postgres containers; set PG_CONTAINER=<name>"
    PG_CONTAINER="$found"
  fi
  pgq "select count(*) from events" >/dev/null || die "cannot query table 'events' in $PG_CONTAINER (migrations? PGUSER/PGDB?)"
  [ -f "$API_DIR/dist/main.js" ] || die "$API_DIR/dist/main.js missing: run 'pnpm build' in services/ingest-api"
  grep -q 'statement_timeout_ms' "$API_DIR/dist/observability/metrics.service.js" 2>/dev/null \
    || die "dist/ does not report the pool timeouts in /debug/stats (B10 change): rebuild ingest-api (pnpm build)"
  [ -f "$API_DIR/.env" ] || die "$API_DIR/.env missing"
  if [ -z "${API_KEY:-}" ]; then      # first key in API_KEYS=key:tenant,key2:tenant2
    API_KEY=$(grep -E '^API_KEYS=' "$API_DIR/.env" | head -1 | cut -d= -f2- | tr -d '"' | cut -d, -f1 | cut -d: -f1 || true)
    [ -n "$API_KEY" ] || die "API_KEY not set and not found in $API_DIR/.env"
  fi
  export API_KEY
  DIRECT_URL=$(grep -E '^DATABASE_URL=' "$API_DIR/.env" | head -1 | cut -d= -f2- | tr -d '"')
  [ -n "$DIRECT_URL" ] || die "DATABASE_URL not found in $API_DIR/.env"
  PROXY_URL=$(sed -E "s#@[^/]+/#@localhost:${TOXI_PORT}/#" <<<"$DIRECT_URL")    # same user/password/db, other host:port
  echo "postgres container: $PG_CONTAINER | server connects via localhost:$TOXI_PORT (Toxiproxy)"
}

# ---- 5. Toxiproxy (only the fault "lat" uses a toxic) -----------------------------
toxi() { curl -fsS --max-time 5 "$@"; }
setup_proxy() {
  curl -sS --max-time 5 -o /dev/null -X POST "$TOXI_URL/proxies" -H 'Content-Type: application/json' \
    -d "{\"name\":\"pg\",\"listen\":\"0.0.0.0:${TOXI_PORT}\",\"upstream\":\"${TOXI_UPSTREAM}\",\"enabled\":true}" || true   # 409 = exists, fine
  toxi "$TOXI_URL/proxies/pg" >/dev/null || die "proxy 'pg' does not exist and could not be created"
  clear_toxic
}
clear_toxic() { curl -sS --max-time 5 -o /dev/null -X DELETE "$TOXI_URL/proxies/pg/toxics/lat" || true; }
set_latency() {   # latency toxic on the DOWNSTREAM direction (database -> API): every answer is delayed
  clear_toxic
  toxi -X POST "$TOXI_URL/proxies/pg/toxics" -H 'Content-Type: application/json' \
    -d "{\"name\":\"lat\",\"type\":\"latency\",\"stream\":\"downstream\",\"toxicity\":1.0,\"attributes\":{\"latency\":$1,\"jitter\":0}}" >/dev/null \
    || die "could not add the latency toxic"
}
check_latency() {   # one real request must take at least the added latency, otherwise the server is not behind the proxy
  local ms
  ms=$(curl -s -o /dev/null -w '%{time_total}' -X POST "$BASE_URL/v1/events" -H "Content-Type: application/json" -H "Authorization: Bearer $API_KEY" \
    -d "{\"event_id\":\"$(cat /proc/sys/kernel/random/uuid)\",\"type\":\"selfcheck\",\"occurred_at\":\"$(date -u +%Y-%m-%dT%H:%M:%S.000Z)\",\"schema_version\":1,\"payload\":{\"w\":1}}" \
    | awk '{printf "%d", $1*1000}')
  [ "$ms" -ge "$(( $1 * 9 / 10 ))" ] || die "one request took ${ms} ms with $1 ms added: the server is not going through the proxy (see $OUT/server.out)"
  echo "   self-check: one request took ${ms} ms with $1 ms added"
}

# ---- 6. The stall: a table lock held by a separate session ------------------------
# BEGIN + LOCK TABLE ... ACCESS EXCLUSIVE blocks every INSERT/SELECT on "events" until COMMIT. pg_sleep keeps the
# transaction open for STALL_S seconds. The lock disappears by itself when the sleep ends (or if the session dies).
STALL_PID_FILE="$OUT/stall.pid"
hold_lock() {   # hold_lock <testid>  (blocks for STALL_S seconds; run it in the background)
  local s e; s=$(date +%s.%N)
  printf 'BEGIN;\nLOCK TABLE events IN ACCESS EXCLUSIVE MODE;\nSELECT pg_sleep(%s);\nCOMMIT;\n' "$STALL_S" \
    | docker exec -i "$PG_CONTAINER" psql -U "$PGUSER" -d "$PGDB" -X -q >/dev/null 2>&1 || true
  e=$(date +%s.%N)
  printf '%s\tstall\t%s\t%s\n' "$1" "$s" "$e" >>"$FAULTS_TSV"
}
release_lock() {   # safety net for the EXIT trap: end any session that is still sleeping with the lock
  [ -n "${PG_CONTAINER:-}" ] || return 0
  pgq "select pg_terminate_backend(pid) from pg_stat_activity where query like '%LOCK TABLE events%' and pid<>pg_backend_pid()" >/dev/null 2>&1 || true
}

# ---- 7. Server lifecycle (ingest-api) --------------------------------------------
SERVER_PID=""
start_server() {   # start_server <database_url> <conn_timeout_ms> <statement_timeout_ms> (0 = no deadline). Env vars beat --env-file.
  [ "$MANAGE_SERVER" = 1 ] || return 0
  stop_server
  ( cd "$API_DIR" && exec env DATABASE_URL="$1" PG_POOL_MAX="$POOL" PG_CONN_TIMEOUT_MS="$2" PG_STATEMENT_TIMEOUT_MS="$3" \
      taskset -c "$SERVER_CPUS" node --env-file=.env dist/main.js ) >"$OUT/server.out" 2>&1 &
  SERVER_PID=$!       # exec keeps the pid: this is the node pid
  for _ in $(seq 1 30); do curl -fsS --max-time 1 "$BASE_URL/healthz" >/dev/null 2>&1 && break; sleep 1; done
  curl -fsS --max-time 2 "$BASE_URL/readyz" >/dev/null || die "ingest-api not ready (see $OUT/server.out)"
  local got; got=$(curl -fsS --max-time 2 "$BASE_URL/debug/stats" | python3 -c 'import sys,json; p=json.load(sys.stdin)["pool"]; print(p["max"],p["conn_timeout_ms"],p["statement_timeout_ms"])' 2>/dev/null || echo "?")
  [ "$got" = "$POOL $2 $3" ] || die "server reports (pool, conn timeout, statement timeout) = '$got', I asked for '$POOL $2 $3' (see $OUT/server.out)"
}
stop_server() {
  [ "$MANAGE_SERVER" = 1 ] || return 0
  if [ -n "${SERVER_PID:-}" ] && kill -0 "$SERVER_PID" 2>/dev/null; then kill "$SERVER_PID" 2>/dev/null || true; wait "$SERVER_PID" 2>/dev/null || true; fi
  SERVER_PID=""
}

EXPORTER_PID=""
cleanup() { stop_server; clear_toxic; release_lock; [ -n "$EXPORTER_PID" ] && kill "$EXPORTER_PID" 2>/dev/null || true; }
trap cleanup EXIT

# ---- 8. Per-run helpers ---------------------------------------------------------
wait_db_quiet() {   # wait until no other backend is working
  local n waited=0
  while :; do
    n=$(pgq "select count(*) from pg_stat_activity where datname='$PGDB' and state<>'idle' and pid<>pg_backend_pid()")
    [ "${n:-0}" -eq 0 ] && return 0
    [ "$waited" -ge 120 ] && { echo "WARN: $n backends still active after 120 s, continuing"; return 0; }
    echo "  waiting for postgres to go quiet ($n active)..."; sleep 3; waited=$((waited+3))
  done
}
reset_db() {        # empty table so growth of earlier runs cannot contaminate this one
  pgq "TRUNCATE events" >/dev/null
  pgq "SELECT pg_stat_statements_reset()" >/dev/null 2>&1 || echo "WARN: pg_stat_statements_reset failed (extension missing?)" >&2
  pgq "CHECKPOINT" >/dev/null
}
warmup() {          # 400 requests, 40 at a time: opens (almost) all pool connections BEFORE the fault; truncated afterwards
  seq 1 400 | xargs -P 40 -I{} sh -c 'curl -s -o /dev/null -X POST "'"$BASE_URL"'/v1/events" \
    -H "Content-Type: application/json" -H "Authorization: Bearer $API_KEY" \
    -d "{\"event_id\":\"$(cat /proc/sys/kernel/random/uuid)\",\"type\":\"warmup\",\"occurred_at\":\"$(date -u +%Y-%m-%dT%H:%M:%S.000Z)\",\"schema_version\":1,\"payload\":{\"w\":1}}"'
}
# Seconds after k6 finished until the server has nothing queued and nothing running. In the "unset" variant the
# server still works through requests whose clients already left; this is how long that takes.
wait_server_drained() {
  local t0 now n
  t0=$(date +%s)
  while :; do
    n=$(curl -fsS --max-time 2 "$BASE_URL/debug/stats" 2>/dev/null | python3 -c 'import sys,json; d=json.load(sys.stdin); p=d["pool"]; print(p["waiting"]+(p["total"]-p["idle"]))' 2>/dev/null || echo 1)
    [ "$n" -eq 0 ] && break
    now=$(date +%s); [ $(( now - t0 )) -ge "$DRAIN_MAX_S" ] && { echo "WARN: server not drained after ${DRAIN_MAX_S} s" >&2; break; }
    sleep 1
  done
  DRAIN_S=$(( $(date +%s) - t0 ))
}

CAP_FILE=""
tag() { printf '\n=== exp=%s testid=%s phase=%s ts=%s cmd: %s\n' "$EXP_ID" "$testid" "$1" "$(date -Is)" "$2" >>"$CAP_FILE"; }
light_snapshot() {   # cheap, before and after only. Counters: subtract pre from post.
  tag "$1" "pg_stat_database + /debug/stats"
  pgqt "select xact_commit, xact_rollback, tup_inserted from pg_stat_database where datname='$PGDB'" >>"$CAP_FILE" 2>&1 || true
  curl -fsS --max-time 2 "$BASE_URL/debug/stats" >>"$CAP_FILE" 2>&1 || true; echo >>"$CAP_FILE"
}
err_types() {   # error types of THIS run (the server was restarted for this run, so it starts at 0)
  curl -fsS --max-time 2 "$BASE_URL/metrics" 2>/dev/null | awk '
    /^ep_db_errors_by_type_total\{/ { match($0,/type="[^"]*"/); t=substr($0,RSTART+6,RLENGTH-7); n=$NF; if (n>0) printf "%s%s=%d", (c++?";":""), t, n }'
}
# Heavy tools: ONLY in the probe run. In the middle of the stall: what are the Postgres backends doing?
heavy_probe() {
  tag probe-mid "pg_stat_activity by state and wait event (Lock = waiting for the table lock held by the stall session)"
  pgqt "select state, wait_event_type, wait_event, count(*) from pg_stat_activity where datname='$PGDB' group by 1,2,3 order by 4 desc" >>"$CAP_FILE" 2>&1 || true
  tag probe-mid "ungranted locks"; pgqt "select locktype, mode, granted, count(*) from pg_locks group by 1,2,3 order by 4 desc" >>"$CAP_FILE" 2>&1 || true
  tag probe-mid "/debug/stats"; curl -fsS --max-time 2 "$BASE_URL/debug/stats" >>"$CAP_FILE" 2>&1 || true; echo >>"$CAP_FILE"
  tag probe-mid "docker stats (postgres)"; docker stats --no-stream "$PG_CONTAINER" >>"$CAP_FILE" 2>&1 || true
  tag probe-mid "vmstat 1 5";   vmstat 1 5 >>"$CAP_FILE" 2>&1 || true
  tag probe-mid "pidstat -u -r -p $SERVER_PID 1 5 (cpu and memory of the node process)"
  command -v pidstat >/dev/null && [ -n "$SERVER_PID" ] && pidstat -u -r -p "$SERVER_PID" 1 5 >>"$CAP_FILE" 2>&1 || true
}

run_k6() {   # run_k6 <rate> <duration_s> <testid> <phase> <fault> <variant> <pad>
  ( cd "$RUNNER_DIR" && env RESULTS_ROOT="$HERE/results" RESULTS_NAME="$RESULTS_NAME" LABEL="$3" SCRIPT="stage-1/B10/script.js" \
      RATE="$1" DURATION="${2}s" PHASE="$4" FAULT="$5" VARIANT="$6" PAD="$7" REQ_TIMEOUT="$REQ_TIMEOUT" PRE_VUS="$PRE_VUS" POOL_MAX="$POOL" \
      API_KEY="$API_KEY" BASE_URL="$BASE_URL" ROUTE="/healthz" ./runner.sh )
}

# ONE run. one_run <phase> <fault> <variant> <rep>   (phase = measured | probe_stall)
one_run() {
  local phase="$1" fault="$2" variant="$3" rep="$4"
  local rate dur pad conn stmt; rate=$(rate_of "$fault"); dur=$(duration_of "$fault"); pad=$(pad_of "$fault"); conn=$(conn_of "$variant"); stmt=$(stmt_of "$variant")
  testid="b10-${fault}-${variant}-n${rep}"; [ "$phase" = probe_stall ] && testid="b10-probe-stall-${variant}"
  local tphase="$fault"; [ "$phase" != measured ] && tphase="$phase"      # k6 tag: lat | stall | probe_stall
  echo; echo "== $tphase: variant=$variant (conn ${conn} ms, stmt ${stmt} ms) rate=$rate dur=${dur}s repeat=$rep ($testid) =="
  CAP_FILE="$CAP/$testid.txt"; : >"$CAP_FILE"
  stop_server; clear_toxic; wait_db_quiet
  start_server "$PROXY_URL" "$conn" "$stmt"
  warmup
  if [ "$fault" = lat ]; then set_latency "$LAT_MS"; check_latency "$LAT_MS"; fi
  wait_db_quiet; reset_db; sleep 5
  light_snapshot pre; local t0 t1 status=ok; t0=$(date +%s)
  run_k6 "$rate" "$dur" "$testid" "$tphase" "$fault" "$variant" "$pad" & local kpid=$!
  if [ "$fault" = stall ]; then
    for _ in $(seq 1 60); do pgrep -x k6 >/dev/null && break; sleep 1; done
    sleep "$STALL_AT"
    hold_lock "$testid" & local lpid=$!
    if [ "$phase" = probe_stall ]; then sleep $(( STALL_S / 2 )); heavy_probe; fi
    wait "$lpid" || true
  fi
  wait "$kpid" || { status=FAILED; echo "!! $testid failed: see its manifest.md"; }
  t1=$(date +%s)
  clear_toxic
  wait_server_drained; echo "   server drained $DRAIN_S s after k6 finished"
  wait_db_quiet
  local rows; rows=$(pgq "select count(*) from events"); local et; et=$(err_types || true)
  light_snapshot post; tag post "error types from the server: ${et:-none}; drain_s=$DRAIN_S"
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$testid" "$tphase" "$variant" "$rate" "$t0" "$t1" "$status" "$DRAIN_S" >>"$RUNS_TSV"
  local js; js=$(ls -t "$OUT"/*/"${testid}_1-lap.json" 2>/dev/null | head -1 || true)
  if [ -n "$js" ]; then
    B10_START_EPOCH="$t0" B10_END_EPOCH="$t1" B10_ROWS="$rows" B10_DRAIN_S="$DRAIN_S" PROM_URL="$PROM_URL" \
    python3 "$ANALYZE" row --json "$js" --testid "$testid" --phase "$tphase" --rate "$rate" --prefix b10 \
      --var "variant=$variant" --var "pool=$POOL" --extra "err_types=${et:-none}" \
      --hook "$HERE/hook.py" --out "$SUMMARY"
    echo "   -> row added to $SUMMARY (rows in table after drain: $rows)"   # no --rows audit on purpose: stale rows ARE the finding, the hook reports them
  else
    echo "   !! no summary json for $testid (k6 failed before finishing?)"
  fi
}

# ---- 9. Main ----------------------------------------------------------------------
preflight
mkdir -p "$CAP"
printf 'testid\tphase\tvariant\trate\tstart_epoch\tend_epoch\tstatus\tdrain_s\n' >"$RUNS_TSV"
printf 'testid\tfault\tstart_epoch\tend_epoch\n' >"$FAULTS_TSV"
setup_proxy

{   # environment snapshot (read only)
  echo "experiment: $EXP_ID   date: $(date -Is)"
  echo "git: $(git -C "$REPO" rev-parse --short HEAD 2>/dev/null || echo n/a), $(git -C "$REPO" status --porcelain 2>/dev/null | wc -l) dirty files"
  echo "kernel: $(uname -r)   node: $(node -v)   k6: $(k6 version | head -1)   cpus: $(nproc)"
  echo "pinning: server=$SERVER_CPUS k6=$K6_CPUS; container cpusets (from compose):"
  for c in "$PG_CONTAINER" ep-toxiproxy ep-prometheus ep-grafana ep-node-exporter ep-postgres-exporter; do
    docker inspect -f '  {{.Name}} cpuset={{.HostConfig.CpusetCpus}} nanocpus={{.HostConfig.NanoCpus}}' "$c" 2>/dev/null || true
  done
  echo "settings: VARIANTS=[$VARIANTS] FAULTS=[$FAULTS] REPEATS=$REPEATS POOL=$POOL PROBES=$PROBES REQ_TIMEOUT=$REQ_TIMEOUT"
  echo "variant set: PG_CONN_TIMEOUT_MS=$SET_CONN_MS PG_STATEMENT_TIMEOUT_MS=$SET_STMT_MS   variant unset: both 0 (no deadline)"
  echo "fault lat: ${LAT_MS} ms latency toxic (downstream), ${LAT_RATE} req/s, ${LAT_DURATION} s, filler ${PAD_LAT} B"
  echo "fault stall: ACCESS EXCLUSIVE lock on events at +${STALL_AT} s for ${STALL_S} s, ${STALL_RATE} req/s, ${STALL_DURATION} s, filler ${PAD_STALL} B"
  echo "toxiproxy: $(curl -fsS --max-time 3 "$TOXI_URL/version" 2>/dev/null)   image: $(docker inspect -f '{{.Config.Image}}' ep-toxiproxy 2>/dev/null)"
  echo "--- postgres ---"
  docker inspect -f 'image={{.Config.Image}} nanocpus={{.HostConfig.NanoCpus}} memory={{.HostConfig.Memory}} cpuset={{.HostConfig.CpusetCpus}}' "$PG_CONTAINER"
  pgq "select version()" || true
  for s in max_connections shared_buffers synchronous_commit fsync lock_timeout statement_timeout idle_in_transaction_session_timeout; do echo "$s = $(pgq "show $s" 2>/dev/null)"; done
  echo "--- cpu model ---"; lscpu | grep -E 'Model name|Thread|Core|Socket' || true
} >"$OUT/env.txt"

# Postgres container CPU/mem/disk -> Prometheus (needs cgroup v2; if it cannot start, those panels stay empty)
PG_CONTAINER="$PG_CONTAINER" bash "$REPO/infra/scripts/pg-container-exporter.sh" 2>"$OUT/pg-exporter.log" &
EXPORTER_PID=$!; sleep 2
kill -0 "$EXPORTER_PID" 2>/dev/null || { echo "WARN: pg-container-exporter did not start ($(tail -1 "$OUT/pg-exporter.log")); Postgres CPU/memory panels will be empty" >&2; EXPORTER_PID=""; }

for rep in $(seq 1 "$REPEATS"); do
  for f in $FAULTS; do
    for v in $(variants_for_rep "$rep"); do one_run measured "$f" "$v" "$rep"; done
  done
done
[ "$PROBES" = 1 ] && one_run probe_stall stall unset 1

echo; python3 "$ANALYZE" table "$SUMMARY"
echo; echo "Done. Zoom Grafana to the times in $RUNS_TSV (epoch seconds). Summary: $SUMMARY  captures: $CAP  env: $OUT/env.txt  lock times: $FAULTS_TSV"
