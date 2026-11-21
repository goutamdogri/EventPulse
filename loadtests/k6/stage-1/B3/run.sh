#!/usr/bin/env bash
# B3: Slow database propagates to the API
# ONE command runs the whole experiment:
#     ./run.sh
#
# Idea: ingest-api talks to Postgres THROUGH Toxiproxy (container ep-toxiproxy, port 15434).
# The only thing I change is a "latency" toxic on the proxy: 0, 20, 50, 200 ms added to every answer
# the database sends back, so every query takes that much longer. The pool stays fixed (PG_POOL_MAX=25).
#
# For every latency level there are TWO measured runs (one per phase):
#   under  about 0.5 x the predicted capacity -> measures latency and Little's Law (nothing queues)
#   over   about 1.2 x the predicted capacity -> measures capacity (accepted 202/s) and the queue
# Predicted capacity = pool / (h0 + latency), h0 about 5 ms (B1/B2), see Explanation.md.
#
# What the script does (so I can re-read it later):
#   1. preflight    - tools, Prometheus, postgres-exporter, Toxiproxy API, build of ingest-api, Postgres container
#   2. proxy        - creates the proxy "pg" (listen :15434 -> postgres:5432) if missing, clears old toxics
#   3. env snapshot - versions, Postgres settings, cpusets of the containers (read only)
#   4. for every repeat x latency x phase: restart server, warm up, add the toxic, check one request
#                     really got slower, truncate table, k6 via runner.sh, snapshots, row audit, summary row
#   5. probes       - SEPARATE runs (never headline numbers):
#        reca / recb  recovery: slow for SLOW_S, then the toxic is deleted, k6 keeps going. Heavy tools run
#                     in the middle of the slow part. The removal time goes to captures and removal.tsv.
#        ctl          control: server connects DIRECTLY to Postgres (:5434), no proxy, to see what the proxy costs
#
# Order: repeat is the OUTER loop. Odd repeats go latency 0 -> 200, even repeats 200 -> 0.
#
# CPU pinning: NOT done here. I start the containers fresh with docker compose (cpusets are in the compose file).
#   server 0,6 (taskset below)   postgres 1,7,2,8   k6 3,9,4,10   monitoring + toxiproxy 5,11
#
# Usage:  DRY_RUN=1 ./run.sh                                          (print the plan, run nothing)
#         LEVELS_MS="50" PHASES="over" REPEATS=1 DURATION=30s PROBES=0 RESULTS_NAME=b3-trial ./run.sh   (trial, NOT a result)
#         OVER_RATES="0:5000 20:1200 50:550 200:150" ./run.sh         (override the offered rates, level:rate)
set -euo pipefail

EXP_ID="B3"
export PYTHONDONTWRITEBYTECODE=1                 # analyze.py loads hook.py; do not leave a __pycache__ in the repo

# ---- 1. Settings (all can be overridden from the environment) ---------------
LEVELS_MS="${LEVELS_MS:-0 20 50 200}"            # added latency in ms (the toxic), level 0 = proxy without toxic
PHASES="${PHASES:-under over}"
POOL="${POOL:-25}"                               # PG_POOL_MAX, fixed in B3
# offered rates, "level:rate" pairs. under = 0.5 x predicted capacity, over = 1.2 x.
UNDER_RATES="${UNDER_RATES:-0:2000 20:500 50:225 200:60}"
OVER_RATES="${OVER_RATES:-0:5000 20:1200 50:550 200:150}"
REPEATS="${REPEATS:-3}"
DURATION="${DURATION:-45s}"
PROBES="${PROBES:-1}"                            # 1 = also run the probe runs
SLOW_S="${SLOW_S:-30}"                           # recovery probes: seconds slow before the toxic is removed
REC_TOTAL_S="${REC_TOTAL_S:-90}"                 # recovery probes: total k6 duration in seconds
RECA_LEVEL="${RECA_LEVEL:-200}"; RECA_RATE="${RECA_RATE:-300}"
RECB_LEVEL="${RECB_LEVEL:-50}";  RECB_RATE="${RECB_RATE:-600}"
# control probe (server connected straight to Postgres, no proxy): same rate and duration as the level-0 "under" runs
# (set after rate_of() below), so "proxy vs no proxy" is compared at the same load
RESULTS_NAME="${RESULTS_NAME:-b3}"
BASE_URL="${BASE_URL:-http://localhost:8000}"
PROM_URL="${PROM_URL:-http://localhost:9090}"
TOXI_URL="${TOXI_URL:-http://localhost:8474}"
TOXI_PORT="${TOXI_PORT:-15434}"                  # proxied port (published by the compose service)
TOXI_UPSTREAM="${TOXI_UPSTREAM:-postgres:5432}"  # as seen from INSIDE the compose network
MANAGE_SERVER="${MANAGE_SERVER:-1}"
SERVER_CPUS="${SERVER_CPUS:-0,6}"
export K6_CPUS="${K6_CPUS:-3,9,4,10}"            # runner.sh reads this (taskset for k6)
PG_CONTAINER="${PG_CONTAINER:-}"                 # empty = auto-detect
PGUSER="${PGUSER:-eventpulse}"; PGDB="${PGDB:-eventpulse}"
DRY_RUN="${DRY_RUN:-0}"
OVERHEAD_S=45                                    # restart + warm-up + truncate + settle per run (rough)

# ---- 2. Paths ----------------------------------------------------------------
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"      # loadtests/k6/stage-1/B3
STAGE_DIR="$(dirname "$HERE")"                             # loadtests/k6/stage-1
RUNNER_DIR="$(dirname "$STAGE_DIR")"                       # loadtests/k6  (runner.sh lives here)
REPO="$(cd "$RUNNER_DIR/../.." && pwd)"
API_DIR="$REPO/services/ingest-api"
ANALYZE="$RUNNER_DIR/analyze.py"                   # shared analyzer
OUT="$HERE/results/$RESULTS_NAME"
CAP="$OUT/captures"; RUNS_TSV="$OUT/runs.tsv"; SUMMARY="$OUT/summary.tsv"; REMOVAL_TSV="$OUT/removal.tsv"

die() { echo "ERROR: $*" >&2; exit 1; }
to_seconds() { case "$1" in *h) echo $(( ${1%h}*3600 ));; *m) echo $(( ${1%m}*60 ));; *s) echo "${1%s}";; *) echo "$1";; esac; }
rate_of() {   # rate_of "<level:rate ...>" <level>
  local p; for p in $1; do [ "${p%%:*}" = "$2" ] && { echo "${p#*:}"; return 0; }; done
  die "no rate for level $2 in '$1'"
}
CTL_RATE="${CTL_RATE:-$(rate_of "$UNDER_RATES" 0)}"; CTL_DURATION="${CTL_DURATION:-$DURATION}"

# order of latency levels for repeat number $1: odd = as listed, even = reversed
levels_for_rep() { if [ $(( $1 % 2 )) -eq 1 ]; then echo "$LEVELS_MS"; else echo "$LEVELS_MS" | tr ' ' '\n' | tac | tr '\n' ' '; fi; }

# ---- 3. Plan / dry run ---------------------------------------------------------
n_levels=$(wc -w <<<"$LEVELS_MS"); n_phases=$(wc -w <<<"$PHASES")
dur_s=$(to_seconds "$DURATION")
total_s=$(( n_levels*n_phases*REPEATS*(dur_s+OVERHEAD_S) ))
[ "$PROBES" = 1 ] && total_s=$(( total_s + 2*(REC_TOTAL_S+OVERHEAD_S+20) + $(to_seconds "$CTL_DURATION")+OVERHEAD_S ))
echo "== $EXP_ID: $((n_levels*n_phases*REPEATS)) measured runs$([ "$PROBES" = 1 ] && echo " + 3 probe runs"), results in $OUT =="
echo "== pool $POOL | levels (ms) $LEVELS_MS | phases $PHASES | duration $DURATION | repeats $REPEATS | expected total about $(( total_s/60 )) min =="
if [ "$DRY_RUN" = "1" ]; then
  for rep in $(seq 1 "$REPEATS"); do echo "  repeat $rep order: $(levels_for_rep "$rep")"; done
  for l in $LEVELS_MS; do echo "  level ${l} ms: under=$(rate_of "$UNDER_RATES" "$l")  over=$(rate_of "$OVER_RATES" "$l") req/s"; done
  [ "$PROBES" = 1 ] && echo "  probes: reca ($RECA_LEVEL ms, $RECA_RATE req/s, slow ${SLOW_S}s of ${REC_TOTAL_S}s), recb ($RECB_LEVEL ms, $RECB_RATE req/s), ctl (no proxy, $CTL_RATE req/s, $CTL_DURATION)"
  echo "  CPU plan (from compose, not changed here): server=$SERVER_CPUS k6=$K6_CPUS"
  exit 0
fi

# ---- 4. Helpers ------------------------------------------------------------------
pgq()  { docker exec -i "$PG_CONTAINER" psql -U "$PGUSER" -d "$PGDB" -X -At -c "$1"; }   # one value / rows, no decoration
pgqt() { docker exec -i "$PG_CONTAINER" psql -U "$PGUSER" -d "$PGDB" -X -c "$1"; }       # pretty table for captures

preflight() {
  for c in k6 taskset curl docker python3 tac; do command -v "$c" >/dev/null || die "'$c' not found in PATH"; done
  command -v iostat >/dev/null || echo "WARN: iostat missing (apt install sysstat); probe runs will skip it" >&2
  [ -x "$RUNNER_DIR/runner.sh" ] || die "$RUNNER_DIR/runner.sh missing or not executable"
  [ -f "$ANALYZE" ] || die "$ANALYZE missing"
  curl -fsS --max-time 3 "$PROM_URL/-/ready" >/dev/null || die "Prometheus not ready at $PROM_URL (docker compose -f infra/compose/docker-compose.yml up -d)"
  curl -fsS --max-time 3 "$PROM_URL/api/v1/query?query=up%7Bjob%3D%22postgres%22%7D" | grep -q '"value":\[[^]]*"1"\]' \
    || die "postgres-exporter (job=postgres) is not 'up' (docker compose -f infra/compose/docker-compose.yml up -d postgres-exporter)"
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
  grep -q 'ep_db_errors_by_type_total' "$API_DIR/dist/"*.js "$API_DIR/dist/"*/*.js 2>/dev/null \
    || die "dist/ has no ep_db_errors_by_type_total: rebuild ingest-api (pnpm build)"
  [ -f "$API_DIR/.env" ] || die "$API_DIR/.env missing"
  if [ -z "${API_KEY:-}" ]; then      # first key in API_KEYS=key:tenant,key2:tenant2
    API_KEY=$(grep -E '^API_KEYS=' "$API_DIR/.env" | head -1 | cut -d= -f2- | tr -d '"' | cut -d, -f1 | cut -d: -f1 || true)
    [ -n "$API_KEY" ] || die "API_KEY not set and not found in $API_DIR/.env"
  fi
  export API_KEY
  DIRECT_URL=$(grep -E '^DATABASE_URL=' "$API_DIR/.env" | head -1 | cut -d= -f2- | tr -d '"')
  [ -n "$DIRECT_URL" ] || die "DATABASE_URL not found in $API_DIR/.env"
  PROXY_URL=$(sed -E "s#@[^/]+/#@localhost:${TOXI_PORT}/#" <<<"$DIRECT_URL")    # same user/password/db, other host:port
  echo "postgres container: $PG_CONTAINER | server connects via localhost:$TOXI_PORT (proxy) or the original URL (ctl probe)"
}

# ---- 5. Toxiproxy ---------------------------------------------------------------
toxi() { curl -fsS --max-time 5 "$@"; }
setup_proxy() {
  # create the proxy if it does not exist yet (409 = exists, fine), then delete every old toxic
  curl -sS --max-time 5 -o /dev/null -X POST "$TOXI_URL/proxies" -H 'Content-Type: application/json' \
    -d "{\"name\":\"pg\",\"listen\":\"0.0.0.0:${TOXI_PORT}\",\"upstream\":\"${TOXI_UPSTREAM}\",\"enabled\":true}" || true
  toxi "$TOXI_URL/proxies/pg" >/dev/null || die "proxy 'pg' does not exist and could not be created"
  clear_toxic
}
clear_toxic() { curl -sS --max-time 5 -o /dev/null -X DELETE "$TOXI_URL/proxies/pg/toxics/lat" || true; }
# latency toxic on the DOWNSTREAM (database -> API) direction: every answer is delayed, so one query costs one added latency.
# Existing connections are affected too, so I can add it after the warm-up and remove it in the middle of a run.
set_latency() {   # set_latency <ms>   (0 = no toxic)
  clear_toxic
  [ "$1" -eq 0 ] && return 0
  toxi -X POST "$TOXI_URL/proxies/pg/toxics" -H 'Content-Type: application/json' \
    -d "{\"name\":\"lat\",\"type\":\"latency\",\"stream\":\"downstream\",\"toxicity\":1.0,\"attributes\":{\"latency\":$1,\"jitter\":0}}" >/dev/null \
    || die "could not add the latency toxic"
}
# One real request must now take at least the added latency, otherwise the server is not going through the proxy.
check_latency() {   # check_latency <ms>
  [ "$1" -eq 0 ] && return 0
  local ms
  ms=$(curl -s -o /dev/null -w '%{time_total}' -X POST "$BASE_URL/v1/events" -H "Content-Type: application/json" -H "Authorization: Bearer $API_KEY" \
    -d "{\"event_id\":\"$(cat /proc/sys/kernel/random/uuid)\",\"type\":\"selfcheck\",\"occurred_at\":\"$(date -u +%Y-%m-%dT%H:%M:%S.000Z)\",\"schema_version\":1,\"payload\":{\"w\":1}}" \
    | awk '{printf "%d", $1*1000}')
  [ "$ms" -ge "$(( $1 * 9 / 10 ))" ] || die "one request took ${ms} ms with $1 ms added: the server is not going through the proxy (see $OUT/server.out)"
  echo "   self-check: one request took ${ms} ms with $1 ms added"
}

# ---- 6. Server lifecycle (ingest-api, same as B2) ---------------------------------
SERVER_PID=""
start_server() {   # start_server <database_url>   (env vars beat --env-file, so DATABASE_URL/PG_POOL_MAX here win over .env)
  [ "$MANAGE_SERVER" = 1 ] || return 0
  stop_server
  ( cd "$API_DIR" && exec env DATABASE_URL="$1" PG_POOL_MAX="$POOL" taskset -c "$SERVER_CPUS" node --env-file=.env dist/main.js ) >"$OUT/server.out" 2>&1 &
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

EXPORTER_PID=""
cleanup() { stop_server; clear_toxic; [ -n "$EXPORTER_PID" ] && kill "$EXPORTER_PID" 2>/dev/null || true; }
trap cleanup EXIT

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
reset_db() {        # empty table so size/index growth of earlier runs cannot contaminate this one
  pgq "TRUNCATE events" >/dev/null
  pgq "SELECT pg_stat_statements_reset()" >/dev/null 2>&1 || echo "WARN: pg_stat_statements_reset failed (extension missing?)" >&2
  pgq "CHECKPOINT" >/dev/null
}
warmup() {          # 400 requests, 40 at a time: opens (almost) all 25 pool connections BEFORE the latency is added; truncated afterwards
  seq 1 400 | xargs -P 40 -I{} sh -c 'curl -s -o /dev/null -X POST "'"$BASE_URL"'/v1/events" \
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
err_types() {
  curl -fsS --max-time 2 "$BASE_URL/metrics" 2>/dev/null | awk '
    /^ep_db_errors_by_type_total\{/ { match($0,/type="[^"]*"/); t=substr($0,RSTART+6,RLENGTH-7); n=$NF; if (n>0) printf "%s%s=%d", (c++?";":""), t, n }'
}
# Heavy tools: ONLY from probe runs. The interesting question in B3: while the API is slow, is Postgres busy or idle?
heavy_probe() {
  tag probe-mid "pg_stat_activity grouped by state and wait event (idle = the database is waiting for the API, not working)"
  pgqt "select state, wait_event_type, wait_event, count(*) from pg_stat_activity where datname='$PGDB' group by 1,2,3 order by 4 desc" >>"$CAP_FILE" 2>&1 || true
  tag probe-mid "toxics on the proxy"; toxi "$TOXI_URL/proxies/pg/toxics" >>"$CAP_FILE" 2>&1 || true; echo >>"$CAP_FILE"
  tag probe-mid "docker stats (postgres, toxiproxy)"; docker stats --no-stream ep-toxiproxy "$PG_CONTAINER" >>"$CAP_FILE" 2>&1 || true
  tag probe-mid "iostat -x 1 5"; command -v iostat >/dev/null && iostat -x 1 5 >>"$CAP_FILE" 2>&1 || true
  tag probe-mid "vmstat 1 5";   vmstat 1 5 >>"$CAP_FILE" 2>&1 || true
  tag probe-mid "pidstat -u -p $SERVER_PID 1 5"; command -v pidstat >/dev/null && [ -n "$SERVER_PID" ] && pidstat -u -p "$SERVER_PID" 1 5 >>"$CAP_FILE" 2>&1 || true
}

run_k6() {   # run_k6 <rate> <duration> <testid> <phase> <added_ms>
  ( cd "$RUNNER_DIR" && env RESULTS_ROOT="$HERE/results" RESULTS_NAME="$RESULTS_NAME" LABEL="$3" SCRIPT="stage-1/B3/script.js" \
      RATE="$1" DURATION="$2" PHASE="$4" ADDED_MS="$5" POOL_MAX="$POOL" API_KEY="$API_KEY" BASE_URL="$BASE_URL" ROUTE="/healthz" ./runner.sh )
}
record() { printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$testid" "$PHASE" "$LEVEL" "$RATE_NOW" "$1" "$2" "$3" >>"$RUNS_TSV"; }

# ONE run. one_run <phase> <level_ms> <rate> <duration> <rep> <db_mode: proxy|direct>
one_run() {
  PHASE="$1"; LEVEL="$2"; RATE_NOW="$3"; local dur="$4" rep="$5" mode="$6"
  testid="b3-${PHASE}-l${LEVEL}-n${rep}"
  case "$PHASE" in probe_*) testid="b3-${PHASE//_/-}";; esac
  echo; echo "== $PHASE: added=${LEVEL}ms rate=$RATE_NOW repeat=$rep db=$mode ($testid) =="
  CAP_FILE="$CAP/$testid.txt"; : >"$CAP_FILE"
  stop_server; clear_toxic; wait_db_quiet
  if [ "$mode" = direct ]; then start_server "$DIRECT_URL"; else start_server "$PROXY_URL"; fi
  warmup; set_latency "$LEVEL"; check_latency "$LEVEL"; wait_db_quiet; reset_db
  sleep 5                                    # let the reset/checkpoint I/O finish before measuring
  light_snapshot pre; local t0; t0=$(date +%s); local status=ok
  case "$PHASE" in
    probe_rec*)   # slow for SLOW_S, then delete the toxic while k6 keeps sending; heavy tools in the middle of the slow part
      run_k6 "$RATE_NOW" "${dur}s" "$testid" "$PHASE" "$LEVEL" & local kpid=$!
      for _ in $(seq 1 60); do pgrep -x k6 >/dev/null && break; sleep 1; done
      sleep $(( SLOW_S / 2 )); heavy_probe
      local left=$(( SLOW_S - SLOW_S/2 - 15 )); [ "$left" -gt 0 ] && sleep "$left"
      clear_toxic; local tr; tr=$(date +%s.%N)
      printf '%s\t%s\n' "$testid" "$tr" >>"$REMOVAL_TSV"; tag removal "toxic deleted at epoch $tr"
      wait "$kpid" || status=FAILED ;;
    probe_ctl)
      run_k6 "$RATE_NOW" "$dur" "$testid" "$PHASE" "$LEVEL" || status=FAILED ;;
    *)
      run_k6 "$RATE_NOW" "$dur" "$testid" "$PHASE" "$LEVEL" || { status=FAILED; echo "!! $testid failed: see its manifest.md"; } ;;
  esac
  sleep 3; wait_db_quiet                      # let requests that were still running finish before counting rows
  local rows; rows=$(pgq "select count(*) from events")
  local et; et=$(err_types || true)
  light_snapshot post; tag post "error types from the server: ${et:-none}"; record "$t0" "$(date +%s)" "$status"
  clear_toxic
  local js; js=$(ls -t "$OUT"/*/"${testid}_1-lap.json" 2>/dev/null | head -1 || true)
  if [ -n "$js" ]; then
    python3 "$ANALYZE" row --json "$js" --testid "$testid" --phase "$PHASE" --rate "$RATE_NOW" --prefix b3 \
      --var "latency_ms=$LEVEL" --var "pool=$POOL" --rows "$rows" --extra "err_types=${et:-none}" --extra "db=$mode" \
      --hook "$HERE/hook.py" --out "$SUMMARY"
    tail -1 "$SUMMARY" | awk -F'\t' '{printf "   -> latency %s ms: offered %s, accepted %s rps, err %s%%, p50 %s ms, p99 %s ms, dropped %s, audit %s | %s\n",$3,$5,$7,$12,$13,$15,$17,$19,$NF}'
  else
    echo "   !! no summary json for $testid (k6 failed before finishing?)"
  fi
}

# ---- 8. Main ----------------------------------------------------------------------
preflight
mkdir -p "$CAP"
printf 'testid\tphase\tlatency_ms\trate\tstart_epoch\tend_epoch\tstatus\n' >"$RUNS_TSV"
printf 'testid\tremoved_epoch\n' >"$REMOVAL_TSV"
setup_proxy

{   # environment snapshot (read only)
  echo "experiment: $EXP_ID   date: $(date -Is)"
  echo "git: $(git -C "$REPO" rev-parse --short HEAD 2>/dev/null || echo n/a), $(git -C "$REPO" status --porcelain 2>/dev/null | wc -l) dirty files"
  echo "kernel: $(uname -r)   node: $(node -v)   k6: $(k6 version | head -1)   cpus: $(nproc)"
  echo "pinning: server=$SERVER_CPUS k6=$K6_CPUS; container cpusets (from compose):"
  for c in "$PG_CONTAINER" ep-toxiproxy ep-prometheus ep-grafana ep-node-exporter ep-postgres-exporter; do
    docker inspect -f '  {{.Name}} cpuset={{.HostConfig.CpusetCpus}} nanocpus={{.HostConfig.NanoCpus}}' "$c" 2>/dev/null || true
  done
  echo "settings: POOL=$POOL LEVELS_MS=[$LEVELS_MS] PHASES=[$PHASES] REPEATS=$REPEATS DURATION=$DURATION PROBES=$PROBES"
  echo "rates: under=[$UNDER_RATES] over=[$OVER_RATES]  recovery: A=$RECA_LEVEL ms@$RECA_RATE B=$RECB_LEVEL ms@$RECB_RATE slow=${SLOW_S}s total=${REC_TOTAL_S}s  ctl=$CTL_RATE"
  echo "toxiproxy: $(curl -fsS --max-time 3 "$TOXI_URL/version" 2>/dev/null)   image: $(docker inspect -f '{{.Config.Image}}' ep-toxiproxy 2>/dev/null)"
  echo "toxic: type=latency stream=downstream (database -> API), jitter 0, added AFTER the warm-up"
  echo "--- postgres ---"
  docker inspect -f 'image={{.Config.Image}} nanocpus={{.HostConfig.NanoCpus}} memory={{.HostConfig.Memory}} cpuset={{.HostConfig.CpusetCpus}}' "$PG_CONTAINER"
  pgq "select version()" || true
  for s in max_connections superuser_reserved_connections shared_buffers synchronous_commit fsync wal_level; do echo "$s = $(pgq "show $s" 2>/dev/null)"; done
  echo "--- ingest-api pool settings (from services/ingest-api/src/config.ts defaults, not read at run time) ---"
  echo "connectionTimeoutMillis=PG_CONN_TIMEOUT_MS (default 2000)  statement_timeout=PG_STATEMENT_TIMEOUT_MS (default 5000)"
  grep -E '^(PG_CONN_TIMEOUT_MS|PG_STATEMENT_TIMEOUT_MS)=' "$API_DIR/.env" 2>/dev/null || echo "(no PG_*_TIMEOUT overrides in .env)"
  echo "--- cpu model ---"; lscpu | grep -E 'Model name|Thread|Core|Socket' || true
} >"$OUT/env.txt"

# Postgres container CPU/mem/disk -> Prometheus (needs cgroup v2; if it cannot start, those panels stay empty)
PG_CONTAINER="$PG_CONTAINER" bash "$REPO/infra/scripts/pg-container-exporter.sh" 2>"$OUT/pg-exporter.log" &
EXPORTER_PID=$!; sleep 2
kill -0 "$EXPORTER_PID" 2>/dev/null || { echo "WARN: pg-container-exporter did not start ($(tail -1 "$OUT/pg-exporter.log")); Postgres CPU/memory panels will be empty" >&2; EXPORTER_PID=""; }

# measured runs: repeat is the OUTER loop; odd repeats ascending, even repeats descending latency; under before over
for rep in $(seq 1 "$REPEATS"); do
  for lvl in $(levels_for_rep "$rep"); do
    for ph in $PHASES; do
      if [ "$ph" = under ]; then r=$(rate_of "$UNDER_RATES" "$lvl"); else r=$(rate_of "$OVER_RATES" "$lvl"); fi
      one_run "$ph" "$lvl" "$r" "$DURATION" "$rep" proxy
    done
  done
done
# probe runs: separate
if [ "$PROBES" = 1 ]; then
  one_run probe_reca "$RECA_LEVEL" "$RECA_RATE" "$REC_TOTAL_S" 1 proxy
  one_run probe_recb "$RECB_LEVEL" "$RECB_RATE" "$REC_TOTAL_S" 1 proxy
  one_run probe_ctl 0 "$CTL_RATE" "$CTL_DURATION" 1 direct
fi

echo; python3 "$ANALYZE" table "$SUMMARY"
echo; echo "Done. Zoom Grafana to the times in $RUNS_TSV (epoch seconds). Summary: $SUMMARY  captures: $CAP  env: $OUT/env.txt  removal times: $REMOVAL_TSV"
