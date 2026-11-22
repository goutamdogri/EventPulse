#!/usr/bin/env bash
# B9: Retry storm
# ONE command runs the whole experiment:
#     ./run.sh
#
# Idea: clients that retry at once when a request fails send MORE traffic exactly when the server is weakest.
# ingest-api talks to Postgres THROUGH Toxiproxy (container ep-toxiproxy, port 15434), like in B3. Every run is
# one k6 run with three parts, at the same new-event rate the whole time:
#     healthy  PRE_S s    no toxic
#     slow     SLOW_S s   a "latency" toxic adds SLOW_MS to every database answer (the trigger)
#     after    POST_S s   the toxic is deleted. Does the system go back to normal by itself?
# The only thing that changes between runs is the client retry policy (variant):
#     none              no retries (control)
#     immediate         up to 3 retries, no waiting
#     backoff           3 retries, wait 100/200/400 ms with full jitter
#     backoff_nojitter  3 retries, wait 100/200/400 ms, no randomness
#
# Pool is small on purpose (POOL=5): a retrying iteration holds a k6 virtual user for seconds, so the original rate
# has to be modest or k6 itself would run out of VUs (that would be dropped iterations = invalid run).
#
# What the script does (so I can re-read it later):
#   1. preflight    - tools, Prometheus, postgres-exporter, Toxiproxy API, build of ingest-api, Postgres container
#   2. proxy        - creates the proxy "pg" (listen :15434 -> postgres:5432) if missing, clears old toxics
#   3. env snapshot - versions, Postgres settings, cpusets of the containers (read only)
#   4. for every repeat x variant: restart server, warm up, check the toxic really slows one request (then remove it),
#      truncate table, start k6 via runner.sh, wait until traffic flows, sleep PRE_S, add toxic, sleep SLOW_S,
#      delete toxic, wait for k6, row count, summary row (analyze.py + hook.py, which also asks Prometheus for the
#      recovery time)
#   5. probe        - SEPARATE run (never a headline number): variant immediate, but after the "after" part the
#                     generator drops to RELIEF_RATE new events/s for RELIEF_TAIL_S s. Shows whether a stuck system
#                     is freed when load is shed.
#
# Order: repeat is the OUTER loop and the list of variants rotates by one each repeat (drift hits all variants alike).
#
# CPU pinning: NOT done here. Containers have cpusets in the compose file.
#   server 0,6 (taskset below)   postgres 1,7,2,8   k6 3,9,4,10   monitoring + toxiproxy 5,11
#
# Usage:  DRY_RUN=1 ./run.sh                                                        (print the plan, run nothing)
#         VARIANTS="immediate" REPEATS=1 PROBES=0 RESULTS_NAME=b9-trial ./run.sh      (trial, NOT a result)
#         BASE_RATE=700 SLOW_MS=100 ./run.sh                                          (other rate / trigger)
set -euo pipefail

EXP_ID="B9"
export PYTHONDONTWRITEBYTECODE=1                 # analyze.py loads hook.py; do not leave a __pycache__ in the repo

# ---- 1. Settings (all can be overridden from the environment) ---------------
VARIANTS="${VARIANTS:-none immediate backoff backoff_nojitter}"
POOL="${POOL:-5}"                                # PG_POOL_MAX. Healthy capacity is about POOL / 5 ms = 1000 req/s
BASE_RATE="${BASE_RATE:-600}"                    # original rate, new events per second (about 60 % of healthy capacity)
SLOW_MS="${SLOW_MS:-50}"                         # latency added by the toxic (capacity drops to about POOL / 55 ms = 90 req/s)
PRE_S="${PRE_S:-30}"                             # healthy part
SLOW_S="${SLOW_S:-20}"                           # slow part (the trigger)
POST_S="${POST_S:-120}"                          # after the toxic is removed
REPEATS="${REPEATS:-3}"
PROBES="${PROBES:-1}"                            # 1 = also run the relief probe
RELIEF_RATE="${RELIEF_RATE:-$(( BASE_RATE / 10 ))}"
RELIEF_TAIL_S="${RELIEF_TAIL_S:-60}"
REQ_TIMEOUT="${REQ_TIMEOUT:-1s}"                 # per-attempt client timeout (shorter than the server's pool wait)
MAX_RETRIES="${MAX_RETRIES:-3}"
PRE_VUS="${PRE_VUS:-1000}"; MAX_VUS="${MAX_VUS:-8000}"
PG_CONN_TIMEOUT_MS="${PG_CONN_TIMEOUT_MS:-2000}" # server pool wait; set explicitly so .env cannot change it silently
PG_STATEMENT_TIMEOUT_MS="${PG_STATEMENT_TIMEOUT_MS:-5000}"
RESULTS_NAME="${RESULTS_NAME:-b9}"
BASE_URL="${BASE_URL:-http://localhost:8000}"
PROM_URL="${PROM_URL:-http://localhost:9090}"; export PROM_URL
TOXI_URL="${TOXI_URL:-http://localhost:8474}"
TOXI_PORT="${TOXI_PORT:-15434}"                  # proxied port (published by the compose service)
TOXI_UPSTREAM="${TOXI_UPSTREAM:-postgres:5432}"  # as seen from INSIDE the compose network
MANAGE_SERVER="${MANAGE_SERVER:-1}"
SERVER_CPUS="${SERVER_CPUS:-0,6}"
export K6_CPUS="${K6_CPUS:-3,9,4,10}"            # runner.sh reads this (taskset for k6)
PG_CONTAINER="${PG_CONTAINER:-}"                 # empty = auto-detect
PGUSER="${PGUSER:-eventpulse}"; PGDB="${PGDB:-eventpulse}"
DRY_RUN="${DRY_RUN:-0}"
OVERHEAD_S=60                                    # restart + warm-up + truncate + settle + k6 start per run (rough)

# ---- 2. Paths ----------------------------------------------------------------
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"      # loadtests/k6/stage-1/B9
STAGE_DIR="$(dirname "$HERE")"                             # loadtests/k6/stage-1
RUNNER_DIR="$(dirname "$STAGE_DIR")"                       # loadtests/k6  (runner.sh lives here)
REPO="$(cd "$RUNNER_DIR/../.." && pwd)"
API_DIR="$REPO/services/ingest-api"
ANALYZE="$RUNNER_DIR/analyze.py"                   # shared analyzer
OUT="$HERE/results/$RESULTS_NAME"
CAP="$OUT/captures"; RUNS_TSV="$OUT/runs.tsv"; SUMMARY="$OUT/summary.tsv"; EVENTS_TSV="$OUT/events.tsv"

die() { echo "ERROR: $*" >&2; exit 1; }
RUN_S=$(( PRE_S + SLOW_S + POST_S ))
PROBE_S=$(( RUN_S + RELIEF_TAIL_S ))

# variants for repeat number $1: the list rotated left by ($1 - 1)
variants_for_rep() {
  local arr=($VARIANTS) n k i; n=${#arr[@]}; k=$(( ($1 - 1) % n ))
  for i in $(seq 0 $(( n - 1 ))); do printf '%s ' "${arr[$(( (i + k) % n ))]}"; done
}

# ---- 3. Plan / dry run ---------------------------------------------------------
n_var=$(wc -w <<<"$VARIANTS")
total_s=$(( n_var*REPEATS*(RUN_S+OVERHEAD_S) ))
[ "$PROBES" = 1 ] && total_s=$(( total_s + PROBE_S + OVERHEAD_S ))
echo "== $EXP_ID: $((n_var*REPEATS)) measured runs$([ "$PROBES" = 1 ] && echo " + 1 probe run"), results in $OUT =="
echo "== pool $POOL | variants $VARIANTS | base rate $BASE_RATE new events/s | trigger +${SLOW_MS} ms for ${SLOW_S}s after ${PRE_S}s | then ${POST_S}s | repeats $REPEATS | expected total about $(( total_s/60 )) min =="
if [ "$DRY_RUN" = "1" ]; then
  for rep in $(seq 1 "$REPEATS"); do echo "  repeat $rep order: $(variants_for_rep "$rep")"; done
  echo "  per run: ${RUN_S}s of k6 (retries: up to $MAX_RETRIES, attempt timeout $REQ_TIMEOUT, VUs $PRE_VUS..$MAX_VUS)"
  [ "$PROBES" = 1 ] && echo "  probe: variant immediate, after ${RUN_S}s the rate drops to $RELIEF_RATE new events/s for ${RELIEF_TAIL_S}s"
  echo "  server: PG_POOL_MAX=$POOL PG_CONN_TIMEOUT_MS=$PG_CONN_TIMEOUT_MS PG_STATEMENT_TIMEOUT_MS=$PG_STATEMENT_TIMEOUT_MS"
  echo "  CPU plan (from compose, not changed here): server=$SERVER_CPUS k6=$K6_CPUS"
  exit 0
fi

# ---- 4. Helpers ------------------------------------------------------------------
pgq()  { docker exec -i "$PG_CONTAINER" psql -U "$PGUSER" -d "$PGDB" -X -At -c "$1"; }   # one value / rows, no decoration
pgqt() { docker exec -i "$PG_CONTAINER" psql -U "$PGUSER" -d "$PGDB" -X -c "$1"; }       # pretty table for captures

preflight() {
  for c in k6 taskset curl docker python3; do command -v "$c" >/dev/null || die "'$c' not found in PATH"; done
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
  [ -f "$API_DIR/.env" ] || die "$API_DIR/.env missing"
  if [ -z "${API_KEY:-}" ]; then      # first key in API_KEYS=key:tenant,key2:tenant2
    API_KEY=$(grep -E '^API_KEYS=' "$API_DIR/.env" | head -1 | cut -d= -f2- | tr -d '"' | cut -d, -f1 | cut -d: -f1 || true)
    [ -n "$API_KEY" ] || die "API_KEY not set and not found in $API_DIR/.env"
  fi
  export API_KEY
  DIRECT_URL=$(grep -E '^DATABASE_URL=' "$API_DIR/.env" | head -1 | cut -d= -f2- | tr -d '"')
  [ -n "$DIRECT_URL" ] || die "DATABASE_URL not found in $API_DIR/.env"
  PROXY_URL=$(sed -E "s#@[^/]+/#@localhost:${TOXI_PORT}/#" <<<"$DIRECT_URL")    # same user/password/db, other host:port
  echo "postgres container: $PG_CONTAINER | server connects via localhost:$TOXI_PORT (proxy)"
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
# Existing connections are affected too, so it can be added and removed in the middle of a run.
set_latency() {   # set_latency <ms>
  clear_toxic
  toxi -X POST "$TOXI_URL/proxies/pg/toxics" -H 'Content-Type: application/json' \
    -d "{\"name\":\"lat\",\"type\":\"latency\",\"stream\":\"downstream\",\"toxicity\":1.0,\"attributes\":{\"latency\":$1,\"jitter\":0}}" >/dev/null \
    || die "could not add the latency toxic"
}
# One real request must take at least the added latency, otherwise the server is not going through the proxy.
# Done BEFORE k6 starts, then the toxic is removed again (the real trigger comes later, in the middle of the run).
check_latency() {   # check_latency <ms>
  local ms
  ms=$(curl -s -o /dev/null -w '%{time_total}' -X POST "$BASE_URL/v1/events" -H "Content-Type: application/json" -H "Authorization: Bearer $API_KEY" \
    -d "{\"event_id\":\"$(cat /proc/sys/kernel/random/uuid)\",\"type\":\"selfcheck\",\"occurred_at\":\"$(date -u +%Y-%m-%dT%H:%M:%S.000Z)\",\"schema_version\":1,\"payload\":{\"w\":1}}" \
    | awk '{printf "%d", $1*1000}')
  [ "$ms" -ge "$(( $1 * 9 / 10 ))" ] || die "one request took ${ms} ms with $1 ms added: the server is not going through the proxy (see $OUT/server.out)"
  echo "   self-check: one request took ${ms} ms with $1 ms added"
}

# ---- 6. Server lifecycle (ingest-api, same as B3) ---------------------------------
SERVER_PID=""
start_server() {   # env vars beat --env-file, so these win over .env
  [ "$MANAGE_SERVER" = 1 ] || return 0
  stop_server
  ( cd "$API_DIR" && exec env DATABASE_URL="$PROXY_URL" PG_POOL_MAX="$POOL" PG_CONN_TIMEOUT_MS="$PG_CONN_TIMEOUT_MS" \
      PG_STATEMENT_TIMEOUT_MS="$PG_STATEMENT_TIMEOUT_MS" taskset -c "$SERVER_CPUS" node --env-file=.env dist/main.js ) >"$OUT/server.out" 2>&1 &
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
warmup() {          # 400 requests, 40 at a time: opens the pool connections BEFORE measuring; the table is truncated afterwards
  seq 1 400 | xargs -P 40 -I{} sh -c 'curl -s -o /dev/null -X POST "'"$BASE_URL"'/v1/events" \
    -H "Content-Type: application/json" -H "Authorization: Bearer $API_KEY" \
    -d "{\"event_id\":\"$(cat /proc/sys/kernel/random/uuid)\",\"type\":\"warmup\",\"occurred_at\":\"$(date -u +%Y-%m-%dT%H:%M:%S.000Z)\",\"schema_version\":1,\"payload\":{\"w\":1}}"'
}
# number of POST /v1/events the server has seen so far (all statuses); used to notice when k6 really starts sending
events_seen() {
  curl -fsS --max-time 2 "$BASE_URL/metrics" 2>/dev/null | awk '/^http_request_duration_seconds_count/ && /v1\/events/ {s+=$NF} END {printf "%d", s}'
}

CAP_FILE=""
tag() { printf '\n=== exp=%s testid=%s phase=%s ts=%s cmd: %s\n' "$EXP_ID" "$testid" "$1" "$(date -Is)" "$2" >>"$CAP_FILE"; }
# Cheap snapshots: one small query each, before and after the run (never during it). Counters: subtract pre from post.
light_snapshot() {
  tag "$1" "pg_stat_database + /debug/stats"
  pgqt "select xact_commit, xact_rollback, tup_inserted from pg_stat_database where datname='$PGDB'" >>"$CAP_FILE" 2>&1 || true
  curl -fsS --max-time 2 "$BASE_URL/debug/stats" >>"$CAP_FILE" 2>&1 || true; echo >>"$CAP_FILE"
}
# Error types of THIS run, from the server counter (the server was restarted for this run, so it starts at 0).
err_types() {
  curl -fsS --max-time 2 "$BASE_URL/metrics" 2>/dev/null | awk '
    /^ep_db_errors_by_type_total\{/ { match($0,/type="[^"]*"/); t=substr($0,RSTART+6,RLENGTH-7); n=$NF; if (n>0) printf "%s%s=%d", (c++?";":""), t, n }'
}

run_k6() {   # run_k6 <variant> <duration_s> <testid> <phase> <relief_at>
  ( cd "$RUNNER_DIR" && env RESULTS_ROOT="$HERE/results" RESULTS_NAME="$RESULTS_NAME" LABEL="$3" SCRIPT="stage-1/B9/script.js" \
      VARIANT="$1" RATE="$BASE_RATE" DURATION="${2}s" PHASE="$4" RELIEF_AT="$5" RELIEF_RATE="$RELIEF_RATE" MAX_RETRIES="$MAX_RETRIES" \
      REQ_TIMEOUT="$REQ_TIMEOUT" PRE_VUS="$PRE_VUS" MAX_VUS="$MAX_VUS" API_KEY="$API_KEY" BASE_URL="$BASE_URL" ROUTE="/healthz" ./runner.sh )
}

# ONE run. one_run <variant> <phase: measured|probe_relief> <rep>
one_run() {
  local variant="$1" phase="$2" rep="$3" dur="$RUN_S" relief=0
  testid="b9-${variant//_/-}-n${rep}"
  if [ "$phase" = probe_relief ]; then testid="b9-probe-relief"; dur="$PROBE_S"; relief="$RUN_S"; fi
  echo; echo "== $phase: variant=$variant base=$BASE_RATE/s repeat=$rep ($testid, ${dur}s) =="
  CAP_FILE="$CAP/$testid.txt"; : >"$CAP_FILE"
  stop_server; clear_toxic; wait_db_quiet
  start_server
  warmup; set_latency "$SLOW_MS"; check_latency "$SLOW_MS"; clear_toxic
  wait_db_quiet; reset_db
  sleep 5                                    # let the reset/checkpoint I/O finish before measuring
  light_snapshot pre
  local n0 t0 t_add t_rem t_end status=ok
  n0=$(events_seen)
  run_k6 "$variant" "$dur" "$testid" "$phase" "$relief" & local kpid=$!
  # k6 needs some seconds to start its virtual users: wait until the server really sees new requests
  for _ in $(seq 1 120); do [ "$(( $(events_seen) - n0 ))" -gt 50 ] && break; sleep 1; done
  t0=$(date +%s)
  sleep "$PRE_S"
  set_latency "$SLOW_MS"; t_add=$(date +%s.%N); tag trigger "toxic latency ${SLOW_MS} ms added at epoch $t_add"
  sleep "$SLOW_S"
  clear_toxic; t_rem=$(date +%s.%N); tag trigger "toxic deleted at epoch $t_rem"
  wait "$kpid" || { status=FAILED; echo "!! $testid failed: see its manifest.md"; }
  t_end=$(date +%s.%N)
  printf '%s\t%s\t%s\t%s\t%s\n' "$testid" "$t0" "$t_add" "$t_rem" "$t_end" >>"$EVENTS_TSV"
  sleep 3; wait_db_quiet                      # let requests that were still running finish before counting rows
  local rows et; rows=$(pgq "select count(*) from events"); et=$(err_types || true)
  light_snapshot post; tag post "error types from the server: ${et:-none}"
  printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$testid" "$phase" "$variant" "$t0" "${t_end%.*}" "$status" >>"$RUNS_TSV"
  clear_toxic
  local js; js=$(ls -t "$OUT"/*/"${testid}_1-lap.json" 2>/dev/null | head -1 || true)
  if [ -n "$js" ]; then
    B9_BASE_RATE="$BASE_RATE" B9_ADDED_EPOCH="$t_add" B9_REMOVED_EPOCH="$t_rem" B9_END_EPOCH="$t_end" B9_ROWS="$rows" \
    python3 "$ANALYZE" row --json "$js" --testid "$testid" --phase "$phase" --rate "$BASE_RATE" --prefix b9 \
      --var "variant=$variant" --var "pool=$POOL" --extra "err_types=${et:-none}" \
      --hook "$HERE/hook.py" --out "$SUMMARY"
    echo "   -> $(tail -1 "$SUMMARY" | cut -f1-30)"
  else
    echo "   !! no summary json for $testid (k6 failed before finishing?)"
  fi
}

# ---- 8. Main ----------------------------------------------------------------------
preflight
mkdir -p "$CAP"
printf 'testid\tphase\tvariant\tstart_epoch\tend_epoch\tstatus\n' >"$RUNS_TSV"
printf 'testid\ttraffic_start_epoch\ttoxic_added_epoch\ttoxic_removed_epoch\tk6_end_epoch\n' >"$EVENTS_TSV"
setup_proxy

{   # environment snapshot (read only)
  echo "experiment: $EXP_ID   date: $(date -Is)"
  echo "git: $(git -C "$REPO" rev-parse --short HEAD 2>/dev/null || echo n/a), $(git -C "$REPO" status --porcelain 2>/dev/null | wc -l) dirty files"
  echo "kernel: $(uname -r)   node: $(node -v)   k6: $(k6 version | head -1)   cpus: $(nproc)"
  echo "pinning: server=$SERVER_CPUS k6=$K6_CPUS; container cpusets (from compose):"
  for c in "$PG_CONTAINER" ep-toxiproxy ep-prometheus ep-grafana ep-node-exporter ep-postgres-exporter; do
    docker inspect -f '  {{.Name}} cpuset={{.HostConfig.CpusetCpus}} nanocpus={{.HostConfig.NanoCpus}}' "$c" 2>/dev/null || true
  done
  echo "settings: POOL=$POOL VARIANTS=[$VARIANTS] BASE_RATE=$BASE_RATE SLOW_MS=$SLOW_MS PRE_S=$PRE_S SLOW_S=$SLOW_S POST_S=$POST_S REPEATS=$REPEATS PROBES=$PROBES"
  echo "client: MAX_RETRIES=$MAX_RETRIES REQ_TIMEOUT=$REQ_TIMEOUT PRE_VUS=$PRE_VUS MAX_VUS=$MAX_VUS RELIEF_RATE=$RELIEF_RATE RELIEF_TAIL_S=$RELIEF_TAIL_S"
  echo "server: PG_CONN_TIMEOUT_MS=$PG_CONN_TIMEOUT_MS PG_STATEMENT_TIMEOUT_MS=$PG_STATEMENT_TIMEOUT_MS (set by run.sh, they win over .env)"
  echo "toxiproxy: $(curl -fsS --max-time 3 "$TOXI_URL/version" 2>/dev/null)   image: $(docker inspect -f '{{.Config.Image}}' ep-toxiproxy 2>/dev/null)"
  echo "toxic: type=latency stream=downstream (database -> API), jitter 0, added/removed in the middle of the k6 run"
  echo "--- postgres ---"
  docker inspect -f 'image={{.Config.Image}} nanocpus={{.HostConfig.NanoCpus}} memory={{.HostConfig.Memory}} cpuset={{.HostConfig.CpusetCpus}}' "$PG_CONTAINER"
  pgq "select version()" || true
  for s in max_connections shared_buffers synchronous_commit fsync wal_level; do echo "$s = $(pgq "show $s" 2>/dev/null)"; done
  echo "--- cpu model ---"; lscpu | grep -E 'Model name|Thread|Core|Socket' || true
} >"$OUT/env.txt"

# Postgres container CPU/mem/disk -> Prometheus (needs cgroup v2; if it cannot start, those panels stay empty)
PG_CONTAINER="$PG_CONTAINER" bash "$REPO/infra/scripts/pg-container-exporter.sh" 2>"$OUT/pg-exporter.log" &
EXPORTER_PID=$!; sleep 2
kill -0 "$EXPORTER_PID" 2>/dev/null || { echo "WARN: pg-container-exporter did not start ($(tail -1 "$OUT/pg-exporter.log")); Postgres CPU/memory panels will be empty" >&2; EXPORTER_PID=""; }

# measured runs: repeat is the OUTER loop, the variant order rotates
for rep in $(seq 1 "$REPEATS"); do
  for v in $(variants_for_rep "$rep"); do
    one_run "$v" measured "$rep"
  done
done
# probe run: separate
if [ "$PROBES" = 1 ]; then
  one_run immediate probe_relief 1
fi

echo; python3 "$ANALYZE" table "$SUMMARY"
echo; echo "Done. Zoom Grafana to the times in $RUNS_TSV and $EVENTS_TSV (epoch seconds). Summary: $SUMMARY  captures: $CAP  env: $OUT/env.txt"
