#!/usr/bin/env bash
# B11: Table growth soak
# ONE command runs the whole experiment:
#     ./run.sh
#
# Idea: a short test starts on an empty table. A long test does not: the table and its indexes grow until they are bigger
# than the memory Postgres uses as cache, and checkpoints keep coming. I send events at a FIXED rate that is about 70 % of
# the capacity (the knee) for a long time and watch if the service gets slower while the table grows.
#
# Four runs, always in this order (PHASES can skip some):
#   cap_start  probe   PROBE_RATE req/s for CAP_DURATION on an EMPTY table. It is above the capacity on purpose, so the
#                      accepted rate shows the capacity (the knee) of the empty table.
#   soak       MEASURED RATE req/s for DURATION on a table that starts empty. This is the experiment.
#   cap_end    probe   the same probe as cap_start, but on the BIG table the soak left behind (no TRUNCATE before it).
#                      If the accepted rate is lower than in cap_start, the capacity moved.
#   fresh      control RATE req/s for FRESH_DURATION on a TRUNCATED table. Same rate as the soak, empty table again. If it is
#                      fast again, the slowdown was caused by the table size and not by something else in the machine.
# A fixed offered rate cannot show a lower capacity by itself (at 70 % there is headroom), that is why the two probes exist.
#
# Every run: restart the server (fresh pool), warm up, (TRUNCATE events unless the run is cap_end), checkpoint, k6.
# During every run a sampler (pinned to the monitoring cores) writes one line per SAMPLE_S seconds to
# captures/<testid>.samples.tsv: table and index size, checkpoint counters, cache hit counters, WAL counters, free disk,
# events inserted and insert time as the server counts them. hook.py turns that file into summary columns.
#
# CPU pinning (same as B3 to B7): server 0,6 (taskset below), postgres 1,7,2,8 (compose), k6 3,9,4,10, monitoring 5,11.
#
# This script changes (all restored or harmless on exit): rows of table events (TRUNCATE before each run except cap_end,
# the soak leaves about 10 million rows behind), and it starts two small exporter scripts that write files into
# infra/compose/textfile (removed on exit). It does NOT change Postgres settings, sysctls, or anything with sudo.
# A guard stops the soak if free disk space falls below MIN_FREE_GB.
#
# Usage:  DRY_RUN=1 ./run.sh                                   (print the plan, run nothing)
#         PHASES="soak" DURATION=3m RESULTS_NAME=b11-trial ./run.sh      (plumbing trial, NOT a result)
#         PHASES="cap_start soak cap_end fresh" DURATION=3h ./run.sh     (the long version: check the disk first, see README)
set -euo pipefail

EXP_ID="B11"
export PYTHONDONTWRITEBYTECODE=1                 # analyze.py loads hook.py; do not leave a __pycache__ in the repo

# ---- 1. Settings (all can be overridden from the environment) ---------------
PHASES="${PHASES:-cap_start soak cap_end fresh}"
RATE="${RATE:-3000}"                             # about 70 % of the capacity (about 4300 req/s with pool 25, B6 and B7)
DURATION="${DURATION:-60m}"                      # the soak
PROBE_RATE="${PROBE_RATE:-6000}"                 # above the capacity: the accepted rate then shows the capacity
CAP_DURATION="${CAP_DURATION:-60s}"
FRESH_DURATION="${FRESH_DURATION:-180s}"
SAMPLE_S="${SAMPLE_S:-60}"                       # sampler period (the experiment block says "every minute")
MIN_FREE_GB="${MIN_FREE_GB:-10}"                 # abort the run if free space on DISK_PATH falls below this
DISK_PATH="${DISK_PATH:-/}"                      # filesystem that holds the docker volume of Postgres
TENANTS="${TENANTS:-100}"
POOL="${POOL:-25}"                               # PG_POOL_MAX, same as B3 to B8
KEY_PREFIX="${KEY_PREFIX:-epk_b11_}"             # api key i = KEY_PREFIX + i, tenant i = t<i>
RESULTS_NAME="${RESULTS_NAME:-b11}"
BASE_URL="${BASE_URL:-http://localhost:8000}"
PROM_URL="${PROM_URL:-http://localhost:9090}"
MANAGE_SERVER="${MANAGE_SERVER:-1}"
SERVER_CPUS="${SERVER_CPUS:-0,6}"
SAMPLER_CPUS="${SAMPLER_CPUS:-5,11}"             # sampler and exporters run on the monitoring cores
export K6_CPUS="${K6_CPUS:-3,9,4,10}"            # runner.sh reads this (taskset for k6)
PG_CONTAINER="${PG_CONTAINER:-}"                 # empty = auto-detect
PGUSER="${PGUSER:-eventpulse}"; PGDB="${PGDB:-eventpulse}"
DRY_RUN="${DRY_RUN:-0}"
OVERHEAD_S=60                                    # server restart + warm-up + truncate + checkpoint + settle + row count (rough)

# ---- 2. Paths ----------------------------------------------------------------
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"      # loadtests/k6/stage-1/B11
STAGE_DIR="$(dirname "$HERE")"                             # loadtests/k6/stage-1
RUNNER_DIR="$(dirname "$STAGE_DIR")"                       # loadtests/k6  (runner.sh lives here)
REPO="$(cd "$RUNNER_DIR/../.." && pwd)"
API_DIR="$REPO/services/ingest-api"
ANALYZE="$RUNNER_DIR/analyze.py"                           # shared analyzer
OUT="$HERE/results/$RESULTS_NAME"
CAP="$OUT/captures"; RUNS_TSV="$OUT/runs.tsv"; SUMMARY="$OUT/summary.tsv"
ANALYZE_FLAGS=(--prefix b11 --ok-code 202 --fail-code 503 --err-limit-pct 100 --p99-limit-ms 100000000)
# The probes run ABOVE the capacity, so errors, huge p99 and dropped iterations are expected there. The analyzer's
# "collapsed" limits are switched off; the verdicts that can still fire are LOST-ROWS and dropped=N.

die() { echo "ERROR: $*" >&2; exit 1; }
to_seconds() { case "$1" in *h) echo $(( ${1%h}*3600 ));; *m) echo $(( ${1%m}*60 ));; *s) echo "${1%s}";; *) echo "$1";; esac; }

# per phase: rate, duration, whether the table is truncated first
phase_rate()  { case "$1" in cap_start|cap_end) echo "$PROBE_RATE";; soak|fresh) echo "$RATE";; *) die "unknown phase '$1'";; esac; }
phase_dur()   { case "$1" in cap_start|cap_end) echo "$CAP_DURATION";; soak) echo "$DURATION";; fresh) echo "$FRESH_DURATION";; esac; }
phase_trunc() { [ "$1" = cap_end ] && echo 0 || echo 1; }
phase_kind()  { case "$1" in soak) echo measured;; *) echo probe;; esac; }

# ---- 3. Plan / dry run ---------------------------------------------------------
total_s=0
for p in $PHASES; do total_s=$(( total_s + $(to_seconds "$(phase_dur "$p")") + OVERHEAD_S )); done
echo "== $EXP_ID: phases [$PHASES], 1 run each (a soak is not repeated), results in $OUT =="
echo "== soak $RATE req/s for $DURATION | probes $PROBE_RATE req/s for $CAP_DURATION | fresh $FRESH_DURATION | sample every ${SAMPLE_S}s | pool $POOL | tenants $TENANTS | expected total about $(( total_s/60 )) min =="
if [ "$DRY_RUN" = "1" ]; then
  for p in $PHASES; do
    echo "  $p ($(phase_kind "$p")): $(phase_rate "$p") req/s for $(phase_dur "$p"), truncate first: $([ "$(phase_trunc "$p")" = 1 ] && echo yes || echo no)"
  done
  echo "  disk guard: abort the run if free space on $DISK_PATH < ${MIN_FREE_GB} GB (now: $(df --output=avail -BG "$DISK_PATH" 2>/dev/null | tail -1 | tr -d ' ' || echo n/a))"
  echo "  CPU plan (from compose, not changed here): server=$SERVER_CPUS k6=$K6_CPUS sampler+exporters=$SAMPLER_CPUS"
  echo "  changes made: TRUNCATE events (every phase except cap_end); two textfile exporters started and stopped; no sudo, no sysctl, Postgres settings untouched"
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
  for c in k6 taskset curl docker python3 seq awk jq df; do command -v "$c" >/dev/null || die "'$c' not found in PATH"; done
  [ -x "$RUNNER_DIR/runner.sh" ] || die "$RUNNER_DIR/runner.sh missing or not executable"
  [ -f "$ANALYZE" ] || die "$ANALYZE missing"
  [ -x "$REPO/infra/scripts/pg-wal-exporter.sh" ] || [ -f "$REPO/infra/scripts/pg-wal-exporter.sh" ] || die "pg-wal-exporter.sh missing"
  curl -fsS --max-time 3 "$PROM_URL/-/ready" >/dev/null || die "Prometheus not ready at $PROM_URL (docker compose -f infra/compose/docker-compose.yml up -d)"
  curl -fsS --max-time 3 "$PROM_URL/api/v1/query?query=up%7Bjob%3D%22postgres%22%7D" | grep -q '"value":\[[^]]*"1"\]' \
    || die "postgres-exporter (job=postgres) is not 'up' (docker compose -f infra/compose/docker-compose.yml up -d postgres-exporter)"
  curl -fsS --max-time 3 "$PROM_URL/api/v1/query?query=up%7Bjob%3D%22node%22%7D" | grep -q '"value":\[[^]]*"1"\]' \
    || echo "WARN: node_exporter (job=node) is not 'up'; disk and host panels will be empty" >&2
  find_pg_container
  [ "$(docker inspect -f '{{.State.Running}}' "$PG_CONTAINER" 2>/dev/null)" = true ] || die "container $PG_CONTAINER is not running"
  pgq "select count(*) from events" >/dev/null || die "cannot query table 'events' in $PG_CONTAINER (migrations? PGUSER/PGDB?)"
  pgq "select count(*) from pg_stat_wal" >/dev/null || die "pg_stat_wal not available (needs Postgres 14 or newer)"
  [ -f "$API_DIR/dist/main.js" ] || die "$API_DIR/dist/main.js missing: run 'pnpm build' in services/ingest-api"
  [ -f "$API_DIR/.env" ] || die "$API_DIR/.env missing"
  if [ "$MANAGE_SERVER" = 1 ] && curl -fsS --max-time 2 "$BASE_URL/healthz" >/dev/null 2>&1; then
    die "something already answers on $BASE_URL; stop it (run.sh starts its own ingest-api per run) or set MANAGE_SERVER=0"
  fi
  local free; free=$(df --output=avail -BG "$DISK_PATH" | tail -1 | tr -dc 0-9)
  [ "$free" -ge $(( MIN_FREE_GB + 5 )) ] || die "only ${free} GB free on $DISK_PATH; the soak needs room (guard is ${MIN_FREE_GB} GB). Free some space or lower DURATION"
  echo "postgres container: $PG_CONTAINER | free disk on $DISK_PATH: ${free} GB"
}

# ---- 5. Server lifecycle (ingest-api, connects straight to Postgres like B3 to B7) -------------------
SERVER_PID=""
# Env vars beat --env-file, so API_KEYS/COUNTER_MODE/PG_POOL_MAX here win over .env. DATABASE_URL is the one of .env.
start_server() {
  [ "$MANAGE_SERVER" = 1 ] || return 0
  stop_server
  local keys; keys=$(seq 0 $(( TENANTS - 1 )) | awk -v p="$KEY_PREFIX" '{printf "%s%s%d:t%d", (NR>1?",":""), p, $1, $1}')
  ( cd "$API_DIR" && exec env API_KEYS="$keys" COUNTER_MODE=off PG_POOL_MAX="$POOL" taskset -c "$SERVER_CPUS" node --env-file=.env dist/main.js ) >"$OUT/server.out" 2>&1 &
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

# Two textfile exporters (same as B6): Postgres container CPU/memory/disk, and WAL volume. They feed Prometheus.
PGC_PID=""; WAL_PID=""; SAMPLER_PID=""
stop_sampler() { [ -n "$SAMPLER_PID" ] && { kill "$SAMPLER_PID" 2>/dev/null || true; wait "$SAMPLER_PID" 2>/dev/null || true; }; SAMPLER_PID=""; }
cleanup() {
  stop_sampler; stop_server
  [ -n "$PGC_PID" ] && kill "$PGC_PID" 2>/dev/null || true
  # pg-wal-exporter stops its own children (docker exec, read loop) when it gets TERM; I give it a moment to do that
  if [ -n "$WAL_PID" ]; then kill "$WAL_PID" 2>/dev/null || true; sleep 1; fi
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

# One sampler line. Columns (tab separated, the same names as in hook.py):
#   epoch  n_live  table_bytes  index_bytes  ckpt_timed  ckpt_req  buf_ckpt  buf_backend  buf_clean  ckpt_write_ms  ckpt_sync_ms
#   db_hit  db_read  wal_bytes  wal_fpi  autovac  inserted  qdur_sum_s  qdur_count  free_gb
# Postgres columns come from ONE psql call (cheap: sizes are file sizes, no table scan). n_live is the statistics
# collector's estimate of live rows (count(*) on 10 million rows would load the database while I measure).
# inserted / qdur_* are the server's own counters (ep_events_inserted_total, ep_pg_query_duration_seconds sum and count).
SAMPLE_COLS="epoch n_live table_bytes index_bytes ckpt_timed ckpt_req buf_ckpt buf_backend buf_clean ckpt_write_ms ckpt_sync_ms db_hit db_read wal_bytes wal_fpi autovac inserted qdur_sum_s qdur_count free_gb"
sample_once() {   # sample_once <file>
  local pg srv free
  pg=$(docker exec -i "$PG_CONTAINER" psql -U "$PGUSER" -d "$PGDB" -X -At -F $'\t' -c "
    select coalesce((select n_live_tup from pg_stat_user_tables where relname='events'),0),
           pg_table_size('events'), pg_indexes_size('events'),
           b.checkpoints_timed, b.checkpoints_req, b.buffers_checkpoint, b.buffers_backend, b.buffers_clean,
           b.checkpoint_write_time::bigint, b.checkpoint_sync_time::bigint,
           d.blks_hit, d.blks_read, w.wal_bytes::bigint, w.wal_fpi,
           coalesce((select autovacuum_count from pg_stat_user_tables where relname='events'),0)
    from pg_stat_bgwriter b, pg_stat_database d, pg_stat_wal w where d.datname=current_database()" 2>/dev/null) || pg=$(printf 'n/a\t%.0s' {1..14}; printf 'n/a')   # 15 postgres columns, all unknown
  srv=$(curl -fsS --max-time 2 "$BASE_URL/metrics" 2>/dev/null | awk '
      $1=="ep_events_inserted_total" {ins=$2}
      $1=="ep_pg_query_duration_seconds_sum" {qs=$2}
      $1=="ep_pg_query_duration_seconds_count" {qc=$2}
      END {printf "%s\t%s\t%s", (ins==""?"n/a":ins), (qs==""?"n/a":qs), (qc==""?"n/a":qc)}') || srv=$'n/a\tn/a\tn/a'
  [ -n "$srv" ] || srv=$'n/a\tn/a\tn/a'
  free=$(df --output=avail -BG "$DISK_PATH" | tail -1 | tr -dc 0-9)
  printf '%s\t%s\t%s\t%s\n' "$(date +%s)" "$pg" "$srv" "$free" >>"$1"
}
# Background sampler: first line now, then one every <period> s. If free disk is below the guard it creates $OUT/ABORT.
start_sampler() {   # start_sampler <file> <period seconds>
  printf '%s\n' "${SAMPLE_COLS// /$'\t'}" >"$1"
  ( while :; do
      sample_once "$1"
      f=$(tail -1 "$1" | awk -F'\t' '{print $NF}')
      if [ -n "$f" ] && [ "$f" -lt "$MIN_FREE_GB" ] 2>/dev/null; then touch "$OUT/ABORT"; fi
      sleep "$2"
    done ) &
  SAMPLER_PID=$!; taskset -cp "$SAMPLER_CPUS" "$SAMPLER_PID" >/dev/null 2>&1 || true
}

run_k6() {   # run_k6 <testid> <phase> <rate> <duration>
  ( cd "$RUNNER_DIR" && env RESULTS_ROOT="$HERE/results" RESULTS_NAME="$RESULTS_NAME" LABEL="$1" SCRIPT="stage-1/B11/script.js" \
      RATE="$3" DURATION="$4" PHASE="$2" TENANTS="$TENANTS" POOL_MAX="$POOL" KEY_PREFIX="$KEY_PREFIX" \
      BASE_URL="$BASE_URL" ROUTE="/healthz" ./runner.sh )
}

metric1() { curl -fsS --max-time 2 "$BASE_URL/metrics" 2>/dev/null | awk -v m="$1" '$1==m {printf "%d", $2; f=1} END{if(!f) printf "n/a"}'; }
pool_end() { curl -fsS --max-time 2 "$BASE_URL/debug/stats" 2>/dev/null | python3 -c 'import sys,json; p=json.load(sys.stdin)["pool"]; print("%s/%s/%s of %s" % (p["total"],p["idle"],p["waiting"],p["max"]))' 2>/dev/null || echo n/a; }

# ONE run. one_run <phase>
one_run() {
  local phase="$1" rate dur
  rate=$(phase_rate "$phase"); dur=$(phase_dur "$phase")
  testid="b11-${phase}"
  echo; echo "== $phase ($(phase_kind "$phase")): rate=$rate duration=$dur ($testid) =="
  CAP_FILE="$CAP/$testid.txt"; : >"$CAP_FILE"
  rm -f "$OUT/ABORT"
  # fresh start: fresh server (new pool), table empty (except cap_end: the big table is the point)
  stop_sampler; stop_server; wait_db_quiet; start_server; warmup; wait_db_quiet
  if [ "$(phase_trunc "$phase")" = 1 ]; then reset_db; else pgq "CHECKPOINT" >/dev/null; fi
  sleep 5
  local rows_pre; rows_pre=$(pgq "select count(*) from events")
  tag pre "rows_pre=$rows_pre pool=$(pool_end) table=$(pgq "select pg_size_pretty(pg_table_size('events'))") index=$(pgq "select pg_size_pretty(pg_indexes_size('events'))")"
  local t0 status=ok; t0=$(date +%s)
  # short runs (probes, control) are sampled more often so that they still get at least 6 samples
  local per=$(( $(to_seconds "$dur") / 6 )); [ "$per" -gt "$SAMPLE_S" ] && per="$SAMPLE_S"; [ "$per" -lt 5 ] && per=5
  start_sampler "$CAP/$testid.samples.tsv" "$per"
  run_k6 "$testid" "$phase" "$rate" "$dur" & local kpid=$!
  # wait for k6; every 5 s look for the disk guard file
  while kill -0 "$kpid" 2>/dev/null; do
    if [ -e "$OUT/ABORT" ]; then
      echo "!! free disk below ${MIN_FREE_GB} GB: stopping $testid"; status=ABORTED-DISK
      pkill -TERM -f -- "testid=$testid" 2>/dev/null || true
      break
    fi
    sleep 5
  done
  wait "$kpid" || { [ "$status" = ok ] && status=FAILED; echo "!! $testid ended with an error: see its manifest.md"; }
  stop_sampler
  sleep 3; wait_db_quiet                       # let requests that were still running finish before counting rows
  sample_once "$CAP/$testid.samples.tsv"       # last sample, right after the run
  local rows_post; rows_post=$(pgq "select count(*) from events"); local rows=$(( rows_post - rows_pre ))
  tag post "pool_end=$(pool_end) connects=$(metric1 ep_pg_pool_connects_total) removes=$(metric1 ep_pg_pool_removes_total); events rows $rows_post (before the run: $rows_pre)"
  # heavy captures: only here, after k6 has finished
  tag post "pg_stat_user_tables / statio for events"
  pgqt "select n_live_tup, n_tup_ins, autovacuum_count, last_autovacuum, pg_size_pretty(pg_table_size('events')) as table_size, pg_size_pretty(pg_indexes_size('events')) as index_size from pg_stat_user_tables where relname='events'" >>"$CAP_FILE" 2>&1 || true
  pgqt "select heap_blks_read, heap_blks_hit, idx_blks_read, idx_blks_hit from pg_statio_user_tables where relname='events'" >>"$CAP_FILE" 2>&1 || true
  pgqt "select indexrelname, pg_size_pretty(pg_relation_size(indexrelid)) as size from pg_stat_user_indexes where relname='events'" >>"$CAP_FILE" 2>&1 || true
  pgqt "select * from pg_stat_bgwriter" >>"$CAP_FILE" 2>&1 || true
  printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$testid" "$phase" "$t0" "$(date +%s)" "$status" "$(phase_kind "$phase")" >>"$RUNS_TSV"
  local js; js=$(ls -t "$OUT"/*/"${testid}_1-lap.json" 2>/dev/null | head -1 || true)
  if [ -n "$js" ]; then
    B11_SAMPLES="$CAP/$testid.samples.tsv" \
    python3 "$ANALYZE" row --json "$js" --testid "$testid" --phase "$phase" --rate "$rate" "${ANALYZE_FLAGS[@]}" \
      --rows "$rows" --extra "status=$status" \
      --hook "$HERE/hook.py" --out "$SUMMARY" \
      || echo "WARN: could not add the summary row of $testid (the numbers are still in its capture files); continuing" >&2
    echo "   -> $(python3 "$ANALYZE" table "$SUMMARY" | tail -1 | tr -s ' ')"
  else
    echo "   !! no summary json for $testid (k6 failed before finishing?)"
  fi
}

# ---- 7. Main ----------------------------------------------------------------------
preflight
mkdir -p "$CAP"
printf 'testid\tphase\tstart_epoch\tend_epoch\tstatus\tkind\n' >"$RUNS_TSV"

{   # environment snapshot (read only)
  echo "experiment: $EXP_ID   date: $(date -Is)"
  echo "git: $(git -C "$REPO" rev-parse --short HEAD 2>/dev/null || echo n/a), $(git -C "$REPO" status --porcelain 2>/dev/null | wc -l) dirty files"
  echo "kernel: $(uname -r)   node: $(node -v)   k6: $(k6 version | head -1)   cpus: $(nproc)"
  echo "pinning: server=$SERVER_CPUS k6=$K6_CPUS sampler=$SAMPLER_CPUS; container cpusets (from compose):"
  for c in "$PG_CONTAINER" ep-prometheus ep-grafana ep-node-exporter ep-postgres-exporter; do
    docker inspect -f '  {{.Name}} cpuset={{.HostConfig.CpusetCpus}} nanocpus={{.HostConfig.NanoCpus}} memory={{.HostConfig.Memory}}' "$c" 2>/dev/null || true
  done
  echo "settings: PHASES=[$PHASES] RATE=$RATE DURATION=$DURATION PROBE_RATE=$PROBE_RATE CAP_DURATION=$CAP_DURATION FRESH_DURATION=$FRESH_DURATION SAMPLE_S=$SAMPLE_S POOL=$POOL TENANTS=$TENANTS MIN_FREE_GB=$MIN_FREE_GB"
  echo "server .env (as used, passwords hidden):"; grep -E '^(PG_|RETRY|BODY|LOG)' "$API_DIR/.env" | sed 's/^/  /'
  echo "--- postgres ---"
  pgq "select version()" || true
  for s in max_connections shared_buffers effective_cache_size synchronous_commit fsync full_page_writes checkpoint_timeout checkpoint_completion_target max_wal_size wal_buffers autovacuum; do echo "$s = $(pgq "show $s" 2>/dev/null)"; done
  echo "rows in events at start: $(pgq "select count(*) from events")"
  echo "--- disk ---"; df -h "$DISK_PATH" | tail -1; docker inspect -f '{{range .Mounts}}{{.Source}} {{end}}' "$PG_CONTAINER" || true
  echo "--- cpu model ---"; lscpu | grep -E 'Model name|Thread|Core|Socket' || true
} >"$OUT/env.txt"

# Postgres container CPU/mem/disk and WAL volume -> Prometheus (if one cannot start, its panels stay empty)
PG_CONTAINER="$PG_CONTAINER" taskset -c "$SAMPLER_CPUS" bash "$REPO/infra/scripts/pg-container-exporter.sh" 2>"$OUT/pg-exporter.log" &
PGC_PID=$!; sleep 2
kill -0 "$PGC_PID" 2>/dev/null || { echo "WARN: pg-container-exporter did not start ($(tail -1 "$OUT/pg-exporter.log")); Postgres CPU/memory panels will be empty" >&2; PGC_PID=""; }
PG_CONTAINER="$PG_CONTAINER" PGUSER="$PGUSER" PGDB="$PGDB" taskset -c "$SAMPLER_CPUS" bash "$REPO/infra/scripts/pg-wal-exporter.sh" 2>"$OUT/pg-wal-exporter.log" &
WAL_PID=$!; sleep 2
kill -0 "$WAL_PID" 2>/dev/null || { echo "WARN: pg-wal-exporter did not start ($(tail -1 "$OUT/pg-wal-exporter.log")); the WAL panels will be empty (the sampler file still has WAL bytes)" >&2; WAL_PID=""; }

for p in $PHASES; do one_run "$p"; done

echo; [ -f "$SUMMARY" ] && python3 "$ANALYZE" table "$SUMMARY"
echo; echo "Done. Zoom Grafana to the times in $RUNS_TSV (epoch seconds). Summary: $SUMMARY  samples + captures: $CAP  env: $OUT/env.txt"
echo "The soak leaves its rows in table events; the next run of any experiment truncates it."
