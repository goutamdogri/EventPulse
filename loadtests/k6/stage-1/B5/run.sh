#!/usr/bin/env bash
# B5: Index write amplification
# ONE command runs the whole experiment:
#     ./run.sh
#
# Idea: every secondary index on the events table is one more thing Postgres must update for every INSERT (one more
# b-tree page change and one more WAL record, sometimes a whole 8 KB page image). I change how many secondary
# indexes exist (0, 1, 3, 5) and whether the primary key event_id is random (uuid v4) or time-ordered (uuid v7).
# The primary key itself always exists (the server's ON CONFLICT (event_id) needs it), so "0 indexes" means "only the PK".
#
# Index levels (cumulative):
#   0  PK only
#   1  + events_tenant_time_idx (tenant_id, occurred_at)            (this is the index migration 0002 creates)
#   3  + b5_type_idx (type), b5_tenant_type_time_idx (tenant_id, type, occurred_at)
#   5  + b5_payload_gin (GIN on payload), b5_received_idx (received_at)
#   1g probe only: level 1 + the GIN index, to see what the GIN costs alone
#
# Cells (key:level): v4:0 v7:0 v4:1 v7:1 v4:3 v7:3 v4:5 v7:5.  Every cell has TWO measured runs:
#   under  1500 req/s  comfortable for every level -> cost per row (WAL bytes, CPU, latency) without saturation
#   over   6000 req/s  above the 4300 req/s capacity B3 measured (with one index) -> max accepted rps (202/s)
# The SAME offered rates are used for every cell, so cells can be compared directly.
#
# What the script does (so I can re-read it later):
#   1. preflight    - tools, Prometheus, postgres-exporter, build of ingest-api, nothing already on the server port
#   2. env snapshot - versions, Postgres settings, cpusets of the containers (read only)
#   3. for every repeat x cell x phase: restart server (own API_KEYS, 100 tenants), warm up, TRUNCATE events and build
#      the index set of the cell, CHECKPOINT, snapshot, k6 via runner.sh, snapshot, index sizes, row audit, summary row.
#      In the middle of every "over" run ONE cheap capture (pg_stat_activity by wait event + pg_stat_wal).
#   4. probes (SEPARATE runs, never headline numbers):
#        probe_gin     v4 + level 1g, over
#        probe_pre_v4  v4 + level 1, table pre-loaded with PRELOAD_ROWS rows, over
#        probe_pre_v7  v7 + level 1, table pre-loaded with PRELOAD_ROWS rows, over
#      The pre-loaded pair is there because after a 45 s run on an empty table everything fits in memory, so the
#      random-key penalty (cache misses, page splits) is hidden. With millions of rows it should show.
#
# Order: repeat is the OUTER loop. Odd repeats go through the cells as listed, even repeats reversed.
# The server connects DIRECTLY to Postgres (:5434 from .env). No Toxiproxy in B5.
#
# CPU pinning: NOT done here. Containers come from docker compose (cpusets are in the compose file).
#   server 0,6 (taskset below)   postgres 1,7,2,8   k6 3,9,4,10   monitoring + toxiproxy 5,11
#
# This script changes the schema of the events table (DROP/CREATE INDEX) and restores the original index when it exits.
# If it is killed hard (kill -9) the trap does not run; restore by hand:   ./run.sh RESTORE   (see README)
#
# Usage:  DRY_RUN=1 ./run.sh                                            (print the plan, run nothing)
#         ./run.sh RESTORE                                              (only drop the b5_* indexes and recreate events_tenant_time_idx)
#         CELLS="v4:3" PHASES="over" REPEATS=1 DURATION=30s PROBES=0 RESULTS_NAME=b5-trial ./run.sh   (trial, NOT a result)
#         OVER_RATE=5000 ./run.sh                                       (override the offered rate)
set -euo pipefail

EXP_ID="B5"
export PYTHONDONTWRITEBYTECODE=1                 # analyze.py loads hook.py; do not leave a __pycache__ in the repo

# ---- 1. Settings (all can be overridden from the environment) ---------------
CELLS="${CELLS:-v4:0 v7:0 v4:1 v7:1 v4:3 v7:3 v4:5 v7:5}"   # key:level
PHASES="${PHASES:-under over}"
TENANTS="${TENANTS:-100}"
POOL="${POOL:-25}"                               # PG_POOL_MAX, same as B3 and B4
UNDER_RATE="${UNDER_RATE:-1500}"                 # below the capacity of every level (predicted lowest is about 2300)
OVER_RATE="${OVER_RATE:-6000}"                   # above the 4300 req/s capacity B3 measured (with one index)
REPEATS="${REPEATS:-3}"
DURATION="${DURATION:-45s}"
PROBES="${PROBES:-1}"                            # 1 = also run the three probe runs
PRELOAD_ROWS="${PRELOAD_ROWS:-2000000}"          # rows in the table before the pre-loaded probes
KEY_PREFIX="${KEY_PREFIX:-epk_b5_}"              # api key i = KEY_PREFIX + i, tenant i = t<i>
RESULTS_NAME="${RESULTS_NAME:-b5}"
BASE_URL="${BASE_URL:-http://localhost:8000}"
PROM_URL="${PROM_URL:-http://localhost:9090}"
MANAGE_SERVER="${MANAGE_SERVER:-1}"
SERVER_CPUS="${SERVER_CPUS:-0,6}"
export K6_CPUS="${K6_CPUS:-3,9,4,10}"            # runner.sh reads this (taskset for k6)
PG_CONTAINER="${PG_CONTAINER:-}"                 # empty = auto-detect
PGUSER="${PGUSER:-eventpulse}"; PGDB="${PGDB:-eventpulse}"
DRY_RUN="${DRY_RUN:-0}"
OVERHEAD_S=45                                    # restart + warm-up + truncate + index build + settle per run (rough)
PRELOAD_S=150                                    # extra time for one pre-load (rough, I do not know it yet)

# ---- 2. Paths ----------------------------------------------------------------
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"      # loadtests/k6/stage-1/B5
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

# ---- 3. Index sets ------------------------------------------------------------
# All index names this experiment may create. events_tenant_time_idx is the one from migration 0002 (the "baseline").
ALL_IDX="events_tenant_time_idx, b5_type_idx, b5_tenant_type_time_idx, b5_payload_gin, b5_received_idx"
DDL_BASE="CREATE INDEX events_tenant_time_idx ON events (tenant_id, occurred_at);"
DDL_TYPE="CREATE INDEX b5_type_idx ON events (type); CREATE INDEX b5_tenant_type_time_idx ON events (tenant_id, type, occurred_at);"
DDL_GIN="CREATE INDEX b5_payload_gin ON events USING gin (payload);"
DDL_RECV="CREATE INDEX b5_received_idx ON events (received_at);"
ddl_for_level() {
  case "$1" in
    0)  echo "" ;;
    1)  echo "$DDL_BASE" ;;
    3)  echo "$DDL_BASE $DDL_TYPE" ;;
    5)  echo "$DDL_BASE $DDL_TYPE $DDL_GIN $DDL_RECV" ;;
    1g) echo "$DDL_BASE $DDL_GIN" ;;
    *)  die "unknown index level '$1' (use 0, 1, 3, 5 or 1g)" ;;
  esac
}

# ---- 4. Plan / dry run ---------------------------------------------------------
if [ "${1:-}" != "RESTORE" ]; then
  n_cells=$(wc -w <<<"$CELLS"); n_phases=$(wc -w <<<"$PHASES")
  dur_s=$(to_seconds "$DURATION")
  total_s=$(( n_cells*n_phases*REPEATS*(dur_s+OVERHEAD_S) ))
  [ "$PROBES" = 1 ] && total_s=$(( total_s + 3*(dur_s+OVERHEAD_S) + 2*PRELOAD_S ))
  echo "== $EXP_ID: $((n_cells*n_phases*REPEATS)) measured runs$([ "$PROBES" = 1 ] && echo " + 3 probe runs"), results in $OUT =="
  echo "== pool $POOL | tenants $TENANTS | cells $CELLS | phases $PHASES | duration $DURATION | repeats $REPEATS | expected total about $(( total_s/60 )) min =="
  if [ "$DRY_RUN" = "1" ]; then
    for rep in $(seq 1 "$REPEATS"); do echo "  repeat $rep order: $(cells_for_rep "$rep")"; done
    echo "  offered rates: under=$UNDER_RATE  over=$OVER_RATE req/s (the same for every cell)"
    [ "$PROBES" = 1 ] && echo "  probes: probe_gin (v4, level 1g), probe_pre_v4 and probe_pre_v7 (level 1, $PRELOAD_ROWS rows pre-loaded), all $OVER_RATE req/s, $DURATION"
    echo "  CPU plan (from compose, not changed here): server=$SERVER_CPUS k6=$K6_CPUS"
    exit 0
  fi
fi

# ---- 5. Helpers ------------------------------------------------------------------
pgq()  { docker exec -i "$PG_CONTAINER" psql -U "$PGUSER" -d "$PGDB" -X -At -c "$1"; }   # one value / rows, no decoration
pgqt() { docker exec -i "$PG_CONTAINER" psql -U "$PGUSER" -d "$PGDB" -X -c "$1"; }       # pretty table for captures

find_pg_container() {
  if [ -z "$PG_CONTAINER" ]; then
    local found; found=$(docker ps --format '{{.Names}} {{.Image}}' | awk '$2 ~ /^postgres:/ {print $1}')
    [ "$(wc -w <<<"$found")" = 1 ] || die "found [$found] postgres containers; set PG_CONTAINER=<name>"
    PG_CONTAINER="$found"
  fi
}

# Put the database back the way migration 0002 made it: only events_tenant_time_idx.
INDEX_TOUCHED=0
restore_indexes() {
  pgq "DROP INDEX IF EXISTS $ALL_IDX; $DDL_BASE" >/dev/null 2>&1 && echo "indexes restored: events has events_tenant_time_idx again" \
    || echo "WARN: could not restore indexes; run  ./run.sh RESTORE" >&2
}

if [ "${1:-}" = "RESTORE" ]; then
  find_pg_container; restore_indexes; exit 0
fi

preflight() {
  for c in k6 taskset curl docker python3 tac seq awk; do command -v "$c" >/dev/null || die "'$c' not found in PATH"; done
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
  [ -f "$API_DIR/dist/main.js" ] || die "$API_DIR/dist/main.js missing: run 'pnpm build' in services/ingest-api"
  [ -f "$API_DIR/.env" ] || die "$API_DIR/.env missing"
  if [ "$MANAGE_SERVER" = 1 ] && curl -fsS --max-time 2 "$BASE_URL/healthz" >/dev/null 2>&1; then
    die "something already answers on $BASE_URL; stop it (run.sh starts its own ingest-api per run) or set MANAGE_SERVER=0"
  fi
  echo "postgres container: $PG_CONTAINER | server connects directly to Postgres (no proxy)"
}

# ---- 6. Server lifecycle (ingest-api) ---------------------------------------------
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
  [ "$INDEX_TOUCHED" = 1 ] && restore_indexes
  return 0
}
trap cleanup EXIT
trap 'exit 130' INT TERM        # so Ctrl+C also runs the EXIT trap above (and restores the indexes)

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

# SQL that makes one uuid per generated row g (1..N). v4 = random. v7 = 12 hex of milliseconds (older than now, growing with g)
# + '7' + 3 random hex + variant nibble (8..b) + 15 random hex. md5(random()::text) is just a cheap source of random hex digits.
# PG 15 has no uuidv7(), so I build the 32 hex digits myself and cast them to uuid.
uuid_sql() {   # uuid_sql <v4|v7> <N>
  if [ "$1" = v7 ]; then
    echo "(lpad(to_hex((extract(epoch from now())*1000)::bigint - 3600000 - $2 + g), 12, '0') || '7' || substr(md5(random()::text),1,3) || substr('89ab', 1 + floor(random()*4)::int, 1) || substr(md5(random()::text),1,15))::uuid"
  else
    echo "gen_random_uuid()"
  fi
}
# Fill the table with N rows through normal INSERTs (so the indexes grow the same way they do under load), then
# VACUUM ANALYZE so autovacuum does not start in the middle of the measured run, then CHECKPOINT.
preload() {   # preload <key> <N>
  echo "   pre-loading $2 rows ($1 keys), this takes a while..."
  pgq "INSERT INTO events (event_id, tenant_id, type, occurred_at, schema_version, payload)
       SELECT $(uuid_sql "$1" "$2"), 't' || (g % $TENANTS), 'preload.' || (g % 10), now() - ($2 - g) * interval '1 millisecond', 1,
              jsonb_build_object('user', 'u' || (random()*1000000)::int, 'data', repeat('x', 300))
       FROM generate_series(1, $2) g" >/dev/null
  pgq "VACUUM (ANALYZE) events" >/dev/null
}

# reset_db <level> <key> <preload 0|1>: empty table, the index set of the cell, fresh statistics, checkpoint.
reset_db() {
  INDEX_TOUCHED=1
  pgq "TRUNCATE events" >/dev/null
  pgq "DROP INDEX IF EXISTS $ALL_IDX" >/dev/null
  local ddl; ddl=$(ddl_for_level "$1"); [ -n "$ddl" ] && pgq "$ddl" >/dev/null
  [ "$3" = 1 ] && preload "$2" "$PRELOAD_ROWS"
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

# One psql call that returns the cumulative numbers I need, separated by '|' (psql -At default).
#   wal_bytes from the WAL position (exact, no stats delay); records and fpi from pg_stat_wal (full-page images);
#   checkpoints timed (by checkpoint_timeout) and requested (by WAL size or a manual CHECKPOINT) and buffers they wrote;
#   autovacuum runs on events; blocks Postgres read from outside its cache and found in cache.
stat_vec() {
  pgq "select pg_wal_lsn_diff(pg_current_wal_insert_lsn(),'0/0')::bigint,
              (select wal_records from pg_stat_wal), (select wal_fpi from pg_stat_wal),
              (select checkpoints_timed from pg_stat_bgwriter), (select checkpoints_req from pg_stat_bgwriter), (select buffers_checkpoint from pg_stat_bgwriter),
              (select coalesce(autovacuum_count,0) from pg_stat_user_tables where relname='events'),
              (select blks_read from pg_stat_database where datname='$PGDB'), (select blks_hit from pg_stat_database where datname='$PGDB')"
}
PRE=""
snapshot() {   # snapshot <pre|post>: raw vector into the capture file, and into $PRE for pre
  local v; v=$(stat_vec)
  tag "$1" "stat_vec: wal_bytes|wal_records|wal_fpi|ckpt_timed|ckpt_req|buffers_ckpt|autovacuum|blks_read|blks_hit"
  echo "$v" >>"$CAP_FILE"
  [ "$1" = pre ] && PRE="$v"
  return 0
}
index_sizes() {   # readable copy of the sizes for the write-up
  tag sizes "pg_relation_size of the table and of every index of events"
  pgqt "select c.relname as relation, pg_relation_size(c.oid) as bytes, pg_size_pretty(pg_relation_size(c.oid)) as size
        from pg_class c where c.oid = 'events'::regclass or c.oid in (select indexrelid from pg_index where indrelid = 'events'::regclass) order by 2 desc" >>"$CAP_FILE" 2>&1 || true
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

run_k6() {   # run_k6 <rate> <duration> <testid> <phase> <key> <level> <preload>
  ( cd "$RUNNER_DIR" && env RESULTS_ROOT="$HERE/results" RESULTS_NAME="$RESULTS_NAME" LABEL="$3" SCRIPT="stage-1/B5/script.js" \
      RATE="$1" DURATION="$2" PHASE="$4" KEY_KIND="$5" IDX="$6" PRELOAD="$7" TENANTS="$TENANTS" POOL_MAX="$POOL" KEY_PREFIX="$KEY_PREFIX" BASE_URL="$BASE_URL" ROUTE="/healthz" ./runner.sh )
}
record() { printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$testid" "$PHASE" "$KEY" "$LEVEL" "$RATE_NOW" "$1" "$2" "$3" >>"$RUNS_TSV"; }

# ONE run. one_run <phase> <key v4|v7> <level> <rate> <duration> <rep> <preload 0|1>
one_run() {
  PHASE="$1"; KEY="$2"; LEVEL="$3"; RATE_NOW="$4"; local dur="$5" rep="$6" pre="$7"
  testid="b5-${PHASE}-${KEY}i${LEVEL}-n${rep}"
  case "$PHASE" in probe_*) testid="b5-${PHASE//_/-}";; esac
  echo; echo "== $PHASE: key=$KEY indexes=$LEVEL rate=$RATE_NOW repeat=$rep preload=$pre ($testid) =="
  CAP_FILE="$CAP/$testid.txt"; : >"$CAP_FILE"
  stop_server; wait_db_quiet
  start_server "$POOL"
  warmup; wait_db_quiet; reset_db "$LEVEL" "$KEY" "$pre"
  local rows_pre; rows_pre=$(pgq "select count(*) from events")
  sleep 5                                    # let the reset/checkpoint I/O finish before measuring
  snapshot pre; local t0; t0=$(date +%s); local status=ok
  if [ "$PHASE" = over ] || [ "$PHASE" = probe_gin ] || [ "$PHASE" = probe_pre_v4 ] || [ "$PHASE" = probe_pre_v7 ]; then
    run_k6 "$RATE_NOW" "$dur" "$testid" "$PHASE" "$KEY" "$LEVEL" "$pre" & local kpid=$!
    sleep $(( $(to_seconds "$dur") / 2 + 3 )); mid_capture
    wait "$kpid" || { status=FAILED; echo "!! $testid failed: see its manifest.md"; }
  else
    run_k6 "$RATE_NOW" "$dur" "$testid" "$PHASE" "$KEY" "$LEVEL" "$pre" || { status=FAILED; echo "!! $testid failed: see its manifest.md"; }
  fi
  sleep 3; wait_db_quiet                      # let requests that were still running finish before counting rows
  local rows_post; rows_post=$(pgq "select count(*) from events"); local rows=$(( rows_post - rows_pre ))
  local et; et=$(err_types || true)
  snapshot post; local POST; POST=$(stat_vec); index_sizes
  tag post "error types from the server: ${et:-none}; events rows $rows_post (before the run: $rows_pre)"; record "$t0" "$(date +%s)" "$status"

  # per-row numbers from the two snapshots (post - pre) and the sizes of the primary key and the secondary indexes
  local pk_b sec_b
  pk_b=$(pgq "select pg_relation_size('events_pkey')")
  sec_b=$(pgq "select coalesce(sum(pg_relation_size(indexrelid)),0) from pg_index where indrelid='events'::regclass and not indisprimary")
  local wal_per_row fpi_per_row pk_per_row sec_per_row ckpt av hitpct
  IFS='|' read -r p1 p2 p3 p4 p5 p6 p7 p8 p9 <<<"$PRE"; IFS='|' read -r q1 q2 q3 q4 q5 q6 q7 q8 q9 <<<"$POST"
  wal_per_row=$(awk -v a="$((q1-p1))" -v n="$rows" 'BEGIN{ if (n>0) printf "%.0f", a/n; else print "n/a" }')
  fpi_per_row=$(awk -v a="$((q3-p3))" -v n="$rows" 'BEGIN{ if (n>0) printf "%.3f", a/n; else print "n/a" }')
  pk_per_row=$(awk -v a="$pk_b" -v n="$rows_post" 'BEGIN{ if (n>0) printf "%.1f", a/n; else print "n/a" }')
  sec_per_row=$(awk -v a="$sec_b" -v n="$rows_post" 'BEGIN{ if (n>0) printf "%.1f", a/n; else print "n/a" }')
  ckpt="$((q4-p4))t/$((q5-p5))r"; av=$((q7-p7))
  hitpct=$(awk -v r="$((q8-p8))" -v h="$((q9-p9))" 'BEGIN{ if (r+h>0) printf "%.2f", 100*h/(r+h); else print "n/a" }')
  local js; js=$(ls -t "$OUT"/*/"${testid}_1-lap.json" 2>/dev/null | head -1 || true)
  if [ -n "$js" ]; then
    python3 "$ANALYZE" row --json "$js" --testid "$testid" --phase "$PHASE" --rate "$RATE_NOW" --prefix b5 \
      --var "key=$KEY" --var "idx=$LEVEL" --var "preload=$pre" --rows "$rows" \
      --extra "wal_B_per_row=$wal_per_row" --extra "fpi_per_row=$fpi_per_row" --extra "pk_B_per_row=$pk_per_row" --extra "sec_idx_B_per_row=$sec_per_row" \
      --extra "ckpt=$ckpt" --extra "autovac=$av" --extra "cache_hit_pct=$hitpct" --extra "err_types=${et:-none}" \
      --hook "$HERE/hook.py" --out "$SUMMARY"
    echo "   -> $(python3 "$ANALYZE" table "$SUMMARY" | tail -1 | tr -s ' ')"
  else
    echo "   !! no summary json for $testid (k6 failed before finishing?)"
  fi
}

# ---- 8. Main ----------------------------------------------------------------------
preflight
mkdir -p "$CAP"
printf 'testid\tphase\tkey\tindexes\trate\tstart_epoch\tend_epoch\tstatus\n' >"$RUNS_TSV"

{   # environment snapshot (read only)
  echo "experiment: $EXP_ID   date: $(date -Is)"
  echo "git: $(git -C "$REPO" rev-parse --short HEAD 2>/dev/null || echo n/a), $(git -C "$REPO" status --porcelain 2>/dev/null | wc -l) dirty files"
  echo "kernel: $(uname -r)   node: $(node -v)   k6: $(k6 version | head -1)   cpus: $(nproc)"
  echo "pinning: server=$SERVER_CPUS k6=$K6_CPUS; container cpusets (from compose):"
  for c in "$PG_CONTAINER" ep-prometheus ep-grafana ep-node-exporter ep-postgres-exporter; do
    docker inspect -f '  {{.Name}} cpuset={{.HostConfig.CpusetCpus}} nanocpus={{.HostConfig.NanoCpus}}' "$c" 2>/dev/null || true
  done
  echo "settings: POOL=$POOL TENANTS=$TENANTS CELLS=[$CELLS] PHASES=[$PHASES] REPEATS=$REPEATS DURATION=$DURATION PROBES=$PROBES PRELOAD_ROWS=$PRELOAD_ROWS"
  echo "rates: under=$UNDER_RATE over=$OVER_RATE (same for every cell)"
  echo "--- postgres ---"
  docker inspect -f 'image={{.Config.Image}} nanocpus={{.HostConfig.NanoCpus}} memory={{.HostConfig.Memory}} cpuset={{.HostConfig.CpusetCpus}}' "$PG_CONTAINER"
  pgq "select version()" || true
  for s in max_connections shared_buffers synchronous_commit fsync wal_level full_page_writes wal_compression max_wal_size min_wal_size checkpoint_timeout checkpoint_completion_target autovacuum gin_pending_list_limit wal_buffers; do echo "$s = $(pgq "show $s" 2>/dev/null)"; done
  echo "--- indexes of events at the start (restored at the end) ---"
  pgq "select indexdef from pg_indexes where tablename='events' order by 1" || true
  echo "--- cpu model ---"; lscpu | grep -E 'Model name|Thread|Core|Socket' || true
} >"$OUT/env.txt"

# Postgres container CPU/mem/disk -> Prometheus (needs cgroup v2; if it cannot start, those panels stay empty)
PG_CONTAINER="$PG_CONTAINER" bash "$REPO/infra/scripts/pg-container-exporter.sh" 2>"$OUT/pg-exporter.log" &
PGC_PID=$!; sleep 2
kill -0 "$PGC_PID" 2>/dev/null || { echo "WARN: pg-container-exporter did not start ($(tail -1 "$OUT/pg-exporter.log")); Postgres CPU/memory panels will be empty" >&2; PGC_PID=""; }
# WAL volume -> Prometheus (the postgres-exporter container has no WAL volume metric on Postgres 15)
PG_CONTAINER="$PG_CONTAINER" PGUSER="$PGUSER" PGDB="$PGDB" bash "$REPO/infra/scripts/pg-wal-exporter.sh" 2>"$OUT/pg-wal-exporter.log" &
WAL_PID=$!; sleep 2
kill -0 "$WAL_PID" 2>/dev/null || { echo "WARN: pg-wal-exporter did not start ($(tail -1 "$OUT/pg-wal-exporter.log")); the WAL panels will be empty (summary.tsv still has WAL bytes per row)" >&2; WAL_PID=""; }

# measured runs: repeat is the OUTER loop; odd repeats forward, even repeats reversed; under before over
for rep in $(seq 1 "$REPEATS"); do
  for cell in $(cells_for_rep "$rep"); do
    for ph in $PHASES; do
      if [ "$ph" = under ]; then r="$UNDER_RATE"; else r="$OVER_RATE"; fi
      one_run "$ph" "${cell%%:*}" "${cell#*:}" "$r" "$DURATION" "$rep" 0
    done
  done
done
# probe runs: separate, never headline numbers
if [ "$PROBES" = 1 ]; then
  one_run probe_gin     v4 1g "$OVER_RATE" "$DURATION" 1 0
  one_run probe_pre_v4  v4 1  "$OVER_RATE" "$DURATION" 1 1
  one_run probe_pre_v7  v7 1  "$OVER_RATE" "$DURATION" 1 1
fi

echo; python3 "$ANALYZE" table "$SUMMARY"
echo; echo "Done. Zoom Grafana to the times in $RUNS_TSV (epoch seconds). Summary: $SUMMARY  captures: $CAP  env: $OUT/env.txt"
